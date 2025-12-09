import argparse
import csv
from datetime import datetime
from decimal import Decimal
import sqlite3


def process_lines(
    lines: list[dict],
    statistics_metadata_id: int,
    state_metadata_id: int,
    data_lettura_key: str,
    data_lettura_format: str,
    lettura_key: str,
) -> None:
    state, sum = get_latest_state_and_sum(statistics_metadata_id=statistics_metadata_id)
    for i in range(len(lines)):
        try:
            from_date = datetime.strptime(
                lines[i][data_lettura_key], data_lettura_format
            )
            lettura = Decimal(lines[i][lettura_key].lstrip("0"))
        except:
            continue

        if lettura <= state:
            continue
        sum += lettura - state
        state = lettura
        try:
            to_date = datetime.strptime(
                lines[i + 1][data_lettura_key], data_lettura_format
            )
        except IndexError:
            to_date = datetime.now()

        run_sql(
            state=lettura,
            sum=sum,
            statistics_metadata_id=statistics_metadata_id,
            state_metadata_id=state_metadata_id,
            from_date=from_date,
            to_date=to_date,
        )


def get_metadata_ids(sensor: str) -> dict[str, int]:
    ids = {}

    res = cur.execute(
        f'SELECT metadata_id FROM states_meta WHERE entity_id = "sensor.{sensor}"'
    )
    ids["state_metadata_id"] = res.fetchone()

    res = cur.execute(
        f'SELECT id FROM statistics_meta WHERE statistic_id = "sensor.{sensor}"'
    )
    ids["statistics_metadata_id"] = res.fetchone()

    return ids


def gas(filename: str, sensor: str) -> None:
    DATA_LETTURA_KEY = "DATA LETTURA"
    DATA_LETTURA_FORMAT = "%Y-%m-%d"

    with open(filename, "r") as f:
        lines = list(csv.DictReader(f, delimiter=";"))
        lines.sort(key=lambda l: l.get(DATA_LETTURA_KEY, ""))
        process_lines(
            lines=lines,
            **get_metadata_ids(sensor=sensor),
            data_lettura_key=DATA_LETTURA_KEY,
            data_lettura_format=DATA_LETTURA_FORMAT,
            lettura_key="LETTURA",
        )


def luce_giornaliera(filename: str, sensors: list[str]) -> None:
    DATA_LETTURA_KEY = "data_lettura"
    DATA_LETTURA_FORMAT = "%d/%m/%Y"
    FASCE = [1, 2, 3]

    with open(filename, "r") as f:
        lines = list(csv.DictReader(f, delimiter=";"))
        lines.sort(
            key=lambda l: datetime.strptime(l[DATA_LETTURA_KEY], DATA_LETTURA_FORMAT)
        )
        for fascia, sensor in zip(FASCE, sensors):
            process_lines(
                lines=lines,
                **get_metadata_ids(sensor=sensor),
                data_lettura_key=DATA_LETTURA_KEY,
                data_lettura_format=DATA_LETTURA_FORMAT,
                lettura_key=f"lettura_f{fascia}",
            )


def update_state(
    state_metadata_id: int,
    state: Decimal,
    from_date: datetime,
    to_date: datetime = datetime.now(),
) -> None:
    cur.execute(
        """
        UPDATE states 
        SET state = {} 
        WHERE 
            last_changed_ts IS NULL AND 
            states.metadata_id = {} AND 
            last_reported_ts IS NULL AND
            last_updated_ts>={} AND 
            last_updated_ts<{};
        """.format(
            state, state_metadata_id, from_date.timestamp(), to_date.timestamp()
        )
    )


def run_sql(
    state: Decimal,
    sum: Decimal,
    statistics_metadata_id: int,
    state_metadata_id: int,
    from_date: datetime,
    to_date: datetime = datetime.now(),
) -> None:
    for table in ["statistics", "statistics_short_term"]:
        cur.execute(
            """
            UPDATE {}
            SET state={}, sum={} 
            WHERE 
                metadata_id={} AND
                start_ts>={} AND 
                start_ts<{};
            """.format(
                table,
                state,
                sum,
                statistics_metadata_id,
                from_date.timestamp(),
                to_date.timestamp(),
            )
        )
    update_state(
        state_metadata_id=state_metadata_id,
        state=state,
        from_date=from_date,
        to_date=to_date,
    )


def get_latest_state_and_sum(
    statistics_metadata_id: int, date_limit: datetime = datetime.now()
) -> tuple[Decimal, Decimal]:
    res = cur.execute(
        'SELECT state, "sum" FROM statistics WHERE metadata_id = {} AND start_ts < {} ORDER BY id DESC LIMIT 1;'.format(
            statistics_metadata_id, date_limit.timestamp()
        )
    )
    state, sum = res.fetchone()
    return Decimal(str(state)), Decimal(str(sum))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="CSV file")
    parser.add_argument(
        "--db",
        help="DB file",
        default="/home/pi/homeassistant/config/home-assistant_v2.db",
    )
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    with open(args.csv, "r") as f:
        headers = f.readline()
        if headers.startswith("PDR"):
            print(f"Importing GAS statistics from '{args.csv}'")
            gas(filename=args.csv, sensor="lettura_gas")
        elif headers.startswith("pod"):
            print(f"Importing ENERGY statistics from '{args.csv}'")
            luce_giornaliera(
                filename=args.csv, sensors=[f"lettura_luce_f{i}" for i in [1, 2, 3]]
            )
        else:
            print(f"{args.csv} not recognized, exit.")

    con.commit()
    con.close()
