import argparse
import csv
from datetime import datetime
from decimal import Decimal
import sqlite3


class Lettura:
    def __init__(self, data_lettura: datetime) -> None:
        self.data_lettura: datetime = data_lettura
        self.letture = None

    def __lt__(self, value) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura < value.data_lettura
        return NotImplemented

    def __le__(self, value) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura <= value.data_lettura
        return NotImplemented

    def __gt__(self, value) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura > value.data_lettura
        return NotImplemented

    def __ge__(self, value) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura >= value.data_lettura
        return NotImplemented

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura == value.data_lettura
        return NotImplemented


class LetturaGas(Lettura):
    def __init__(self, LETTURA, **kwargs) -> None:
        super().__init__(
            datetime.strptime(kwargs["DATA LETTURA"], "%Y-%m-%d"),
        )
        self.lettura = Decimal(LETTURA.lstrip("0"))

    def __repr__(self) -> str:
        return f"{self.data_lettura} - {self.lettura} m³"


class LetturaLuce(Lettura):
    def __init__(
        self,
        data_lettura: str,
        lettura_f1: str,
        lettura_f2: str,
        lettura_f3: str,
        **kwargs,
    ) -> None:
        super().__init__(datetime.strptime(data_lettura, "%d/%m/%Y"))
        self.lettura_f1 = Decimal(lettura_f1)
        self.lettura_f2 = Decimal(lettura_f2)
        self.lettura_f3 = Decimal(lettura_f3)
        self.default = 0

    def __repr__(self) -> str:
        return f"{self.data_lettura} - F1 {self.lettura_f1} kWh, F2 {self.lettura_f2} kWh, F3 {self.lettura_f3} kWh"

    @property
    def lettura(self) -> Decimal:
        return getattr(self, f"lettura_f{self.default}")


def process_lines(
    letture: list[LetturaGas] | list[LetturaLuce], sensor_name: str
) -> None:
    state_metadata_id, statistics_metadata_id = get_metadata(id=f"sensor.{sensor_name}")
    state, sum = get_latest_state_and_sum(
        state_metadata_id=state_metadata_id,
        statistics_metadata_id=statistics_metadata_id,
    )
    letture.sort()
    for i in range(len(letture)):

        if letture[i].lettura <= state:
            continue
        sum += letture[i].lettura - state
        state = letture[i].lettura

        try:
            to_date = letture[i + 1].data_lettura
        except IndexError:
            to_date = datetime.now()

        update_states(
            state_metadata_id=state_metadata_id,
            state=letture[i].lettura,
            from_date=letture[i].data_lettura,
            to_date=to_date,
        )
        update_statistics(
            state=letture[i].lettura,
            sum=sum,
            statistics_metadata_id=statistics_metadata_id,
            from_date=letture[i].data_lettura,
            to_date=to_date,
        )


def get_metadata(id: str) -> tuple[int, int]:
    res = cur.execute("SELECT metadata_id FROM states_meta WHERE entity_id = ?", (id,))
    (state_metadata_id,) = res.fetchone()
    res = cur.execute("SELECT id FROM statistics_meta WHERE statistic_id = ?", (id,))
    (statistics_metadata_id,) = res.fetchone()
    return state_metadata_id, statistics_metadata_id


def gas(filename: str, sensor: str) -> None:
    with open(filename, "r") as f:
        letture = []
        for line in csv.DictReader(f, delimiter=";"):
            try:
                letture.append(LetturaGas(**line))
            except:
                continue
        process_lines(
            letture=letture,
            sensor_name=sensor,
        )


def luce_giornaliera(filename: str, sensors: list[str]) -> None:
    with open(filename, "r") as f:
        letture = [LetturaLuce(**line) for line in csv.DictReader(f, delimiter=";")]
        for fascia, sensor in zip(FASCE, sensors):
            for lettura in letture:
                lettura.default = fascia
            process_lines(
                letture=letture,
                sensor_name=sensor,
            )


def update_states(
    state_metadata_id: int,
    state: Decimal,
    from_date: datetime,
    to_date: datetime = datetime.now(),
) -> None:
    cur.execute(
        """
        UPDATE states 
        SET state = ? 
        WHERE 
            last_changed_ts IS NULL AND 
            states.metadata_id = ? AND 
            last_reported_ts IS NULL AND
            last_updated_ts >= ? AND 
            last_updated_ts < ?;
        """,
        (state, state_metadata_id, from_date.timestamp(), to_date.timestamp()),
    )


def update_statistics(
    state: Decimal,
    sum: Decimal,
    statistics_metadata_id: int,
    from_date: datetime,
    to_date: datetime = datetime.now(),
) -> None:
    for table in ["statistics", "statistics_short_term"]:
        cur.execute(
            """
            UPDATE ?
            SET state = ?, sum = ? 
            WHERE 
                metadata_id = ? AND
                start_ts >= ? AND 
                start_ts < ?;
            """,
            (
                table,
                state,
                sum,
                statistics_metadata_id,
                from_date.timestamp(),
                to_date.timestamp(),
            ),
        )


def get_latest_state_and_sum(
    state_metadata_id: int, statistics_metadata_id: int
) -> tuple[Decimal, Decimal]:
    res = cur.execute(
        "SELECT state FROM states WHERE metadata_id = ? ORDER BY state_id DESC LIMIT 1;",
        (state_metadata_id,),
    )
    (state,) = res.fetchone()
    res = cur.execute(
        "SELECT sum FROM statistics WHERE metadata_id = ? ORDER BY id DESC LIMIT 1;",
        (statistics_metadata_id,),
    )
    (sum,) = res.fetchone()
    return Decimal(state), Decimal(str(sum))


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
            FASCE = [1, 2, 3]
            luce_giornaliera(
                filename=args.csv, sensors=[f"lettura_luce_f{i}" for i in FASCE]
            )
        else:
            print(f"{args.csv} not recognized, exit.")

    con.commit()
    con.close()
