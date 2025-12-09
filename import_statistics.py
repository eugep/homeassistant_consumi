import argparse
import csv
from datetime import datetime
from decimal import Decimal
import sqlite3


class Lettura:
    def __init__(self, data_lettura: datetime) -> None:
        self.data_lettura: datetime = data_lettura

    @property
    def lettura(self) -> Decimal:
        return NotImplemented

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
    DATA_LETTURA_KEY = "DATA LETTURA"
    DATA_LETTURA_FORMAT = "%Y-%m-%d"

    def __init__(self, LETTURA, **kwargs) -> None:
        super().__init__(
            datetime.strptime(kwargs[self.DATA_LETTURA_KEY], self.DATA_LETTURA_FORMAT),
        )
        self._lettura = Decimal(LETTURA.lstrip("0"))

    @property
    def lettura(self) -> Decimal:
        return self._lettura

    def __repr__(self) -> str:
        return f"{self.data_lettura} - {self.lettura} m3"


class LetturaLuce(Lettura):
    DATA_LETTURA_FORMAT = "%d/%m/%Y"

    def __init__(
        self,
        data_lettura: str,
        lettura_f1: str,
        lettura_f2: str,
        lettura_f3: str,
        **kwargs,
    ) -> None:
        super().__init__(datetime.strptime(data_lettura, self.DATA_LETTURA_FORMAT))
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
    letture: list[LetturaGas] | list[LetturaLuce],
    statistics_metadata_id: int,
    state_metadata_id: int,
) -> None:
    state, sum = get_latest_state_and_sum(statistics_metadata_id=statistics_metadata_id)
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
    with open(filename, "r") as f:
        letture = []
        for line in csv.DictReader(f, delimiter=";"):
            try:
                letture.append(LetturaGas(**line))
            except:
                continue
        letture.sort()
        process_lines(
            letture=letture,
            **get_metadata_ids(sensor=sensor),
        )


def luce_giornaliera(filename: str, sensors: list[str]) -> None:
    FASCE = [1, 2, 3]

    with open(filename, "r") as f:
        letture = [LetturaLuce(**line) for line in csv.DictReader(f, delimiter=";")]
        letture.sort()
        for fascia, sensor in zip(FASCE, sensors):
            for lettura in letture:
                lettura.default = fascia
            process_lines(
                letture=letture,
                **get_metadata_ids(sensor=sensor),
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
