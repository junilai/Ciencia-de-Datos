"""Carga un mes de NYC Yellow Taxi a NYC_TAXI.RAW.YELLOW_TRIPDATA (idempotente).

Uso: python load_month.py YYYY-MM
Salida: "LOADED <periodo> rows=<n>" | "NOT_PUBLISHED <periodo>". Exit 0 en ambos casos, 1 en error.
"""
import os
import re
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import requests
import snowflake.connector

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{period}.parquet"
FIRST_PERIOD = (2025, 1)
LAST_PERIOD = (2026, 8)
PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
NOT_PUBLISHED_STATUS = {403, 404}


def expected_periods() -> list[str]:
    periods = []
    year, month = FIRST_PERIOD
    while (year, month) <= LAST_PERIOD:
        periods.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return periods


def validate_period(period: str) -> str:
    if not PERIOD_RE.match(period):
        raise ValueError(f"Período inválido '{period}', se espera YYYY-MM")
    return period


def source_url(period: str) -> str:
    return BASE_URL.format(period=period)


def is_published(url: str, session) -> bool:
    status = session.head(url, timeout=30, allow_redirects=True).status_code
    if status == 200:
        return True
    if status in NOT_PUBLISHED_STATUS:
        return False
    raise RuntimeError(f"HEAD {url} devolvió HTTP {status}")


def verify_row_count(expected: int, loaded: int, period: str) -> None:
    if expected != loaded:
        raise RuntimeError(f"{period}: parquet tiene {expected} filas pero se cargaron {loaded}")


def download(url: str, dest: Path, session) -> None:
    with session.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
    )


def load_into_snowflake(conn, local_path: Path, period: str, expected_rows: int) -> int:
    stage_dir = f"@RAW.TLC_STAGE/yellow/{period}/"
    cur = conn.cursor()
    try:
        cur.execute(f"PUT 'file://{local_path}' {stage_dir} AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        cur.execute("BEGIN")
        cur.execute("DELETE FROM RAW.YELLOW_TRIPDATA WHERE _SOURCE_PERIOD = %s", (period,))
        cur.execute(
            f"""
            COPY INTO RAW.YELLOW_TRIPDATA
            FROM {stage_dir}
            FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (_SOURCE_FILE = METADATA$FILENAME, _LOADED_AT = METADATA$START_SCAN_TIME)
            FORCE = TRUE
            """
        )
        cur.execute(
            "UPDATE RAW.YELLOW_TRIPDATA SET _SOURCE_PERIOD = %s WHERE _SOURCE_PERIOD IS NULL",
            (period,),
        )
        cur.execute("SELECT COUNT(*) FROM RAW.YELLOW_TRIPDATA WHERE _SOURCE_PERIOD = %s", (period,))
        loaded = cur.fetchone()[0]
        verify_row_count(expected_rows, loaded, period)
        cur.execute("COMMIT")
        return loaded
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        cur.close()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 1
    period = validate_period(argv[1])
    url = source_url(period)
    session = requests.Session()

    if not is_published(url, session):
        print(f"NOT_PUBLISHED {period}")
        return 0

    with tempfile.TemporaryDirectory() as tmp:
        local_path = Path(tmp) / f"yellow_tripdata_{period}.parquet"
        download(url, local_path, session)
        expected_rows = pq.ParquetFile(local_path).metadata.num_rows
        conn = connect()
        try:
            loaded = load_into_snowflake(conn, local_path, period, expected_rows)
        finally:
            conn.close()

    print(f"LOADED {period} rows={loaded}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
