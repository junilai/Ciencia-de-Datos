"""Imprime filas por período en RAW.YELLOW_TRIPDATA (para verificar idempotencia)."""
from load_month import connect


def main() -> None:
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT _SOURCE_PERIOD, COUNT(*) FROM RAW.YELLOW_TRIPDATA GROUP BY 1 ORDER BY 1"
        )
        for period, rows in cur.fetchall():
            print(f"{period}\t{rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
