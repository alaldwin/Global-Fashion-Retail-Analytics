import os
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv

from src.common.logger import get_logger

load_dotenv()

logger = get_logger(__name__, "loaded.log")

logger.info("LOAD SCANNING...")


def batch_exists(table_name, batch_date):

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    cur = conn.cursor()

    try:
        cur.execute(
            f"""
            SELECT EXISTS(
                SELECT 1
                FROM {table_name}
                WHERE batch_date = %s
            )
            """,
            (batch_date,)
        )

        exists = cur.fetchone()[0]

    except errors.UndefinedTable:
        conn.rollback()
        exists = False

    finally:
        cur.close()
        conn.close()

    return exists


def write_postgresql(tables, batch_date):

    url = (
        f"jdbc:postgresql://"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    print("=" * 80)
    print("JDBC URL:", url)
    print("Database:", os.getenv("DB_NAME"))
    print("=" * 80)

    for table_name, df in tables.items():

        print("\n" + "=" * 80)
        print(f"TABLE: {table_name}")
        print("=" * 80)

        df.printSchema()
        df.show(5, truncate=False)

        if batch_exists(table_name, batch_date):
            print(f"Skipping {table_name}: batch_date={batch_date} already loaded.")
            continue

        print(f"Loading {table_name}...")

        try:

            (
                df.write
                .mode("append")
                .jdbc(
                    url=url,
                    table=table_name,
                    properties=properties
                )
            )

            print(f"✓ {table_name} loaded successfully.")

        except Exception as e:
            import traceback

            traceback.print_exc()

            print("\n===== JAVA ERROR =====")
            print(e)

            if hasattr(e, "java_exception"):
                e.java_exception.printStackTrace()

            raise

    print("\nAll tables loaded successfully.")