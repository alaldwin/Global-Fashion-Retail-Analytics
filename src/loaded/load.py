import os
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv

from src.common.logger import get_logger

load_dotenv()

logger = get_logger(__name__, "pipeline.log")

logger.info("LOADING SCANNING...")


def write_postgresql(tables, batch_date):

    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )

    print(engine)

    try:
        with engine.connect():
            print("✅ Connected to PostgreSQL")
    except Exception:
        import traceback
        traceback.print_exc()
        raise

    for tablename, df in tables.items():

        start = time.time()

        print(f"Converting {tablename}...")
        pdf = df.toPandas()
        print(f"toPandas: {time.time() - start:.2f}s")

        start = time.time()

        pdf.to_sql(
            name=tablename,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )

        print(f"to_sql: {time.time() - start:.2f}s")

    print("\nAll tables loaded successfully.")