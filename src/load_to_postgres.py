import pandas as pd
from sqlalchemy import create_engine, text

PARQUET_PATH = "data/processed/transactions.parquet"

DB_URL = (
    "postgresql+psycopg2://"
    "fraud_user:fraud_password@127.0.0.1:55432/fraud_db"
)


def load_data():
    df = pd.read_parquet(PARQUET_PATH)

    engine = create_engine(DB_URL)

    df.to_sql(
        "transactions",
        engine,
        if_exists="replace",
        index=False
    )

    print(f"Loaded {len(df)} rows into PostgreSQL.")

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT
                    COUNT(*) AS total_rows,
                    MIN("Amount") AS min_amount,
                    MAX("Amount") AS max_amount,
                    SUM("Class") AS fraud_count
                FROM transactions
            """)
        )

        print("Validation:", result.fetchone())


if __name__ == "__main__":
    load_data()