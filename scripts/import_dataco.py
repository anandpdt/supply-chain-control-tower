
from pathlib import Path
from getpass import getpass
import re
import zipfile

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# 1. Locate the original downloaded dataset
project_dir = Path(__file__).resolve().parents[1]
zip_path = project_dir / "data" / "archive.zip"

# 2. Ask for PostgreSQL password securely
password = getpass("Enter PostgreSQL password: ")

db_url = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password=password,
    host="localhost",
    port=5432,
    database="supply_chain_db"
)

engine = create_engine(db_url)

# 3. Exclude unnecessary personal and empty columns
exclude = {
    "Customer Email",
    "Customer Password",
    "Customer Fname",
    "Customer Lname",
    "Customer Street",
    "Customer Zipcode",
    "Latitude",
    "Longitude",
    "Product Image",
    "Product Description",
    "Order Zipcode"
}

total_rows = 0

try:
    # 4. Open CSV inside the ZIP
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open("DataCoSupplyChainDataset.csv") as file:

            chunks = pd.read_csv(
                file,
                encoding="latin1",
                chunksize=5000,
                low_memory=False
            )

            for i, df in enumerate(chunks):

                # Remove unused or sensitive fields
                df = df.drop(columns=list(exclude))

                # Convert column names to snake_case
                df.columns = [
                    re.sub(
                        r"[^a-z0-9]+", "_",
                        col.strip().lower()
                    ).strip("_")
                    for col in df.columns
                ]

                # Simplify date column names
                df = df.rename(columns={
                    "order_date_dateorders": "order_at",
                    "shipping_date_dateorders": "shipped_at"
                })

                # Convert text dates to timestamps
                for col in ["order_at", "shipped_at"]:
                    df[col] = pd.to_datetime(
                        df[col],
                        format="%m/%d/%Y %H:%M"
                    )

                # First chunk creates the staging table;
                # later chunks append records.
                mode = "replace" if i == 0 else "append"

                df.to_sql(
                    "stg_dataco_orders",
                    engine,
                    if_exists=mode,
                    index=False,
                    chunksize=1000
                )

                total_rows += len(df)
                print(f"Imported {total_rows:,} rows")

    # 5. Verify the import
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM stg_dataco_orders")
        ).scalar()

    print(f"PostgreSQL contains {count:,} rows")

finally:
    engine.dispose()
