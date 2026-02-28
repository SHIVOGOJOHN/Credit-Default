"""
utils/db.py
------------
MySQL connection and data upload utilities using mysql-connector-python.
"""

import os
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("DB")


def get_connection():
    """
    Create and return a MySQL connection using .env credentials.

    .env variables required:
        MYSQL_HOST=localhost
        MYSQL_PORT=3306
        MYSQL_USER=root
        MYSQL_PASSWORD=yourpassword
        MYSQL_DATABASE=deta_bank
    """
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE", "deta_bank"),
    )


def ensure_table_exists(df: pd.DataFrame, table_name: str = "customer_defaults"):
    """
    Auto-create the MySQL table based on DataFrame columns if it doesn't exist.
    Maps pandas dtypes → MySQL column types.
    """
    type_map = {
        "int64":   "BIGINT",
        "float64": "DOUBLE",
        "bool":    "TINYINT(1)",
        "object":  "VARCHAR(255)",
        "datetime64[ns]": "DATETIME",
    }

    col_definitions = []
    for col, dtype in df.dtypes.items():
        mysql_type = type_map.get(str(dtype), "VARCHAR(255)")
        col_definitions.append(f"`{col}` {mysql_type}")

    # Add metadata columns
    col_definitions += [
        "`uploaded_by` VARCHAR(100)",
        "`uploaded_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    ]

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(col_definitions)}
        );
    """

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    conn.close()


def upload_dataframe(
    df: pd.DataFrame,
    uploaded_by: str,
    table_name: str = "customer_defaults",
) -> dict:
    """
    Upload a pandas DataFrame to MySQL row by row using executemany.

    Args:
        df:           DataFrame to upload
        uploaded_by:  Username of the data scientist uploading
        table_name:   Target MySQL table name

    Returns:
        dict with rows_inserted and any errors
    """
    # Add metadata
    df = df.copy()
    df["uploaded_by"] = uploaded_by

    # Ensure table exists
    ensure_table_exists(df, table_name)

    conn = get_connection()
    cursor = conn.cursor()

    cols = [f"`{c}`" for c in df.columns]
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO `{table_name}` ({', '.join(cols)}) VALUES ({placeholders})"

    # Convert DataFrame rows to list of tuples
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
        rows_inserted = cursor.rowcount
        return {"success": True, "rows_inserted": rows_inserted, "error": None}
    except Error as e:
        conn.rollback()
        log.error(f"MySQL upload failed: {e}")
        return {"success": False, "rows_inserted": 0, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


def fetch_recent_uploads(table_name: str = "customer_defaults", limit: int = 100) -> pd.DataFrame:
    """
    Fetch the most recently uploaded rows from MySQL.
    """
    try:
        conn = get_connection()
        query = f"SELECT * FROM `{table_name}` ORDER BY uploaded_at DESC LIMIT {limit}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Error as e:
        log.error(f"Fetch failed: {e}")
        return pd.DataFrame()