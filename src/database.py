import sqlite3
import pandas as pd

def create_connection(df: pd.DataFrame, db_path=":memory:"):
    """
    Load DataFrame into a SQLite database.
    """
    conn = sqlite3.connect(db_path)
    df.to_sql("sales", conn, index=False, if_exists="replace")
    return conn
