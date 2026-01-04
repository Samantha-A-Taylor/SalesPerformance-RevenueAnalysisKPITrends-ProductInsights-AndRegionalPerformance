from database import create_connection
import sql_queries as q

def run_all(df):
    conn = create_connection(df)

    q.query_1_preview_sales(conn)
    q.query_2_dataset_summary(conn)
    q.query_7_order_level_aggregates(conn)
    q.query_13_regional_performance(conn)

    return conn
