import pandas as pd


def query_1_preview_sales(conn):
    query = """
    SELECT * FROM sales
    ORDER BY order_id
    LIMIT 7;
    """
    return pd.read_sql(query, conn)


def query_2_dataset_summary(conn):
    query = """
    SELECT
        COUNT(DISTINCT order_id) as order_count,
        COUNT(DISTINCT customer_id) as customer_count,
        COUNT(DISTINCT region) as region_count,
        COUNT(DISTINCT product_category) as category_count,
        COUNT(DISTINCT product_name) as product_count,
        ROUND(AVG(quantity),2) as avg_quanity,
        ROUND(AVG(discount),2) as avg_discount
    FROM sales;
    """
    return pd.read_sql(query, conn)


def query_3_missing_unit_price(conn):
    query = """
    SELECT
        COUNT(*) - COUNT(unit_price) AS NaN_count
    FROM sales_clean;
    """
    return pd.read_sql(query, conn)


def query_4_missing_discount(conn):
    query = """
    SELECT
        COUNT(*) - COUNT(discount) AS NaN_count
    FROM sales_clean;
    """
    return pd.read_sql(query, conn)


def query_5_missing_revenue(conn):
    query = """
    SELECT
        COUNT(*) - COUNT(revenue) AS NaN_count
    FROM sales_clean;
    """
    return pd.read_sql(query, conn)


def query_6_missing_profit(conn):
    query = """
    SELECT
        COUNT(*) - COUNT(profit) AS NaN_count
    FROM sales_clean;
    """
    return pd.read_sql(query, conn)


def query_7_order_level_aggregates(conn):
    query = """
    WITH order_agg AS (
        SELECT
            order_id,
            ROUND(SUM(revenue),2) AS order_revenue,
            ROUND(SUM(profit),2) AS order_profit
        FROM sales_clean
        GROUP BY order_id
    )
    SELECT
        ROUND(AVG(order_revenue),2) AS avg_revenue,
        ROUND(AVG(order_profit),2) AS avg_profit,
        ROUND(AVG(order_profit) FILTER (
            WHERE order_profit > (
                SELECT AVG(order_profit) FROM order_agg
            )
        ),2) AS avg_profit_above_mean,
        ROUND(AVG(order_profit) FILTER (
            WHERE order_profit < (
                SELECT AVG(order_profit) FROM order_agg
            )
        ),2) AS avg_profit_below_mean,
        ROUND(
            (SELECT order_profit
             FROM order_agg
             ORDER BY order_profit
             LIMIT 1 OFFSET (SELECT COUNT(*)/2 FROM order_agg)),2
        ) AS median_profit
    FROM order_agg;
    """
    return pd.read_sql(query, conn)


def query_8_average_order_value(conn):
    query = """
    SELECT 
        ROUND(AVG(order_total),2) AS avg_order_value
    FROM (
        SELECT
            order_id,
            SUM(revenue) AS order_total
        FROM sales_clean
        GROUP BY order_id
    ) AS orders;
    """
    return pd.read_sql(query, conn)


def query_9_top_products_by_category(conn):
    query = """
    SELECT * FROM (
        SELECT
            product_category,
            product_name,
            ROUND(SUM(revenue),2) AS total_revenue,
            RANK() OVER (
                PARTITION BY product_category
                ORDER BY SUM(revenue) DESC
            ) AS revenue_rank
        FROM sales_clean
        GROUP BY product_category, product_name
    )
    WHERE revenue_rank <= 3
    ORDER BY product_category, revenue_rank;
    """
    return pd.read_sql(query, conn)


def query_10_top_products_overall(conn):
    query = """
    SELECT
        product_category,
        product_name,
        ROUND(SUM(revenue),2) AS total_revenue
    FROM sales_clean
    GROUP BY product_category, product_name
    ORDER BY total_revenue DESC
    LIMIT 5;
    """
    return pd.read_sql(query, conn)


def query_11_best_selling_products(conn):
    query = """
    SELECT
        product_category,
        product_name,
        SUM(quantity) AS total_units_sold,
        ROUND(AVG(unit_price),2) AS avg_unit_price
    FROM sales_clean
    GROUP BY product_category, product_name
    ORDER BY total_units_sold DESC
    LIMIT 5;
    """
    return pd.read_sql(query, conn)


def query_12_category_performance(conn):
    query = """
    SELECT
        product_category,
        ROUND(SUM(revenue),2) AS total_revenue,
        ROUND(SUM(profit),2) AS total_profit
    FROM sales_clean
    GROUP BY product_category
    ORDER BY total_revenue DESC;
    """
    return pd.read_sql(query, conn)


def query_13_regional_performance(conn):
    query = """
    SELECT
        region,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(SUM(profit), 2) AS total_profit,
        RANK() OVER (ORDER BY SUM(profit) DESC) AS profit_rank,
        RANK() OVER (ORDER BY SUM(revenue) DESC) AS revenue_rank
    FROM sales_clean
    GROUP BY region
    ORDER BY profit_rank;
    """
    return pd.read_sql(query, conn)
