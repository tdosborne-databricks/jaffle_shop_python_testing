# stolen from https://medium.com/snowflake/a-first-look-at-the-dbt-python-models-with-snowpark-54d9419c1c72
# hopefully under the "you posted it in a medium blog" license? don't sue me

import snowflake.snowpark.functions as f

from snowflake.snowpark.functions import col


def model(dbt, session):
    dbt.config(materialized="table")

    stg_customers = dbt.ref("stg_customers")
    stg_orders = dbt.ref("stg_orders")
    stg_payments = dbt.ref("stg_payments")

    customer_orders = stg_orders.group_by(col("customer_id")).agg(
        [
            f.min(col("order_date")).alias("first_order"),
            f.max("order_date").alias("most_recent_order"),
            f.count("order_id").alias("number_of_orders"),
        ]
    )
    customer_payments = (
        stg_payments.join(
            stg_orders,
            stg_orders["order_id"] == stg_payments["order_id"],
            join_type="left",
        )
        .select(stg_orders["customer_id"], stg_payments["amount"])
        .group_by(stg_orders["customer_id"])
        .agg([f.sum(stg_payments["amount"]).alias("total_amount")])
    )
    final = (
        stg_customers.join(
            customer_orders,
            customer_orders["customer_id"] == stg_customers["customer_id"],
            join_type="left",
        )
        .drop(customer_orders["customer_id"])
        .with_column_renamed(stg_customers["customer_id"], "customer_id")
        .join(
            customer_payments,
            stg_customers["customer_id"] == customer_payments["customer_id"],
            join_type="left",
        )
        .drop(customer_payments["customer_id"])
        .with_column_renamed(stg_customers["customer_id"], "customer_id")
        .with_column_renamed(
            customer_payments["total_amount"], "customer_lifetime_value"
        )
    )
    return final
