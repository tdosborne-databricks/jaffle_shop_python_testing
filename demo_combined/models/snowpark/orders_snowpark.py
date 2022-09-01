# stolen from https://medium.com/snowflake/a-first-look-at-the-dbt-python-models-with-snowpark-54d9419c1c72
# hopefully under the "you posted it in a medium blog" license? don't sue me

import snowflake.snowpark.functions as f

from decimal import Decimal
from functools import reduce

from snowflake.snowpark.functions import col, iff, lit

payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]


def model(dbt, session):
    dbt.config(materialized="table")

    stg_orders = dbt.ref("stg_orders_snowpark")
    stg_payments = dbt.ref("stg_payments_snowpark")

    payment_types = (
        stg_payments.drop("payment_id")
        .pivot("PAYMENT_METHOD", payment_methods)
        .sum("AMOUNT")
        .na.fill(Decimal(0))
    )
    renamed = renameColumns(payment_types)

    order_payments = stg_payments.group_by(col("order_id")).agg(
        [f.sum(col("amount")).alias("total_amount")]
    )
    temp = (
        order_payments.join(renamed, renamed["order_id"] == order_payments["order_id"])
        .drop(order_payments["order_id"])
        .with_column_renamed(renamed["order_id"], "order_id")
    )
    final = (
        stg_orders.join(
            temp, stg_orders["order_id"] == temp["order_id"], join_type="left"
        )
        .drop(temp["order_id"])
        .with_column_renamed(stg_orders["order_id"], "order_id")
        .with_column_renamed(temp["total_amount"], "amount")
    )
    return final


def renameColumns(df):
    colscleaned = [
        col[2:-2] if col.startswith('"') and col.endswith('"') else col
        for col in df.columns
    ]
    cleaned = reduce(
        lambda df, idx: df.with_column_renamed(df.columns[idx], colscleaned[idx]),
        range(len(df.columns)),
        df,
    )

    colsrenamed = [
        col + "_AMOUNT" if col.lower() in payment_methods else col
        for col in cleaned.columns
    ]
    renamed = reduce(
        lambda cleaned, idx: cleaned.with_column_renamed(
            cleaned.columns[idx], colsrenamed[idx]
        ),
        range(len(cleaned.columns)),
        cleaned,
    )
    return renamed
