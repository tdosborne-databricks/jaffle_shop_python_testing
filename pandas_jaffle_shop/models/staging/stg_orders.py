def model(dbt, session):

    orders_renames = {"id": "order_id", "user_id": "customer_id"}
    orders_renames = {
        key.lower(): value.lower() for key, value in orders_renames.items()
    }

    raw_orders = dbt.ref("raw_orders")
    raw_orders = (
        raw_orders.to_pandas_on_spark()
    )  # see https://github.com/dbt-labs/dbt-core/issues/5646

    orders = raw_orders.rename(columns=orders_renames)

    return orders
