def model(dbt, session):

    customers_renames = {"total_amount": "customer_lifetime_value"}
    customers_renames = {
        key.lower(): value.lower() for key, value in customers_renames.items()
    }

    stg_orders = dbt.ref("stg_orders")
    stg_orders = stg_orders.pandas_api()

    stg_payments = dbt.ref("stg_payments")
    stg_payments = stg_payments.pandas_api()

    stg_customers = dbt.ref("stg_customers")
    stg_customers = stg_customers.pandas_api()

    customer_orders = (
        stg_orders.groupby("customer_id".lower())
        .agg(
            first_order=("order_date".lower(), "min"),
            most_recent_order=("order_date".lower(), "max"),
            number_of_orders=("order_id".lower(), "count"),
        )
        .reset_index()
    )

    customer_payments = (
        stg_payments.merge(stg_orders, on="order_id".lower(), how="left")
        .groupby("customer_id".lower())
        .agg(total_amount=("amount".lower(), "sum"))
        .reset_index()
    )

    customers = (
        stg_customers.merge(customer_orders, on="customer_id".lower(), how="left")
        .merge(customer_payments, on="customer_id".lower(), how="left")
        .rename(columns=customers_renames)
    )

    return customers
