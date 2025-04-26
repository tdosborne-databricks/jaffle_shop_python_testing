def model(dbt, session):

    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]

    order_payments_renames = {
        f"{payment_method}": f"{payment_method}_amount"
        for payment_method in payment_methods
    }
    order_payments_renames = {
        key: value.lower() for key, value in order_payments_renames.items()
    }

    orders_renames = {"total_amount": "amount"}
    orders_renames = {
        key.lower(): value.lower() for key, value in orders_renames.items()
    }

    stg_orders = dbt.ref("stg_orders")
    stg_orders = stg_orders.pandas_api()

    stg_payments = dbt.ref("stg_payments")
    stg_payments = stg_payments.pandas_api()

    stg_customers = dbt.ref("stg_customers")
    stg_customers = stg_customers.pandas_api()

    order_payments_totals = stg_payments.groupby("order_id").agg(
        amount=("amount", "sum")
    )

    order_payments = (
        stg_payments.groupby(["order_id", "payment_method"])
        .agg(payment_method_amount=("amount", "sum"))
        .reset_index()
        .pivot(
            index="order_id",
            columns="payment_method",
            values="payment_method_amount".lower(),
        )
        .rename(columns=order_payments_renames)
        .merge(order_payments_totals, on="order_id", how="left")
        .reset_index()
    )

    orders = stg_orders.merge(order_payments, on="order_id", how="left").rename(
        columns=orders_renames
    )
    orders = orders.fillna(0)  # hacked the mainframe (fixes tests)

    return orders
