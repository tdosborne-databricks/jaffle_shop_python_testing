from snowflake.snowpark.functions import col


def model(dbt, session):

    stg_payments = dbt.ref("raw_payments")

    payments_renames = {"ID": "PAYMENT_ID"}

    # goofy
    stg_payments = stg_payments.select(
        *[
            col(col_name).as_(payments_renames[col_name])
            if col_name in payments_renames
            else col_name
            for col_name in stg_payments.schema.names
            if col_name != "AMOUNT"
        ],
        (stg_payments["amount"] / 100).as_("amount"),
    )
    return stg_payments
