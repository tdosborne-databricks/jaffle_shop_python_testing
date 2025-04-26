def model(dbt, session):
    payments_renames = {"id": "payment_id"}
    payments_renames = {
        key.lower(): value.lower() for key, value in payments_renames.items()
    }

    raw_payments = dbt.ref("raw_payments")
    raw_payments = (
        raw_payments.pandas_api()
    )  # see https://github.com/dbt-labs/dbt-core/issues/5646

    payments = raw_payments.rename(columns=payments_renames)
    # -- `amount` is currently stored in cents, so we convert it to dollars
    payments["amount"] /= 100

    return payments
