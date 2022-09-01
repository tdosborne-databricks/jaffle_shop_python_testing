def model(dbt, session):

    raw_payments = dbt.ref("raw_payments").to_pandas()

    payments_renames = {"ID": "PAYMENT_ID"}
    payments = raw_payments.rename(columns=payments_renames)

    # -- `amount` is currently stored in cents, so we convert it to dollars
    payments["AMOUNT"] /= 100

    return payments
