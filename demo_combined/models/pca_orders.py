# imports
import numpy as np
import pandas as pd

from sklearn.decomposition import PCA

# define dbt model for this file
def model(dbt, session):

    # configure packages to install
    dbt.config(packages=["scikit-learn", "numpy", "pandas"])

    # contrived: ref all orders to show in DAG, use one
    orders = dbt.ref("orders_sql").to_pandas()
    orders = dbt.ref("orders_snowpark").to_pandas()
    orders = dbt.ref("orders_pandas").to_pandas()

    # prepare the data for statistical analytics
    orders = prep_data(orders)

    # perform PCA
    orders_pca = pca_transform(orders)

    # return the result as the dbt model for this file
    return orders_pca


# helper functions
def prep_data(df):

    return df.drop("ORDER_DATE", axis=1).drop("STATUS", axis=1)


def pca_transform(df):

    pca = PCA(n_components=2)
    pca.fit(df)

    result = pca.transform(df)

    return pd.DataFrame(result, columns=["PC1", "PC2"])
