# imports
import numpy as np
import pandas as pd

from sklearn.decomposition import PCA

# define dbt model for this file
def model(dbt, session):

    dbt.config(packages=["scikit-learn", "numpy", "pandas"])

    orders = dbt.ref("orders_sql").to_pandas()
    orders = dbt.ref("orders_snowpark").to_pandas()
    orders = dbt.ref("orders_pandas").to_pandas()

    orders = prep_data(orders)

    orders_pca = pca_transform(orders)

    return orders_pca

def prep_data(df):

    return df.drop("ORDER_DATE", axis=1).drop("STATUS", axis=1)

def pca_transform(df):

    pca = PCA(n_components=2)
    pca.fit(df)

    result = pca.transform(df)

    return pd.DataFrame(result, columns=["PC1", "PC2"])
