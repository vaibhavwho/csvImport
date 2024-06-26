import pandas as pd
from sqlalchemy import create_engine
import dask.dataframe as dd

from constants import connection_string


def get_employer_dataframe(client_id: int) -> pd.DataFrame:

    query = "SELECT employer_id FROM tbl_ph_employer_info WHERE client_id = {}".format(client_id)
    engine = create_engine(connection_string)
    employer_df = pd.read_sql_query(query, con=engine)
    return employer_df


def get_provider_dataframe():

    ddf = dd.read_sql_table(
        table_name='tbl_ph_providers',
        con=connection_string,
        index_col='provider_id',
        columns=['provider_id', 'provider_number']
    )

    provider_df = ddf.compute()
    return provider_df
