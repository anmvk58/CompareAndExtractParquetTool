import redshift_connector
import pandas as pd
from src.connector.connector import Connector
from src.common.dictionary_convert import DictionaryConvert

class RedShiftConnector(Connector):
    @property
    def driver(self) -> str:
        # because Redshift dont use ODBC driver
        return None

    def create_connection(self):
        conn = redshift_connector.connect(
            host=self.server,
            database=self.database,
            port=int(self.port),
            user=self.user,
            password=self.pw
        )
        return conn

    def make_query_limit_1(self, query) -> str:
        return "SELECT * FROM " + query + " limit 1"

    def get_mapping_df(self) -> pd.DataFrame:
        # trong return connection khong co length và scale.
        # Chắc phải read from metadata
        return None


    #fix header b'abc' => abc
    def read_sql_query(self, query) -> pd.DataFrame:
        df = pd.read_sql_query(query, self.connection[self.src_name], coerce_float=False)
        list_col = []
        for item in df.columns.values.tolist():
            list_col.append(item.decode("utf-8"))
        df.columns = list_col
        return df
