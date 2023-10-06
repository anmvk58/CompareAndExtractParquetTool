from src.common.config_reader import Config_reader
from src.connector.DB2_connector import DB2Connector
from src.connector.Oracle_connector import OracleConnector
from src.connector.Mssql_connector import MssqlConnector
from src.connector.Redshift_connector import RedShiftConnector
from src.common.default_var import DefaultVar

class Connector_factory:

    @staticmethod
    def create_connector(src_name, config_path):
        config = Config_reader(config_path=config_path)
        connection_string = config.get_info_database(src_name=src_name)
        engine = connection_string['engine']

        if engine in ["DB2"]:
            return DB2Connector(src_name, config_path)

        elif engine in ["MSSQL"]:
            # return MSSQLConnector
            return MssqlConnector(src_name, config_path)

        elif engine in ["ORACLE"]:
            # return ORAConnector
            return OracleConnector(src_name, config_path)

        elif engine in ["REDSHIFT"]:
            # return Redshift connector
            return RedShiftConnector(src_name, config_path)

        else:
            # logger.error("Not founded: engine %s" % engine)
            raise NotImplementedError("Not founded: engine %s" % engine)
import pyodbc

if __name__ == '__main__':
    # db2connector = Connector_factory.create_connector('VPB_STAG_OTHER', config_path='../connection_defi.json')
    ora_cnn = Connector_factory.create_connector('REDSHIFT_TEST', config_path=DefaultVar.DEV_ENV )
    df = ora_cnn.read_sql_query("""
        select * from edz_eod.cake_cms_acnt_contract__s2 limit 1
    """)
    print(df)









