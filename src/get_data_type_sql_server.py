import pandas as pd
from src.connector.connector_factory import Connector_factory
from src.common.default_var import DefaultVar

if __name__ == '__main__':
    cnn_s16 = Connector_factory.create_connector("JAVIS", config_path=DefaultVar.DEV_ENV)
    sql_s16 = """
           Select KYC_STEP , CONTRACT_ID , STEP_HISTORY , CIF_T24,  HAS_RESULT_WS4, COUNT_INFO , COUNT_PHONE , PARTNER_LIMIT , IMAGE_ID_FRONT , COUNT_OCR_FRONT , COUNT_OCR_BACK
            from JARVIS_CUSTOMER
            Where TRUNC(LAST_MODIFIED_TIME) = TO_DATE('20221204', 'YYYYMMDD')
       """

    cursor = cnn_s16.connection["JAVIS"].cursor()
    q = cursor.execute(sql_s16).fetchone()
    list = cursor.description

    meta_data2 = pd.DataFrame(list)
    headers = ["COL_NAME", "DATA_TYPE", "X", "LENGTH", "PRECISION", "SCALE", "Y"]
    meta_data2.columns = headers
    meta_data2["DATA_TYPE"] = meta_data2.apply(
        lambda x: x["DATA_TYPE"].__name__, axis=1
    )
    print(meta_data2)