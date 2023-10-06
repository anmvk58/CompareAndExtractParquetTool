import pandas as pd
from src.connector.connector_factory import Connector_factory
from src.common.default_var import DefaultVar
from src.compare_module.Compare_object import Compare_Object_Query

if __name__ == '__main__':
    query1 = """
        Select KYC_STEP , CONTRACT_ID , STEP_HISTORY , CIF_T24,  HAS_RESULT_WS4, COUNT_INFO , COUNT_PHONE , PARTNER_LIMIT , IMAGE_ID_FRONT , COUNT_OCR_FRONT , COUNT_OCR_BACK
        from DIGIT_F_JARVIS_CUSTOMER
    """
    coq1 = Compare_Object_Query(src_name='WHR2', query=query1)
    meta_data, total = coq1.cnn.get_meta_data_from_query(query1)
    print(meta_data)
    df1 = coq1.get_data_after_process()


