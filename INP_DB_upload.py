# -*- coding: utf-8 -*-

import sys
import config
import INP_header
sys.path.append("./")

import pandas as pd
from openpyxl import load_workbook
from sqlalchemy import create_engine

# mysql DB connect
db_connection_str = f"mysql+pymysql://{config.DATABASE_CONFIG['user']}:{config.DATABASE_CONFIG['password']}@{config.DATABASE_CONFIG['host']}/{config.DATABASE_CONFIG['dbname']}"
db_connection = create_engine(db_connection_str)

# Pricing Model INP 데이터 DB 업로드 Function
def excel_to_sql(file_name,defiend_name):
    wb = load_workbook(f"{file_name}", data_only=True)
    defiend_name_object = wb.defined_names[defiend_name].destinations
    for title, coord in defiend_name_object:
        ws = wb[title][coord]
        '''PDT INFO 설계보험가입금액 추가'''
        if defiend_name == 'PDT_INFO':
            new_coord = f"$B$33:$O$174"
            ws = wb[title][new_coord]

    sheet_list = list()
    for row in ws:
        row_list = list()
        for cell in row:
            row_list.append(cell.value)
        sheet_list.append(row_list)
    if defiend_name == 'PDT_INFO':
        sheet_df = pd.DataFrame(sheet_list, columns=INP_header.PDT_INFO_HEADER)
    elif defiend_name == 'RoRISK_INFO':
        sheet_df = pd.DataFrame(sheet_list, columns=INP_header.RoRISK_INFO_HEADER)
    elif defiend_name == 'ETC_PDT_INFO':
        sheet_df = pd.DataFrame(sheet_list, columns=INP_header.ETC_PDT_INFO_HEADER)
    elif defiend_name == 'Calc_S_INFO':
        sheet_df = pd.DataFrame(sheet_list, columns=INP_header.Calc_S_INFO_HEADER)
    elif defiend_name == 'PV_Files_INFO':
        sheet_df = pd.DataFrame(sheet_list, columns=INP_header.PV_Files_INFO_HEADER)    

    sheet_df.to_sql(name=defiend_name, con=db_connection, if_exists='append', index=False)

# Function 실행
if __name__ == "__main__":
    excel_to_sql("./PricingModel_230919_sample.xlsm","PDT_INFO")
    excel_to_sql("./PricingModel_230919_sample.xlsm","RoRISK_INFO")
    excel_to_sql("./PricingModel_230919_sample.xlsm","ETC_PDT_INFO")
    excel_to_sql("./PricingModel_230919_sample.xlsm","Calc_S_INFO")
    excel_to_sql("./PricingModel_230919_sample.xlsm","PV_Files_INFO")