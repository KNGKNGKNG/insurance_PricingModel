import sys
import time
import datetime
import config
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
sys.path.append("./")

### mysql DB connect ###
db_connection_str = f"mysql+pymysql://{config.DATABASE_CONFIG['user']}:{config.DATABASE_CONFIG['password']}@{config.DATABASE_CONFIG['host']}/{config.DATABASE_CONFIG['dbname']}"
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

### INP Read For P_calc ###
db_PDT_INFO = conn.execute(text("SELECT * FROM pdt_info"))
db_ETC_PDT_INFO = conn.execute(text("SELECT * FROM etc_pdt_info"))
db_RoRISK_INFO = conn.execute(text("SELECT * FROM rorisk_info"))
db_Calc_S_INFO = conn.execute(text("SELECT * FROM calc_s_info"))
db_PV_Files_INFO = conn.execute(text("SELECT * FROM pv_files_info"))

PDT_INFO = pd.DataFrame(db_PDT_INFO.fetchall())
ETC_PDT_INFO = pd.DataFrame(db_ETC_PDT_INFO.fetchall())
RoRISK_INFO = pd.DataFrame(db_RoRISK_INFO.fetchall())
Calc_S_INFO = pd.DataFrame(db_Calc_S_INFO.fetchall())
PV_Files_INFO = pd.DataFrame(db_PV_Files_INFO.fetchall())

nowDate = datetime.datetime.now().strftime("%Y%m%d")

### 기본 정보 ###
'''1. 담보 개수'''
ASSR_CNT = 142
'''2. PV File 산출 유형 조건 개수'''
PV_File_TY_Cnt = 874
'''3. P Table 산출 날짜'''
APPL_DATE = nowDate

### POH_INFO Slot (가입설계정보) ###
'''ENT_AGE = 가입나이
GNDR = 성별구분 (1:남자, 2:여자)
INTY = 종별구분
LEV = 상해급수 (1:1급, 2:2급, 3:3급)
DRV = 운전여부 (1: 운전자, 2:비운전자)
DRTY = 운전형태 (1:자가용, 2:영업용)
RSTY = 요율구분 (1:최초계약, 2:갱신계약)
INSPD = 보험기간
RW_INSPD = 갱신형 보험기간
PMTPD = 납입기간
CYTY = 납입주기 (1:월납, 3:3개월납, 6:6개월납, 12:연납, 99:일시납)
INIT_ENT_AGE = 최초가입나이
FACE_AMT = 보험가입금액'''



# POH_INFO = {'ENT_AGE':[],
#             'GNDR':[],
#             'INTY':[],
#             'LEV':[],
#             'DRV':[],
#             'DRTY':[],
#             'RSTY':[],
#             'INSPD':[],
#             'RW_INSPD':[],
#             'PMTPD':[],
#             'CYTY':[],
#             'INIT_ENT_AGE':[],
#             'FACE_AMT':[]}

# create_num = 1이라면 생성No.가 1인 라인에 대한 PV table 가입조건 데이터프레임 또는 딕셔너리가 나와야 함. (create_num = 1 일때 1,116건의 가입설계조건이 산출되어야 함)
def get_poh_info(create_num):
    
    POH_INFO = pd.DataFrame(columns=['ENT_AGE', 'GNDR', 'INTY', 'LEV', 'DRV', 'DRTY', 'RSTY', 'INSPD', 'RW_INSPD', 'PMTPD', 'CYTY', 'INIT_ENT_AGE', 'FACE_AMT'])

    ''' 고정 변수 '''
    # POH_INFO['INTY'].append(0)
    inty = 0

    ''' 담보No. Load '''
    ASSR_NO = PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '담보No.'].values[0]
    
    ''' 담보No.별 기준보험가입금액(FACE_AMT) Load'''
    FACE_AMT = PDT_INFO.loc[PDT_INFO['담보No.']==ASSR_NO, '기준보험가입금액'].values[0]
    # POH_INFO['FACE_AMT'].append(FACE_AMT)

    ''' L01 '''
    L01_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '보험기간_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '보험기간_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '보험기간_step'].values[0])

    ''' L02 '''
    L02_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '납입기간_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '납입기간_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '납입기간_step'].values[0])
    
    ''' L03 '''
    L03_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '가입나이_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '가입나이_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '가입나이_step'].values[0])
    
    ''' L04 '''
    L04_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '납입주기_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '납입주기_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '납입주기_step'].values[0])

    ''' L05 '''
    L05_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '성별구분_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '성별구분_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '성별구분_step'].values[0])    
    
    ''' L06 '''
    L06_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '상해급수_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '상해급수_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '상해급수_step'].values[0])
    
    ''' L07 '''
    L07_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '운전형태_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '운전형태_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '운전형태_step'].values[0])

    ''' L08'''
    L08_range = range(PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '요율구분_begin'].values[0], PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '요율구분_end'].values[0]+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '요율구분_step'].values[0])

    for L01 in L01_range:
        for L02 in L02_range:
            for L03 in L03_range:
                ent_age = L03
                for L04 in L04_range:
                    cyty = L04
                    for L05 in L05_range:
                        gndr = L05
                        for L06 in L06_range:
                            lev = L06
                            for L07 in L07_range:
                                drty = L07
                                if L07 == 1 or L07 == 2:
                                    drv = 1
                                else:
                                    drv = 2
                                for L08 in L08_range:
                                    if L08 == 0:
                                        rsty = L08
                                        inspd = L01 - L03
                                        pmtpd = L02
                                        rw_inspd = 0
                                    elif L08 == 1:
                                        rsty = L08
                                        inspd = 0
                                        rw_inspd = L01
                                        pmtpd = rw_inspd
                                    else:
                                        if L01 == 100 or L01 == 90:
                                            if L01 - L03 < 1 or L01 - L03 > 19 or L01 - L03 == 10 or L01 - L03 == 15:
                                                break
                                            rsty = L08
                                            inspd = 0
                                            rw_inspd = L01 - L03
                                            pmtpd = rw_inspd
                                        else:
                                            if L01 + L03 > 100:
                                                break
                                            rsty = L08
                                            inspd = 0
                                            rw_inspd = L01
                                            pmtpd = rw_inspd
                                    
                                    tmp_S_idx = 0 if PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '최초가입나이_begin'].values[0] == 0 else 0
                                    tmp_E_idx = 0 if PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '최초가입나이_end'].values[0] == 0 else 0

                                    if PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '최초가입나이_begin'].values[0] == 0:
                                        tmp_S_idx = PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '최초가입나이_begin'].values[0]
                                        tmp_E_idx = PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '최초가입나이_end'].values[0]
                                    elif rw_inspd > 15:
                                        tmp_S_idx = 1
                                        tmp_E_idx = 5
                                    elif rw_inspd > 10:
                                        tmp_S_idx = 1
                                        tmp_E_idx = 14
                                    else:
                                        tmp_S_idx = 1
                                        tmp_E_idx = 16
                                    L09_range = range(tmp_S_idx, tmp_E_idx+1, PV_Files_INFO.loc[PV_Files_INFO['생성No.']==create_num, '최초가입나이_step'].values[0])
                                    for L09 in L09_range:
                                        if L09 == 0:
                                            init_ent_age = L09
                                            if ASSR_NO == 71 or ASSR_NO == 72:
                                                init_ent_age = L03
                                        else:
                                            if L01 == 100 or L01 == 90:
                                                if rw_inspd > 15:
                                                    case_dict = {1: 20, 2: 40, 3: 60, 4: 80, 5:100}
                                                    KK = case_dict.get(L09)
                                                    if L03 - KK < 19:
                                                        break
                                                    init_ent_age = L03 - KK
                                                elif rw_inspd > 10:
                                                    case_dict = {1: 15, 2: 20, 3: 30, 4: 35, 5: 40, 6: 45, 7: 50, 8: 55, 9: 60, 10: 65, 11: 70, 12: 75, 13: 80, 14: 85}
                                                    KK = case_dict.get(L09)
                                                    if L03 - KK < 19:
                                                        break
                                                    init_ent_age = L03 - KK
                                                else:
                                                    case_dict = {1: 10, 2: 15, 3: 20, 4: 25, 5: 30, 6: 35, 7: 40, 8: 45, 9: 50, 10: 55, 11: 60, 12: 65, 13: 70, 14: 75, 15: 80, 16: 85}
                                                    KK = case_dict.get(L09)
                                                    if L03 - KK < 19:
                                                        break
                                                    init_ent_age = L03 - KK
                                            else:
                                                if L03 - L01 * L09 < 19:
                                                    break
                                                init_ent_age = L03 - L01 * L09
                                        POH_INFO = pd.concat([POH_INFO, pd.DataFrame({'ENT_AGE': [ent_age], 'GNDR': [gndr], 'INTY': [inty], 'LEV': [lev], 'DRV': [drv], 'DRTY': [drty], 'RSTY': [rsty], 'INSPD': [inspd], 'RW_INSPD': [rw_inspd], 'PMTPD': [pmtpd], 'CYTY': [cyty], 'INIT_ENT_AGE': [init_ent_age], 'FACE_AMT': [FACE_AMT]})], ignore_index=True)
    return POH_INFO

# print(get_poh_info(310))

result_dfs = []
for i in range(1, PV_File_TY_Cnt+1):
    df = get_poh_info(i)
    result_dfs.append(df)

final_df = pd.concat(result_dfs, ignore_index=True)
print(final_df)

# POH_INFO = pd.DataFrame(columns=['ENT_AGE', 'GNDR', 'INTY', 'LEV', 'DRV', 'DRTY', 'RSTY', 'INSPD', 'RW_INSPD', 'PMTPD', 'CYTY', 'INIT_ENT_AGE', 'FACE_AMT'])
