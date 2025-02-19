import sys
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

### 기본 보험상품 정보 ###
'''1. 담보 개수'''
ASSR_CNT = 142
'''2. 적용위험율 개수'''
RoRisk_Cnt = 295
'''3. PV File 산출조건 유형개수'''
PV_File_TY_Cnt = 874

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


### POH_INFO Setting (가입설계정보 세팅) ###
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

POH_INFO = {'ENT_AGE':'35',
            'GNDR':'1',
            'INTY':None,
            'LEV':'1',
            'DRV':'1',
            'DRTY':'1',
            'RSTY':'1',
            'INSPD':'65',
            'RW_INSPD':'10',
            'PMTPD':'10',
            'CYTY':'1',
            'INIT_ENT_AGE':'25',
            'FACE_AMT':[]}



### P_cal 데이터프레임 ###
P_Cal = pd.DataFrame(columns=['ASSR_CD', 'INTY', 'INSPD', 'PMTPD', 'CYTY', 'POH_TYCD', 'GNDR', 'ENT_AGE', 'LEV', 'DRTY', 'FACE_AMT', 'RSTY', 'INIT_ENT_AGE', 'G_Pm', 'N_Pm', 'PDT_S_N_Pm', 'PDT_S_G_Pm', 'PDT_S_N_Pm_Rnd', 'PDT_S_G_Pm_Rnd', 'FACE_AMT_N_Pm', 'FACE_AMT_G_Pm', 'N_Pm_Y', 'G_Pm_Y', 'k_N_Pm_Y', 'S_ALPH', 'A_ALPH', 'PDT_S_S_ALPH', 'PDT_S_A_ALPH', 'PDT_S_S_ALPH_Rnd', 'PDT_S_A_ALPH_Rnd', 'ALPH', 'PDT_S_ALPH', 'PDT_S_ALPH_Rnd', 'FACE_AMT_ALPH'])


### 1. 특약코드(ASSR_CD) 데이터프레임 생성 ###
''' 담보 별 특약코드 추출
for i in range(1,ASSR_CNT+1):
    ASSR_CD = PDT_INFO.loc[PDT_INFO['담보No.']==i,'특약코드']
    print(ASSR_CD)'''
ASSR_CD = PDT_INFO.loc[PDT_INFO['담보No.']==1,'특약코드']

### 2. 종형구분 코드(INTY) = '00' ###


### 3. 보험기간(NN)_INSPD 산출 ###
'''def NN_calc(ASSR_NO):
    if PDT_INFO.loc[PDT_INFO['담보No.']==ASSR_NO, ['갱신형여부']].values[0][0]==1:
        if ASSR_NO >= 54 and ASSR_NO <=60:
            NN = 80 - int(POH_INFO['ENT_AGE'])
        else:
            NN = int(POH_INFO['INSPD'])
    else:
        NN = int(POH_INFO['RW_INSPD'])
    return NN'''
NN = []
for i in range(1,ASSR_CNT+1):
    if PDT_INFO.loc[PDT_INFO['담보No.']==i, ['갱신형여부']].values[0][0]==1:
        if i >= 54 and i <= 60:
            NN.append(80-int(POH_INFO['ENT_AGE']))
        else:
            NN.append(int(POH_INFO['INSPD']))
    else:
        NN.append(int(POH_INFO['RW_INSPD']))
NN = pd.DataFrame(NN)

### 4. 납입기간(MM)_PMTPD 산출 ###
'''def MM_calc(ASSR_NO):
    if PDT_INFO.loc[PDT_INFO['담보No.']==ASSR_NO, ['갱신형여부']].values[0][0]==1:
        MM = int(POH_INFO['PMTPD'])
    else:
        MM = int(POH_INFO['RW_INSPD'])
    return MM'''
MM = []
for i in range(1,ASSR_CNT+1):
    if PDT_INFO.loc[PDT_INFO['담보No.']==i, ['갱신형여부']].values[0][0]==1:
        MM.append(int(POH_INFO['PMTPD']))
    else:
        MM.append(int(POH_INFO['RW_INSPD']))
MM = pd.DataFrame(MM)

### 5. 납입주기(CYTY) 산출 ###
'''def CYTY_calc():
    CYTY = int(12 / int(POH_INFO['CYTY']))
    return CYTY'''
CYTY = []
for i in range(1,ASSR_CNT+1):
    CYTY_value = int(12 / int(POH_INFO['CYTY']))
    CYTY.append(CYTY_value)
CYTY = pd.DataFrame(CYTY)

### 6. 피보험자관계구분 코드(POH_TYCD) = '00' ###

### 7. 성별구분(GNDR) 산출 ###
GNDR = []
for i in range(1,ASSR_CNT+1):
    GNDR_value = (int(POH_INFO['GNDR']))
    GNDR.append(GNDR_value)
GNDR = pd.DataFrame(GNDR)

### 8. 가입나이(ENT_AGE) 산출 ###
ENT_AGE = []
for i in range(1,ASSR_CNT+1):
    ENT_AGE_value = (int(POH_INFO['ENT_AGE']))
    ENT_AGE.append(ENT_AGE_value)
ENT_AGE = pd.DataFrame(ENT_AGE)

### 9. 상해급수(LEV) 산출 ###
LEV = []
for i in range(1,ASSR_CNT+1):
    LEV_value = (int(POH_INFO['LEV']))
    LEV.append(LEV_value)
LEV = pd.DataFrame(LEV)

### 10. 운전형태(DRTY) 산출 ###
DRTY = []
for i in range(1,ASSR_CNT+1):
    DRTY_value = (int(POH_INFO['DRTY']))
    DRTY.append(DRTY_value)
DRTY = pd.DataFrame(DRTY)

### 11. 보험가입금액(FACE_AMT) 산출 ###
'''가입설계정보(POH_INFO)에 담보 별 기준보험가입금액(FACE_AMT) 입력'''
for i in range(1,ASSR_CNT+1):
    PDT_S = PDT_INFO.loc[PDT_INFO['담보No.']==i, ['기준보험가입금액']].values[0][0]
    POH_INFO['FACE_AMT'].append(PDT_S)
FACE_AMT = pd.DataFrame(POH_INFO['FACE_AMT'])

### 12. 요율구분(신규/갱신구분)(RSTY) 산출 ###
RSTY = []
for i in range(1,ASSR_CNT+1):
    RSTY_value = (int(POH_INFO['RSTY']))
    RSTY.append(RSTY_value)
RSTY = pd.DataFrame(RSTY)

### 13. 최초가입나이(INIT_ENT_AGE) 산출 ###
INIT_ENT_AGE = []
for i in range(1,ASSR_CNT+1):
    INIT_ENT_AGE_value = (int(POH_INFO['INIT_ENT_AGE']))
    INIT_ENT_AGE.append(INIT_ENT_AGE_value)
INIT_ENT_AGE = pd.DataFrame(INIT_ENT_AGE)

### 14. 보험가입금액 1원 영업보험료(G_Pm) / 순보험료(N_Pm) 산출 ###
'''담보 별 보험기간 조건에 따른 적용이율 현가(Vc) 산출'''
'''def Vc_calc(ASSR_NO):
    if NN_calc(ASSR_NO) <= 15:
        Vc = 1 / (1 + ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==ASSR_NO, ['적용이율_15년이하']].values[0][0])
    else:
        Vc = 1 / (1 + ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==ASSR_NO, ['적용이율_15년초과']].values[0][0])
    return Vc'''
Vc = []
for i in range(1,ASSR_CNT+1):
    if NN.loc[i-1].values[0] <= 15:
        Vc_value = 1 / (1+ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==i, ['적용이율_15년이하']].values[0][0])
    else:
        Vc_value = 1 / (1+ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==i, ['적용이율_15년초과']].values[0][0])
    Vc.append(Vc_value)
Vc = pd.DataFrame(Vc)


'''담보 별 위험률 적용개수(Qx_No_CNT) 산출'''
'''def Qx_No_CNT_calc(ASSR_NO):
    Qx_No_CNT = 0
    for i in range(1,16):
        if ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==ASSR_NO, ['위험률_No._q'+str(i).zfill(2)+'x']].values[0][0] != None:
            Qx_No_CNT = Qx_No_CNT + 1
    return Qx_No_CNT
'''
Qx_No_CNT = []
for i in range(1,ASSR_CNT+1):
    result = 0
    for l in range(1,16):
        Qx_No_value = ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==i, ['위험률_No._q'+str(l).zfill(2)+'x']].count().values[0]
        if Qx_No_value != 0:
            result += 1
    Qx_No_CNT.append(result)

'''Mx 세팅'''
Mx_column_names = ['M'+str(i).zfill(2)+'x' for i in range(1,16)]
Mx = pd.DataFrame(columns=Mx_column_names, index=range(1, ASSR_CNT+1))

'''담보No.1; 기수표 세팅'''
PV_calc_table = pd.DataFrame()

'''경과 나이'''
# for i in range(ASSR_CNT):
#     NN_value = NN.loc[i,0]
'''담보No.1의 NN 값'''
NN_1 = NN.loc[0,0]

'''담보No.1의 T(경과년), T_AGE(경과나이) 값'''
T_AGE_1 = [] 
for i in range(NN_1+1):
    T_AGE_1_value = int(POH_INFO['ENT_AGE']) + i
    T_AGE_1.append(T_AGE_1_value)
T_AGE_1 = pd.DataFrame(T_AGE_1)

T_list = list(range(len(T_AGE_1)))
T_1 = pd.DataFrame(T_list)
PV_calc_table['경과년'] = T_1
PV_calc_table['경과나이'] = T_AGE_1

'''담보 별 탈퇴율/위험률 No. 정보 from ETC_PDT_INFO'''
# for i in range(1,ASSR_CNT+1):
    # ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==i, '탈퇴율_No._lq1x':'위험률_No._q15x']

lqx_qx_table1 = ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '탈퇴율_No._lq1x':'위험률_No._q15x']


'''담보 별 면책기간 정보 from ETC_PDT_INFO'''
# for i in range(1,ASSR_CNT+1):
    # ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==i, '면책기간_C01x':'면책기간_C15x']
'''담보 No.1 지급배수'''
pay_rate_1 = ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '지급배수_M01x':'지급배수_M15x'].values[0]
pay_rate_1_value = []
for i in range(len(pay_rate_1)):
    if pay_rate_1[i] == None:
        pay_rate_1_value.append(0)
    else:
        pay_rate_1_value.append(float(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '지급배수_M01x':'지급배수_M15x'].values[0][i]))

'''담보 No.1 면책기간'''
cont_period_1 = ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '면책기간_C01x':'면책기간_C15x'].values[0]
cont_period_1_value = []
for i in range(len(cont_period_1)):
    if cont_period_1[i] == None:
        cont_period_1_value.append(0)
        if PDT_INFO.loc[PDT_INFO['갱신형여부']].values[0][0]==2 and POH_INFO['RSTY']==2:
            cont_period_1_value.append(0)
    else:
        cont_period_1_value.append(float(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '면책기간_C01x':'면책기간_C15x'].values[0][i]))

'''담보 No.1 감액기간'''
abate_period_1 = ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '감액기간_C01x':'감액기간_C15x'].values[0]
abate_period_1_value = []
for i in range(len(abate_period_1)):
    if abate_period_1[i] == None:
        abate_period_1_value.append(0)
        if PDT_INFO.loc[PDT_INFO['갱신형여부']].values[0][0]==2 and POH_INFO['RSTY']==2:
            abate_period_1_value.append(0)
    else:
        abate_period_1_value.append(float(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '감액기간_C01x':'감액기간_C15x'].values[0][i]))

'''담보 No.1 감액지급율'''
abate_rate_1 = ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '감액지급율_C01x':'감액지급율_C15x'].values[0]
abate_rate_1_value = []
for i in range(len(abate_rate_1)):
    if abate_rate_1[i] == None:
        abate_rate_1_value.append(1)
        if PDT_INFO.loc[PDT_INFO['갱신형여부']].values[0][0]==2 and POH_INFO['RSTY']==2:
            abate_rate_1_value.append(1)
    else:
        abate_rate_1_value.append(float(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '감액지급율_C01x':'감액지급율_C15x'].values[0][i]))

'''담보 별 위험률 No.(q01~15x)'''
# for i in range(ASSR_CNT):
#     assr_no = ETC_PDT_INFO['담보No.'].values[i]
#     print(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==assr_no, '위험률_No._q02x'].values[0])

'''담보 No.1의 경과나이 별 위험률 q01x~q15x'''
qNx_df = pd.DataFrame()
for l in range(1,16):
    qNx = []
    if ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '위험률_No._q'+str(l).zfill(2)+'x'].values[0] != None:
        for i in range(len(T_AGE_1)):
            if T_1.loc[i].values[0] <= cont_period_1_value[l-1]:
                qNx_value = max(int(T_1.loc[i].values[0])+1-cont_period_1_value[l-1],0)
            else:
                qNx_value = 1
            row_rorisk = (int(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '위험률_No._q'+str(l).zfill(2)+'x'].values[0])-1)*2+int(POH_INFO['GNDR'])+0
            col_rorisk = T_AGE_1.loc[i].values[0]+1
            rorisk_N = RoRISK_INFO.iloc[row_rorisk-1, col_rorisk+16]
            qNx.append(qNx_value*rorisk_N)
    else:
        for i in range(len(T_AGE_1)):
            qNx.append(0)
    qNx_df[f'q{l:02}x'] = qNx

'''담보 No.1의 경과나이 별 탈퇴율 lq1~5x'''
lqNx_df = pd.DataFrame()
for l in range(1,6):
    lqNx = []
    if ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '탈퇴율_No._lq'+str(l)+'x'].values[0] != None:
        for i in range(len(T_AGE_1)):
            lqNx.append(qNx_df['q'+str(l).zfill(2)+'x'].values[i]) 
    else:
        for i in range(len(T_AGE_1)):
            lqNx.append(0)
    lqNx_df[f'lq{l}x'] = lqNx


'''탈퇴율/위험률 데이터프레임 concat'''
lqx_qx_df = pd.concat([lqNx_df,qNx_df],axis=1)


'''qx 데이터프레임'''
qx_df = pd.DataFrame(columns=['qx'])
if ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '유지자_급부대상자_여부'].values[0] == '1':
    for i in range(len(T_AGE_1)):
        qx_df.loc[i] = 0
else:
    qx_df['qx'] = lqNx_df.sum(axis=1)

print(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==28, '유지자_급부대상자_여부'].values[0])
'''Lx 데이터프레임'''
Lx = [1] # Lx 초기값
for i in range(len(T_AGE_1)-1):
    Lx.append(Lx[-1]*(1-qx_df.loc[i].values[0]))
Lx_df = pd.DataFrame(Lx, columns=['Lx'])

'''Dx 데이터프레임'''
Dx = []
for i in range(len(T_AGE_1)):
    Dx.append(Lx_df.loc[i].values[0]*Vc.loc[0].values[0]**T_1.loc[i].values[0])
Dx_df = pd.DataFrame(Dx, columns=['Dx'])

'''Nx 데이터프레임'''
Nx = []
Nx.append(Dx_df.sum().values[0])
for i in range(len(T_AGE_1)-1):
    excluded_value = Dx_df.loc[i, 'Dx']
    partial_sum = Nx[i] - excluded_value
    Nx.append(partial_sum)
Nx_df = pd.DataFrame(Nx, columns=['Nx'])

'''LNx 데이터프레임'''
LNx_df = pd.DataFrame()
for i in range(1,6):
    LNx = [1]
    for l in range(len(T_AGE_1)-1):
        LNx.append(LNx[-1]*(1-lqNx_df['lq'+str(i)+'x'].values[l]))
    LNx_df[f'L{i}x'] = LNx


'''CNx 데이터프레임'''
CNx_df = pd.DataFrame()
for i in range(1,16):
    CNx = []
    for l in range(len(T_AGE_1)):
        if ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, '유지자_급부대상자_여부'].values[0] == 0:
            CNx.append(Lx_df.loc[l].values[0] * qNx_df['q'+str(i).zfill(2)+'x'][l] * Vc.loc[0].values[0]**(T_1.loc[l].values[0] + 1/2))
        else:
            if i < 5:
                CNx.append(LNx_df['L'+str(i)+'x'][l] * qNx_df['q'+str(i).zfill(2)+'x'][l] * Vc.loc[0].values[0]**(T_1.loc[l].values[0] + 1/2))
            else:
                CNx.append(LNx_df['L5x'][l] * qNx_df['q'+str(i).zfill(2)+'x'][l] * Vc.loc[0].values[0]**(T_1.loc[l].values[0] + 1/2))
    CNx_df[f'C{i:02}x'] = CNx

'''MNx 데이터프레임'''
MNx_df = pd.DataFrame()
for i in range(1,16):
    MNx = []
    MNx.append(CNx_df['C'+str(i).zfill(2)+'x'].sum())
    for l in range(len(T_AGE_1)-1):
        excluded_value2 = CNx_df.loc[l, 'C'+str(i).zfill(2)+'x']
        partial_sum2 = MNx[l] - excluded_value2
        MNx.append(partial_sum2)
    MNx_df[f'M{i:02}x'] = MNx

'''SUMNt 데이터프레임'''
SUMNt_df = pd.DataFrame()
for i in range(1,16):
    SUMNt = []
    for l in range(len(T_AGE_1)):
        if T_1.loc[l].values[0] < abate_period_1_value[i-1]:
            SUMNt.append(pay_rate_1_value[i-1] * ((MNx_df.loc[l, 'M'+str(i).zfill(2)+'x']-MNx_df.loc[NN.loc[0].values[0], 'M'+str(i).zfill(2)+'x'])-(1-abate_rate_1_value[i-1])*CNx_df.loc[l, 'C'+str(i).zfill(2)+'x']))
        else:
            SUMNt.append(pay_rate_1_value[i-1] * ((MNx_df.loc[l, 'M'+str(i).zfill(2)+'x']-MNx_df.loc[NN.loc[0].values[0], 'M'+str(i).zfill(2)+'x'])-(1-abate_rate_1_value[i-1])*0))
    SUMNt_df[f'SUM{i:02}t'] = SUMNt

'''SUMt (SUMNt 데이터프레임 합) 데이터프레임'''
SUMt_df = pd.DataFrame(columns=['SUMt'])
SUMt_df['SUMt'] = SUMNt_df.sum(axis=1)

'''기수표에 계산기수 데이터프레임 추가'''
PV_calc_table = pd.concat([PV_calc_table, Lx_df, qx_df, Dx_df, Nx_df, LNx_df, lqx_qx_df, CNx_df, MNx_df, SUMt_df, SUMNt_df], axis=1)

'''기수표 csv 저장'''
PV_calc_table.to_csv(f"./기수표.csv", index=False, encoding='utf-8-sig')

'''계산기수 활용 보험료 산출'''
NNs = 12 / int(POH_INFO['CYTY']) * (Nx_df.loc[0].values[0] - Nx_df.loc[0+MM.loc[0].values[0]].values[0]-(12 / int(POH_INFO['CYTY'])-1) / (2 * 12/int(POH_INFO['CYTY'])) * (Dx_df.loc[0].values[0] - Dx_df.loc[0+MM.loc[0].values[0]].values[0]))

SUM0_list = []
for i in range(1,16):
    if abate_period_1_value[i-1] <= 1:
        SUM0_list.append(pay_rate_1_value[i-1] * ((MNx_df.loc[0, 'M'+str(i).zfill(2)+'x']-MNx_df.loc[NN.loc[0].values[0], 'M'+str(i).zfill(2)+'x'])-(1-abate_rate_1_value[i-1])*CNx_df.loc[0, 'C'+str(i).zfill(2)+'x']))
    elif abate_period_1_value[i-1] > 1 and abate_period_1_value[i-1] <= 2:
        SUM0_list.append(pay_rate_1_value[i-1] * ((MNx_df.loc[0, 'M'+str(i).zfill(2)+'x']-MNx_df.loc[NN.loc[0].values[0], 'M'+str(i).zfill(2)+'x'])-(1-abate_rate_1_value[i-1])*(CNx_df.loc[0, 'C'+str(i).zfill(2)+'x'] + CNx_df.loc[1, 'C'+str(i).zfill(2)+'x'])))
    elif abate_period_1_value[i-1] > 2:
        SUM0_list.append(pay_rate_1_value[i-1] * ((MNx_df.loc[0, 'M'+str(i).zfill(2)+'x']-MNx_df.loc[NN.loc[0].values[0], 'M'+str(i).zfill(2)+'x'])-(1-abate_rate_1_value[i-1])*(CNx_df.loc[0, 'C'+str(i).zfill(2)+'x'] + CNx_df.loc[1, 'C'+str(i).zfill(2)+'x'] + CNx_df.loc[2, 'C'+str(i).zfill(2)+'x'])))

SUM0 = sum(SUM0_list)

'''순보험료 N_Pm'''
N_Pm = SUM0 / NNs


'''영업보험료 G_Pm'''
alpha_2 = []
if PDT_INFO.loc[PDT_INFO['담보No.']==1, '갱신형여부'].values[0]==1:
    alpha_2.append(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'alpha_최초계약_'+str(MM.loc[0].values[0])+'년'].values[0])
elif PDT_INFO.loc[PDT_INFO['담보No.']==1, '갱신형여부'].values[0] != 1 and POH_INFO['RSTY'] == 1:
    alpha_2.append(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'alpha_최초계약_'+str(MM.loc[0].values[0])+'년'].values[0])
elif PDT_INFO.loc[PDT_INFO['담보No.']==1, '갱신형여부'].values[0] != 1 and POH_INFO['RSTY'] != 1:
    alpha_2.append(ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'alpha_갱신계약_'+str(MM.loc[0].values[0])+'년'].values[0])

G_Pm = N_Pm / (1 - 12/int(POH_INFO['CYTY']) * alpha_2[0] / NNs - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'beta'].values[0] - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'beta_5'].values[0] - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'ce'].values[0])



### 15. 기준 보험가입금액 순보험료 / 영업보험료 산출 (PDT_S_N_Pm / PDT_S_G_Pm) ###
PDT_S_N_Pm = round(N_Pm, 20) * FACE_AMT.loc[0].values[0]
PDT_S_G_Pm = round(G_Pm, 20) * FACE_AMT.loc[0].values[0]


### 16. 기준 보험가입금액 라운드처리 순보험료 / 영업보험료 산출 (PDT_S_N_Pm_Rnd / PDT_S_G_Pm_Rnd) ###
PDT_S_N_Pm_Rnd = round(PDT_S_N_Pm, 0)
PDT_S_G_Pm_Rnd = round(PDT_S_G_Pm, 0)


### 17. 설계보험가입금액 순보험료 / 영업보험료 산출 (FACE_AMT_N_Pm / FACE_AMT_G_Pm) ###
FACE_AMT_N_Pm = round(PDT_S_N_Pm_Rnd, 0) * PDT_INFO.loc[PDT_INFO['담보No.']==1, '설계보험가입금액'].values[0] / PDT_INFO.loc[PDT_INFO['담보No.']==1, '기준보험가입금액'].values[0]
FACE_AMT_G_Pm = round(PDT_S_G_Pm_Rnd, 0) * PDT_INFO.loc[PDT_INFO['담보No.']==1, '설계보험가입금액'].values[0] / PDT_INFO.loc[PDT_INFO['담보No.']==1, '기준보험가입금액'].values[0]


### 18. 연납 기준 순보험료 / 영업보험료 산출 (N_Pm_Y / G_Pm_Y) ###
N_Pm_Y = SUM0 / (Nx_df.loc[0].values[0] - Nx_df.loc[0+MM.loc[0].values[0]].values[0])
G_Pm_Y = N_Pm_Y / (1-alpha_2[0] / (Nx_df.loc[0].values[0] - Nx_df.loc[0+MM.loc[0].values[0]].values[0]) - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'beta'].values[0] - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'beta_5'].values[0] - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'ce'].values[0])


### 19. 보험가입금액 1원 기준 연납 순보험료 산출 (k_N_Pm_Y) ###
k = min(NN.loc[0].values[0], 20)
k_N_Pm_Y = SUM0 / (Nx_df.loc[0].values[0] - Nx_df.loc[k].values[0])


### 20. 보험가입금액 1원 표준신계약비 / 적용신계약비 산출 (S_ALPH / A_ALPH) ###
''' 표준신계약비 산출용 산출 보험가입금액 '''
TMP_Calc_S = []
if PDT_INFO.loc[PDT_INFO['담보No.']==1, '갱신형여부'].values[0] == 1:
    if int(POH_INFO['ENT_AGE']) + NN.loc[0].values[0] == 80:
        TMP_Calc_S.append(Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, '산출보험가입금액_80세만기'].values[0])
    elif int(POH_INFO['ENT_AGE']) + NN.loc[0].values[0] == 90:
        TMP_Calc_S.append(Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, '산출보험가입금액_90세만기'].values[0])
    elif int(POH_INFO['ENT_AGE']) + NN.loc[0].values[0] == 100:
        TMP_Calc_S.append(Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, '산출보험가입금액_100세만기'].values[0])
else:
    if NN.loc[0].values[0] > 15:
        TMP_Calc_S.append(Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, '산출보험가입금액_20년'].values[0])
    elif NN.loc[0].values[0] > 10 and NN.loc[0].values[0] <= 15:
        TMP_Calc_S.append(Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, '산출보험가입금액_15년'].values[0])
    else:
        TMP_Calc_S.append(Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, '산출보험가입금액_10년'].values[0])

S_ALPH = []
if Calc_S_INFO.loc[Calc_S_INFO['담보No.']==1, 'S_산출여부'].values[0] == 1:
    S_ALPH.append(round(k_N_Pm_Y,20) * k * 0.05 + 10/1000 * TMP_Calc_S[0])
else:
    S_ALPH.append(round(k_N_Pm_Y,20) * k * 0.05 + round(k_N_Pm_Y,20) * 0.45)

A_ALPH = round(round(N_Pm,10)/(1 - 12/int(POH_INFO['CYTY']) * alpha_2[0] / NNs - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'beta'].values[0] - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'beta_5'].values[0] - ETC_PDT_INFO.loc[ETC_PDT_INFO['담보No.']==1, 'ce'].values[0]), 10) * 12/int(POH_INFO['CYTY']) * alpha_2[0]


### 21. 기준 보험가입금액 표준신계약비 / 적용신계약비 산출 (PDT_S_S_ALPH / PDT_S_A_ALPH) ###
PDT_S_S_ALPH = round(S_ALPH[0] * FACE_AMT.loc[0].values[0], 0)
PDT_S_A_ALPH = round(A_ALPH * FACE_AMT.loc[0].values[0], 0)


### 22. 기준 보험가입금액 라운드처리 표준신계약비 / 적용신계약비 산출 (PDT_S_S_ALPH_Rnd / PDT_S_A_ALPH_Rnd) ###
PDT_S_S_ALPH_Rnd = int(PDT_S_S_ALPH)
PDT_S_A_ALPH_Rnd = round(PDT_S_A_ALPH, 0)


### 23. 보험가입금액 1원 해약공제액 (ALPH) ###
ALPH = min(S_ALPH[0], A_ALPH)


### 24. 기준 보험가입금액 해약공제액 (PDT_S_ALPH) ###
PDT_S_ALPH = round(ALPH,10) * FACE_AMT.loc[0].values[0]


### 25. 기준 보험가입금액 라운드처리 해약공제액 (PDT_S_ALPH_Rnd) ###
PDT_S_ALPH_Rnd = round(PDT_S_ALPH, 0)


### 26. 설계보험가입금액 해약공제액 (FACE_AMT_ALPH) ###
FACE_AMT_ALPH = PDT_S_ALPH_Rnd * PDT_INFO.loc[PDT_INFO['담보No.']==1, '설계보험가입금액'].values[0] / PDT_INFO.loc[PDT_INFO['담보No.']==1, '기준보험가입금액'].values[0]


### 0. 산출 값 데이터프레임(시리즈)화 ###
N_Pm = pd.Series([N_Pm], name = 'N_Pm')
G_Pm = pd.Series([G_Pm], name = 'G_Pm')
PDT_S_N_Pm = pd.Series([PDT_S_N_Pm], name = 'PDT_S_N_Pm')
PDT_S_G_Pm = pd.Series([PDT_S_G_Pm], name = 'PDT_S_G_Pm')
PDT_S_N_Pm_Rnd = pd.Series([PDT_S_N_Pm_Rnd], name = 'PDT_S_N_Pm_Rnd')
PDT_S_G_Pm_Rnd = pd.Series([PDT_S_G_Pm_Rnd], name = 'PDT_S_G_Pm_Rnd')
FACE_AMT_N_Pm = pd.Series([FACE_AMT_N_Pm], name = 'FACE_AMT_N_Pm')
FACE_AMT_G_Pm = pd.Series([FACE_AMT_G_Pm], name = 'FACE_AMT_G_Pm')
N_Pm_Y = pd.Series([N_Pm_Y], name = 'N_Pm_Y')
G_Pm_Y = pd.Series([G_Pm_Y], name = 'G_Pm_Y')
k_N_Pm_Y = pd.Series([k_N_Pm_Y], name = 'k_N_Pm_Y')
S_ALPH = pd.DataFrame(S_ALPH)
A_ALPH = pd.Series([A_ALPH], name = 'A_ALPH')
PDT_S_S_ALPH = pd.Series([PDT_S_S_ALPH], name = 'PDT_S_S_ALPH')
PDT_S_A_ALPH = pd.Series([PDT_S_A_ALPH], name = 'PDT_S_A_ALPH')
PDT_S_S_ALPH_Rnd = pd.Series([PDT_S_S_ALPH_Rnd], name = 'PDT_S_S_ALPH_Rnd')
PDT_S_A_ALPH_Rnd = pd.Series([PDT_S_A_ALPH_Rnd], name = 'PDT_S_A_ALPH_Rnd')
ALPH = pd.Series([ALPH], name = 'ALPH')
PDT_S_ALPH = pd.Series([PDT_S_ALPH], name = 'PDT_S_ALPH')
PDT_S_ALPH_Rnd = pd.Series([PDT_S_ALPH_Rnd], name = 'PDT_S_ALPH_Rnd')
FACE_AMT_ALPH = pd.Series([FACE_AMT_ALPH], name = 'FACE_AMT_ALPH')

### Final. P_cal 데이터프레임 구성 ###
'''담보코드'''
P_Cal['ASSR_CD'] = ASSR_CD

'''종형구분 코드'''
P_Cal['INTY'] = '00'

'''보험기간'''
P_Cal['INSPD'] = NN

'''납입기간'''
P_Cal['PMTPD'] = MM

'''납입주기'''
P_Cal['CYTY'] = CYTY

'''피보험자관계구분 코드'''
P_Cal['POH_TYCD'] = '00'

'''성별구분'''
P_Cal['GNDR'] = GNDR

'''가입나이'''
P_Cal['ENT_AGE'] = ENT_AGE

'''상해급수'''
P_Cal['LEV'] = LEV

'''운전형태'''
P_Cal['DRTY'] = DRTY

'''보험가입금액'''
P_Cal['FACE_AMT'] = FACE_AMT

'''요율구분(신규/갱신구분)'''
P_Cal['RSTY'] = RSTY

'''최초가입나이'''
P_Cal['INIT_ENT_AGE'] = INIT_ENT_AGE

'''보험가입금액 1원 영업보험료'''
P_Cal['G_Pm'] = G_Pm

'''보험가입금액 1원 순보험료'''
P_Cal['N_Pm'] = N_Pm

'''기준 보험가입금액 순보험료'''
P_Cal['PDT_S_N_Pm'] = PDT_S_N_Pm

'''기준 보험가입금액 영업보험료'''
P_Cal['PDT_S_G_Pm'] = PDT_S_G_Pm

'''기준 보험가입금액_라운드처리순보험료'''
P_Cal['PDT_S_N_Pm_Rnd'] = PDT_S_N_Pm_Rnd

'''기준 보험가입금액_라운드처리영업보험료'''
P_Cal['PDT_S_G_Pm_Rnd'] = PDT_S_G_Pm_Rnd

'''설계보험가입금액 순보험료'''
P_Cal['FACE_AMT_N_Pm'] = FACE_AMT_N_Pm

'''설계보험가입금액 영업보험료'''
P_Cal['FACE_AMT_G_Pm'] = FACE_AMT_G_Pm

'''연납 기준 순보험료'''
P_Cal['N_Pm_Y'] = N_Pm_Y

'''연납 기준 영업보험료'''
P_Cal['G_Pm_Y'] = G_Pm_Y

'''보험가입금액 1원 기준연납 순보험료'''
P_Cal['k_N_Pm_Y'] = k_N_Pm_Y

'''보험가입금액 1원 표준신계약비'''
P_Cal['S_ALPH'] = S_ALPH

'''보험가입금액 1원 적용신계약비'''
P_Cal['A_ALPH'] = A_ALPH

'''기준 보험가입금액 표준신계약비'''
P_Cal['PDT_S_S_ALPH'] = PDT_S_S_ALPH

'''기준 보험가입금액 적용신계약비'''
P_Cal['PDT_S_A_ALPH'] = PDT_S_A_ALPH

'''기준 보험가입금액_라운드처리 표준신계약비'''
P_Cal['PDT_S_S_ALPH_Rnd'] = PDT_S_S_ALPH_Rnd

'''기준 보험가입금액_라운드처리 적용신계약비'''
P_Cal['PDT_S_A_ALPH_Rnd'] = PDT_S_A_ALPH_Rnd

'''보험가입금액1원 해약공제액'''
P_Cal['ALPH'] = ALPH

'''기준 보험가입금액 해약공제액'''
P_Cal['PDT_S_ALPH'] = PDT_S_ALPH

'''기준 보험가입금액_라운드처리 해약공제액'''
P_Cal['PDT_S_ALPH_Rnd'] = PDT_S_ALPH_Rnd

'''설계보험가입금액 해약공제액'''
P_Cal['FACE_AMT_ALPH'] = FACE_AMT_ALPH


### P Calculation result table to csv ###
P_Cal.to_csv(f"./P_Data_table.csv", index=False, encoding='utf-8-sig')





### P_Data 저장 ###
# result_P_DATA = []
# def P_DATA_make(ASSR_NO):
    # P_DATA = []
    # P_DATA.append(NN_calc(ASSR_NO))
    # P_DATA.append(MM_calc(ASSR_NO))
    # P_DATA.append(CYTY_calc()) # 가입설계조건 POH_INFO 설정에 따라 값이 변환
    # result_P_DATA.append(P_DATA)

# 담보 별 보험기간, 납입기간, 납입주기 데이터프레임화 > P_DATA 데이터프레임 저장 #
# for ASSR_NO in range(1,ASSR_CNT+1):
    # P_DATA_make(ASSR_NO)
# 
# P_DATA_df = pd.DataFrame(result_P_DATA)

### P_Data ; .csv 파일로 저장 ###
# create_date = datetime.datetime.now().strftime("%Y-%m-%d")
# P_DATA_df.to_csv(f"./P_table_{create_date}.csv", header=['NN(보험기간)','MM(납입기간)','CYTY(납입주기)'], index=False, encoding='utf-8-sig')