### 보험상품 정보 헤더
PDT_INFO_HEADER = ['담보No.', '담보명', '담보코드', '특약코드', '대분류', '중분류', '소분류', '갱신형여부', '약관No._Lv1', '약관No._Lv2', '가입나이_최저', '가입나이_최대', '기준보험가입금액', '설계보험가입금액']


### 적용위험률 정보 헤더
RoRISK_INFO_HEADER = ['담보No.','담보명','특약코드','종별구분','상해급수','보상한도','운전여부','운전형태','갱신구분','요율구분','위험률No.','적용위험률코드','RA구분코드','단일율여부','단일율판단','성별','경과기간']
"""연령범위"""
AGE_RANGE = list(range(121))
for a in AGE_RANGE:
    RoRISK_INFO_HEADER.append('연령별_'+str(a))


### 기타 보험상품 정보 헤더
ETC_PDT_INFO_HEADER = ['담보No.','담보명','특약코드','적용이율_15년이하','적용이율_15년초과']
"""alpha_최초계약"""
ETC_ALPHA_1 = ['10년','15년','20년','30년']
for b in ETC_ALPHA_1:
    ETC_PDT_INFO_HEADER.append('alpha_최초계약_'+str(b))
"""alpha_갱신계약"""
ETC_ALPHA_2 = []
for c in range(20,0,-1):
    ETC_ALPHA_2.append('alpha_갱신계약_'+str(c)+'년')
for d in ETC_ALPHA_2:
    ETC_PDT_INFO_HEADER.append(str(d))
"""sub1"""
sub1_ETC_PDT_INFO_HEADER = ['beta','beta_2','beta_a','beta_5','ce','상해급수','운전형태']
for e in sub1_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(e))
"""탈퇴율"""
sub2_ETC_PDT_INFO_HEADER = []
for f in range(1,6):
    sub2_ETC_PDT_INFO_HEADER.append('탈퇴율_No._lq'+str(f)+'x')
for g in sub2_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(g))
"""위험률"""
sub3_ETC_PDT_INFO_HEADER = []
for h in range(1,16):
    sub3_ETC_PDT_INFO_HEADER.append('위험률_No._q'+str(h).zfill(2)+'x')
for i in sub3_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(i))
"""지급배수"""
sub4_ETC_PDT_INFO_HEADER = []
for j in range(1,16):
    sub4_ETC_PDT_INFO_HEADER.append('지급배수_M'+str(j).zfill(2)+'x')
for k in sub4_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(k))
"""면책기간"""
sub5_ETC_PDT_INFO_HEADER = []
for l in range(1,16):
    sub5_ETC_PDT_INFO_HEADER.append('면책기간_C'+str(l).zfill(2)+'x')
for m in sub5_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(m))
"""감액기간"""
sub6_ETC_PDT_INFO_HEADER = []
for n in range(1,16):
    sub6_ETC_PDT_INFO_HEADER.append('감액기간_C'+str(n).zfill(2)+'x')
for o in sub6_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(o))
"""감액지급율"""
sub7_ETC_PDT_INFO_HEADER = []
for p in range(1,16):
    sub7_ETC_PDT_INFO_HEADER.append('감액지급율_C'+str(p).zfill(2)+'x')
for q in sub7_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(q))
"""sub8"""
sub8_ETC_PDT_INFO_HEADER = ['사망보험금_유무','유지자_급부대상자_여부','사망외약관상소멸사유']
for r in sub8_ETC_PDT_INFO_HEADER:
    ETC_PDT_INFO_HEADER.append(str(r))


### 산출 보험가입금액 정보 헤더
Calc_S_INFO_HEADER = ['담보No.','담보명','특약코드','S_산출여부','보험기간','납입기간','납입주기','성별','가입나이','상해급수','운전여부','운전급수_형태','신규갱신구분','최초가입나이','기준연령요건_위험보험료_특약별_80세만기','기준연령요건_위험보험료_특약별_90세만기','기준연령요건_위험보험료_특약별_100세만기','특약별_10년','특약별_15년','특약별_20년','정기보험_80세만기','정기보험_90세만기','정기보험_100세만기','정기보험_10년','정기보험_15년','정기보험_20년','산출보험가입금액_80세만기','산출보험가입금액_90세만기','산출보험가입금액_100세만기','산출보험가입금액_10년','산출보험가입금액_15년','산출보험가입금액_20년']


### PV Files 생성 정보 헤더
PV_Files_INFO_HEADER = ['생성No.','담보No.','담보명','특약코드','산출Type','보험기간_begin','보험기간_end','보험기간_step','납입기간_begin','납입기간_end','납입기간_step','가입나이_begin','가입나이_end','가입나이_step','납입주기_begin','납입주기_end','납입주기_step','성별구분_begin','성별구분_end','성별구분_step','상해급수_begin','상해급수_end','상해급수_step','운전형태_begin','운전형태_end','운전형태_step','요율구분_begin','요율구분_end','요율구분_step','최초가입나이_begin','최초가입나이_end','최초가입나이_step']