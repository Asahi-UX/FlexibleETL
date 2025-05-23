import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 1. Подготовка дат
now = pd.Timestamp.now().normalize()
first_day_this_month = now.replace(day=1)
six_months_ago = first_day_this_month - pd.DateOffset(months=6)

# 2. Приведение дат
b['md_ins_date_dt'] = pd.to_datetime(b['md_ins_date'], unit='s')
c['dt_credit_dt'] = pd.to_datetime(c['dt_credit'], unit='s')

# 3. Фильтрация по условиям WHERE
company_ids = [25593, 26453, 27336, 27338]
mask = (
    (ci['insur_comp_id'] == 27) &
    (c['is_delete'] == 0) &
    (c['is_test'] == 0) &
    (b['md_ins_date_dt'] >= six_months_ago) &
    (com['idcompany'].isin(company_ids))
)

# 4. JOIN-ы (MERGE)
df = ci.merge(c, on='idcredit') \
       .merge(b, on='idblank') \
       .merge(cl, left_on='idclient', right_on='idclient') \
       .merge(ban, left_on='idbank', right_on='idbank') \
       .merge(com, on='idcompany') \
       .merge(t, on='idtrade') \
       .merge(tc, on='idtrade') \
       .merge(tar, on='idtrade') \
       .merge(a1, left_on='idaccount', right_on='idaccount', how='left', suffixes=('', '_a1')) \
       .merge(a2, left_on='idaccount_second', right_on='idaccount', how='left', suffixes=('', '_a2')) \
       .merge(a3, left_on='idaccount_third', right_on='idaccount', how='left', suffixes=('', '_a3')) \
       .merge(a4, left_on='idaccount_fourth', right_on='idaccount', how='left', suffixes=('', '_a4')) \
       .merge(ac1, left_on='idaccount', right_on='idaccount', how='left', suffixes=('', '_ac1')) \
       .merge(ac2, left_on='idaccount_second', right_on='idaccount', how='left', suffixes=('', '_ac2')) \
       .merge(ac3, left_on='idaccount_third', right_on='idaccount', how='left', suffixes=('', '_ac3')) \
       .merge(ac4, left_on='idaccount_fourth', right_on='idaccount', how='left', suffixes=('', '_ac4')) \
       .merge(ac_bl, left_on='md_ins_user', right_on='idaccount', how='left', suffixes=('', '_ac_bl')) \
       .merge(rl[rl['md_is_activ'] == 1], left_on='idcredit_insurance', right_on='idcredit_insurance', how='left') \
       .merge(bi, left_on='idbl_insur', right_on='idblank_insurance') \
       .merge(a, left_on='idaccount', right_on='idaccount') \
       .merge(rar, left_on='idaccount', right_on='idaccount') \
       .merge(ar, left_on='idacc_role', right_on='idacc_role') \
       .merge(tp, left_on='type_platform', right_on='idtype_platform', how='left')

# 5. Фильтрация по маске
df = df[mask]

# 6. Вспомогательные функции
def format_fio(row):
    return f"{row['last_name']} {row['first_name'][0]}. {row['second_name'][0]}.".replace(' .', '')

months_ru = [
    'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
]

def sum_credit_type(val):
    if val < 15000: return '0 - 14 999'
    elif val < 30000: return '15 000 - 29 999'
    elif val < 45000: return '30 000 - 44 999'
    elif val < 60000: return '45 000 - 59 999'
    elif val < 75000: return '60 000 - 74 999'
    elif val < 90000: return '75 000 - 89 999'
    elif val < 105000: return '90 000 - 104 999'
    elif val < 120000: return '105 000 - 119 999'
    elif val < 135000: return '120 000 - 134 999'
    elif val < 150000: return '135 000 - 149 999'
    else: return '150 000+'

def week_period(dt):
    if pd.isnull(dt): return ''
    monday = dt - pd.Timedelta(days=dt.weekday())
    sunday = monday + pd.Timedelta(days=6)
    return f"{monday.day} {months_ru[monday.month-1]} - {sunday.day} {months_ru[sunday.month-1]}"

# 7. Формирование итогового DataFrame
result = pd.DataFrame({
    'idblank': df['idblank'],
    'credit_number': df['credit_number'],
    'acc_blank': df.apply(format_fio, axis=1),
    'type_platform': df['name'],
    'name_role': df['name_role'],
    'dt_credit': df['dt_credit_dt'],
    'number_week': df['dt_credit_dt'].dt.isocalendar().week,
    'days_of_week': df['dt_credit_dt'].apply(week_period),
    'dt_month_year_credit': df['dt_credit_dt'].dt.strftime('%Y %m. ') + df['dt_credit_dt'].dt.month.apply(lambda m: months_ru[m-1]),
    'sum_credit': df['sum_credit'],
    'sum_credit_type': df['sum_credit'].apply(sum_credit_type),
    'terms': df['terms'],
    'rate': df['rate'],
    'bank_name': df['bank_name'],
    'type_insur_id': df['type_insur_id'],
    'type_insur_name': df['type_insur_name'],
    'insur_comp_name': df['insur_comp_name'],
    'prod_name': df['prod_name'],
    'premium': df['premium'],
    'idcompany': df['idcompany'],
    'full_name': df['full_name'],
    'is_lite_credit': df['is_lite_credit'],
    'idtrade': df['idtrade'],
    'brand': df['brand'],
    'federal_district': df['federal_district'],
    'tt_name': df['full_name_tc'],
    'region': df['region'],
    'city': df['city'],
    'full_string_address': df['full_string_address'],
    'head_1': df.apply(lambda row: f"{row['last_name_a1']} {row['first_name_a1'][0]}. {row['second_name_a1'][0]}.", axis=1),
    'head_2': df.apply(lambda row: f"{row['last_name_a2']} {row['first_name_a2'][0]}. {row['second_name_a2'][0]}.", axis=1),
    'head_3': df.apply(lambda row: f"{row['last_name_a3']} {row['first_name_a3'][0]}. {row['second_name_a3'][0]}.", axis=1),
    'head_4': df.apply(lambda row: f"{row['last_name_a4']} {row['first_name_a4'][0]}. {row['second_name_a4'][0]}.", axis=1),
    'proc_type': df['idproc_type'].map({0: 'inside', 1: 'outside'}),
    'dt_return': pd.to_datetime(df['md_ins_date'], unit='s').dt.date,
    'return_origin': df['return_origin'].map({1: 'BANK.bdsm', 2: 'IN.bdsm', 3: 'API'}),
    'return_amount_ps': df['return_amount_ps'],
    'is_return': (df['return_amount_ps'] > 0).astype(int),
    'is_forced': df['is_forced'],
    'is_forsed_txt': df['is_forced'].map({1: 'принудительная страховка', 0: 'продажа страховки'}),
    'return_comment': df['return_comment'],
    'return_fin_type': df['return_type_financing'].map({
        1: 'OUTSIDE (реквизиты клиента)',
        2: 'INSIDE (без наших услуг, реквизиты клиента)',
        3: 'INSIDE прямой возврат (с нашими услугами, реквизиты p-s)'
    }).fillna(''),
    'is_1': 1
})

# Готово! DataFrame result содержит все нужные поля.
