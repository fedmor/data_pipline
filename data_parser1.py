#!/usr/bin/python
# -*- coding: UTF-8 -*-
import argparse
import sys
import pandas as pd
from tqdm import tqdm

"""
字段说明
1.file_path 文件路径
2.deposit 入账金额
3.withdraw 出账金额
3.cst_id 客户ID
4.acg_dt 字段更新时间
5.file_type 文件类型
"""


def his_paser(file_path, deposit, withdraw, cst_id, acg_dt, file_type):
    file = pd.read_csv(file_path)

    df_list = []

    file_sum_deposit = file.groupby(cst_id)[deposit].agg('sum')  # 总和 deposit
    file_sum_deposit_df = pd.DataFrame({'cst_id': file_sum_deposit.index, 'sum_deposit': file_sum_deposit.values})
    df_list.append(file_sum_deposit_df)
    file_sum_withdraw = file.groupby(cst_id)[withdraw].agg('sum')  # 总和 deposit
    file_sum_withdraw_df = pd.DataFrame({'cst_id': file_sum_withdraw.index, 'sum_withdraw': file_sum_withdraw.values})
    df_list.append(file_sum_withdraw_df)
    print("get sum_*")

    file_mean = file_sum_deposit / float(len(file[acg_dt].drop_duplicates().tolist()))  # 均值
    file_mean_deposit_df = pd.DataFrame({'cst_id': file_mean.index, 'mean_deposit': file_mean.values})
    df_list.append(file_mean_deposit_df)
    file_mean = file_sum_withdraw / float(len(file[acg_dt].drop_duplicates().tolist()))  # 均值
    file_mean_withdraw_df = pd.DataFrame({'cst_id': file_mean.index, 'mean_withdraw': file_mean.values})
    df_list.append(file_mean_withdraw_df)
    print("get mean_*")

    file_max_deposit = file.groupby(cst_id)[deposit].agg('max')  # 最大值
    file_max_deposit_df = pd.DataFrame({'cst_id': file_max_deposit.index, 'max_deposit': file_max_deposit.values})
    df_list.append(file_max_deposit_df)
    file_max_withdraw = file.groupby(cst_id)[withdraw].agg('max')  # 最大值
    file_max_withdraw_df = pd.DataFrame({'cst_id': file_max_withdraw.index, 'max_withdraw': file_max_withdraw.values})
    df_list.append(file_max_withdraw_df)
    print("get max_*")

    file_min_deposit = file.groupby(cst_id)[deposit].agg('min')  # 最小值
    file_min_deposit_df = pd.DataFrame({'cst_id': file_min_deposit.index, 'min_deposit': file_min_deposit.values})
    df_list.append(file_min_deposit_df)
    file_min_withdraw = file.groupby(cst_id)[withdraw].agg('min')  # 最小值
    file_min_withdraw_df = pd.DataFrame({'cst_id': file_min_withdraw.index, 'min_withdraw': file_min_withdraw.values})
    df_list.append(file_min_withdraw_df)
    print("get min_*")

    file_std_deposit = file.groupby(cst_id)[deposit].agg('std')  # 标准差
    file_std_deposit_df = pd.DataFrame({'cst_id': file_std_deposit.index, 'std_deposit': file_std_deposit.values})
    df_list.append(file_std_deposit_df)
    file_std_withdraw = file.groupby(cst_id)[withdraw].agg('std')  # 标准差
    file_std_withdraw_df = pd.DataFrame({'cst_id': file_std_withdraw.index, 'std_withdraw': file_std_withdraw.values})
    df_list.append(file_std_withdraw_df)
    print("get std_*")

    file_count_deposit = file.groupby(cst_id)[deposit].count()  # 记录数
    file_count_deposit_df = pd.DataFrame({'cst_id': file_count_deposit.index, 'deposit_num': file_count_deposit.values})
    df_list.append(file_count_deposit_df)
    file_count_withdraw = file.groupby(cst_id)[withdraw].count()  # 记录数
    file_count_withdraw_df = pd.DataFrame({'cst_id': file_count_withdraw.index, 'withdraw_num': file_count_withdraw.values})
    df_list.append(file_count_withdraw_df)
    print("get *_num")

    file_ac_bal_by_mounth_deposit = pd.pivot_table(file, values=deposit, index=cst_id, columns=acg_dt, aggfunc="sum")  # 分月余额
    rename_columns = {column: column + 'deposit' for column in file_ac_bal_by_mounth_deposit.columns}
    file_ac_bal_by_mounth_deposit.rename(columns=rename_columns, inplace=True)
    file_ac_bal_by_mounth_deposit[cst_id] = file_ac_bal_by_mounth_deposit.index
    df_list.append(file_ac_bal_by_mounth_deposit)
    file_ac_bal_by_mounth_withdraw = pd.pivot_table(file, values=withdraw, index=cst_id, columns=acg_dt, aggfunc="sum")  # 分月余额
    rename_columns = {column: column + 'withdraw' for column in file_ac_bal_by_mounth_withdraw.columns}
    file_ac_bal_by_mounth_withdraw.rename(columns=rename_columns, inplace=True)
    file_ac_bal_by_mounth_withdraw[cst_id] = file_ac_bal_by_mounth_withdraw.index
    df_list.append(file_ac_bal_by_mounth_withdraw)
    print("get *01,*02,… ")

    # 最大余额月份
    user_max_mounth_deposit = {cst_id: [], 'man_deposit': []}
    for name, group in tqdm(file[[cst_id, acg_dt, deposit]].groupby([cst_id])):
        user_max_mounth_deposit[cst_id].append(name)
        user_max_mounth_deposit['man_deposit'].append(group.xs(group[deposit].argmax())[acg_dt])
    user_max_mounth_df_deposit = pd.DataFrame(user_max_mounth_deposit)
    df_list.append(user_max_mounth_df_deposit)

    user_max_mounth_withdraw = {cst_id: [], 'man_withdraw': []}
    for name, group in tqdm(file[[cst_id, acg_dt, deposit]].groupby([cst_id])):
        user_max_mounth_withdraw[cst_id].append(name)
        user_max_mounth_withdraw['man_withdraw'].append(group.xs(group[deposit].argmax())[acg_dt])
    user_max_mounth_df_withdraw = pd.DataFrame(user_max_mounth_withdraw)
    df_list.append(user_max_mounth_df_withdraw)
    print("get man_*")

    # 前三个月总和
    # 前六个月总和
    # 前九个月总和
    # 十二个月总和
    tmp_deposit = pd.pivot_table(file, values=deposit, index=cst_id, columns=acg_dt, aggfunc="sum")
    columns_list = list(tmp_deposit.columns.sort_values())
    M3_deposit = tmp_deposit[columns_list[:3]].apply(lambda x: x.sum(), axis=1)
    M6_deposit = tmp_deposit[columns_list[:6]].apply(lambda x: x.sum(), axis=1)
    M9_deposit = tmp_deposit[columns_list[:9]].apply(lambda x: x.sum(), axis=1)
    M12_deposit = tmp_deposit[columns_list[:12]].apply(lambda x: x.sum(), axis=1)
    M3_df_deposit = pd.DataFrame({cst_id: M3_deposit.index, "M3_deposit": M3_deposit.values})
    M6_df_deposit = pd.DataFrame({cst_id: M6_deposit.index, "M6_deposit": M6_deposit.values})
    M9_df_deposit = pd.DataFrame({cst_id: M9_deposit.index, "M9_deposit": M9_deposit.values})
    M12_df_deposit = pd.DataFrame({cst_id: M12_deposit.index, "M12_deposit": M12_deposit.values})
    cst_id_Mx_deposit = M3_df_deposit.merge(M6_df_deposit, on=cst_id).merge(M9_df_deposit, on=cst_id).merge(M12_df_deposit, on=cst_id)
    df_list.append(cst_id_Mx_deposit)

    tmp_withdraw= pd.pivot_table(file, values=withdraw, index=cst_id, columns=acg_dt, aggfunc="sum")
    columns_list = list(tmp_withdraw.columns.sort_values())
    M3_withdraw = tmp_withdraw[columns_list[:3]].apply(lambda x: x.sum(), axis=1)
    M6_withdraw = tmp_withdraw[columns_list[:6]].apply(lambda x: x.sum(), axis=1)
    M9_withdraw = tmp_withdraw[columns_list[:9]].apply(lambda x: x.sum(), axis=1)
    M12_withdraw = tmp_withdraw[columns_list[:12]].apply(lambda x: x.sum(), axis=1)
    M3_df_withdraw = pd.DataFrame({cst_id: M3_withdraw.index, "M3_withdraw": M3_withdraw.values})
    M6_df_withdraw = pd.DataFrame({cst_id: M6_withdraw.index, "M6_withdraw": M6_withdraw.values})
    M9_df_withdraw = pd.DataFrame({cst_id: M9_withdraw.index, "M9_withdraw": M9_withdraw.values})
    M12_df_withdraw = pd.DataFrame({cst_id: M12_withdraw.index, "M12_withdraw": M12_withdraw.values})
    cst_id_Mx_withdraw = M3_df_withdraw.merge(M6_df_withdraw, on=cst_id).merge(M9_df_withdraw, on=cst_id).merge(M12_df_withdraw, on=cst_id)
    df_list.append(cst_id_Mx_withdraw)
    print("get M3_*,M6_*,M9_*,M12_*")

    # file_count_m = file.groupby(cst_id)[acg_dt].agg('count')#有数据的月份
    file_count_m_df_deposit = pd.DataFrame({cst_id: file_count_deposit.index, 'm_deposit': file_count_deposit.values})
    df_list.append(file_count_m_df_deposit)
    file_count_m_df_withdraw = pd.DataFrame({cst_id: file_count_withdraw.index, 'm_deposit': file_count_withdraw.values})
    df_list.append(file_count_m_df_withdraw)
    print("get m_*")

    # 拉平均 (总和/总月份数) my_*
    file_mean_la_deposit = file_sum_deposit / float(len(file["acg_dt"].drop_duplicates().tolist()))  # 均值
    file_mean_la_df_deposit = pd.DataFrame({'cst_id': file_mean_la_deposit.index, 'my_deposit': file_mean_la_deposit.values})
    df_list.append(file_mean_la_df_deposit)
    file_mean_la_withdraw = file_sum_withdraw / float(len(file["acg_dt"].drop_duplicates().tolist()))  # 均值
    file_mean_la_df_withdraw = pd.DataFrame({'cst_id': file_mean_la_withdraw.index, 'my_deposit': file_mean_la_withdraw.values})
    df_list.append(file_mean_la_df_withdraw)
    print("my_*")

    # 中间计算值1 sp_
    file['acg_dt_month'] = map(lambda x: int(x.split("-")[1]), file[acg_dt])
    sum_mouth_by_id = file.groupby([cst_id])['acg_dt_month'].agg('sum')

    file['acg_dt_month_X_deposit'] = file['acg_dt_month'] * file[deposit]
    sum_acg_dt_mounth_X_dep_ac_deposit_id = file.groupby([cst_id])['acg_dt_month_X_deposit'].agg('sum')
    value1_deposit = sum_acg_dt_mounth_X_dep_ac_deposit_id * sum_mouth_by_id
    value1_df_deposit = pd.DataFrame({'cst_id': value1_deposit.index, 'sp_deposit': value1_deposit.values})
    df_list.append(value1_df_deposit)

    file['acg_dt_month_X_withdraw'] = file['acg_dt_month'] * file[withdraw]
    sum_acg_dt_mounth_X_dep_withdrawl_by_id = file.groupby([cst_id])['acg_dt_month_X_withdraw'].agg('sum')
    value1_withdraw = sum_acg_dt_mounth_X_dep_withdrawl_by_id * sum_mouth_by_id
    value1_df_withdraw = pd.DataFrame({'cst_id': value1_withdraw.index, 'sp_deposit': value1_withdraw.values})
    df_list.append(value1_df_withdraw)
    print("get sp_*")

    # 中间计算值2 mx_
    mouth_count = file.groupby([cst_id])[acg_dt].agg('count')
    value2 = sum_mouth_by_id / mouth_count
    value2_df = pd.DataFrame({cst_id: value2.index, 'mx_deposit': value2.values})
    df_list.append(value2_df)
    print("get mx_*")

    # 中间计算值3
    value3_deposit = mouth_count ** 2 * file_mean_la_deposit * value2
    value3_df_deposit = pd.DataFrame({'cst_id': value3_deposit.index, 'ps_deposit': value3_deposit.values})
    df_list.append(value3_df_deposit)
    value3_withdraw = mouth_count ** 2 * file_mean_la_withdraw * value2
    value3_df_withdraw = pd.DataFrame({'cst_id': value3_withdraw.index, 'ps_deposit': value3_withdraw.values})
    df_list.append(value3_df_withdraw)
    print("get ps_*")

    # 中间计算值 4 sqx_
    value4 = (mouth_count * value2) ** 2
    value4_df = pd.DataFrame({'cst_id': value4.index, 'sqx_deposit': value4.values})
    df_list.append(value4_df)
    print("get sqx_*")

    # 中间计算值 5 sxq_
    file['acg_dt_month_sqrt_2'] = file['acg_dt_month'] ** 2
    value5 = mouth_count * file.groupby(['cst_id'])['acg_dt_month_sqrt_2'].agg('sum')
    value5_df = pd.DataFrame({cst_id: value5.index, 'sxq_deposit': value5.values})
    df_list.append(value5_df)
    print("get sxq_*")

    # B1
    B1_deposit = (value1_deposit - value3_deposit) / (value5 - value4)
    B1_df_deposit = pd.DataFrame({'cst_id': B1_deposit.index, 'B1_deposit': B1_deposit.values})
    df_list.append(B1_df_deposit)
    B1_withdraw = (value1_withdraw - value3_withdraw) / (value5 - value4)
    B1_df_withdraw = pd.DataFrame({'cst_id': B1_withdraw.index, 'B1_withdraw': B1_withdraw.values})
    df_list.append(B1_df_withdraw)
    print("get B1")

    # B0
    B0_deposit = file_mean_la_deposit - B1_deposit * value2
    B0_df_deposit = pd.DataFrame({'cst_id': B0_deposit.index, 'B0_deposit': B0_deposit.values})
    df_list.append(B0_df_deposit)
    B0_withdraw = file_mean_la_withdraw - B1_withdraw * value2
    B0_df_withdraw = pd.DataFrame({'cst_id': B0_withdraw.index, 'B0_withdraw': B0_withdraw.values})
    df_list.append(B0_df_withdraw)
    print("get B0")

    #类别 week
    weeks = ["01", "02", "03", "04", "05", "06", "07"]
    deposit_num_week_columns_name = {int(week): "weekd_deposit_num_" + week for week in weeks}
    deposit_sum_week_columns_name = {int(week): "weekd_deposit_sum_" + week for week in weeks}
    deposit_num_week_df = pd.pivot_table(file.query(deposit+">0"), values=deposit, index=cst_id, columns='weekd', aggfunc="count")
    deposit_sum_week_df = pd.pivot_table(file.query(deposit + ">0"), values=deposit, index=cst_id, columns='weekd',
                                      aggfunc="sum")
    deposit_num_week_df.rename(columns=deposit_num_week_columns_name, inplace=True)
    deposit_sum_week_df.rename(columns=deposit_sum_week_columns_name,inplace=True)
    df_list.append(deposit_num_week_df)
    df_list.append(deposit_sum_week_df)
    print("get week_*_*")

    # 类别 hour
    hours = ['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
    deposit_num_hour_columns_name = {int(hour): "hourd_deposit_num_" + hour for hour in hours}
    deposit_sum_hour_columns_name = {int(hour): "hourd_deposit_sum_" + hour for hour in hours}
    deposit_num_hour_df = pd.pivot_table(file.query(deposit + ">0"), values=deposit, index=cst_id, columns='hourd',
                                         aggfunc="count")
    deposit_sum_hour_df = pd.pivot_table(file.query(deposit + ">0"), values=deposit, index=cst_id, columns='hourd',
                                         aggfunc="sum")
    deposit_num_hour_df.rename(columns=deposit_num_hour_columns_name, inplace=True)
    deposit_sum_hour_df.rename(columns=deposit_sum_hour_columns_name, inplace=True)
    df_list.append(deposit_num_hour_df)
    df_list.append(deposit_sum_hour_df)
    print("get hour_*_*")

    # join dataframe
    df_result = df_list[0]
    for item in df_list[1:]:
        df_result = df_result.merge(item, on=cst_id)

    file_name = file_type + "_result.csv"
    df_result.to_csv(file_name)
    print("End of mission")

    pass


"""
字段说明
1.file_path 文件路径
2.ac_bal 账户金额
3.cst_id 客户ID
4.acg_dt 字段更新时间
5.file_type 文件类型
"""


def file_paser(file_path, ac_bal, cst_id, acg_dt, file_type):
    file = pd.read_csv(file_path)

    df_list = []

    file_sum = file.groupby(cst_id)[ac_bal].agg('sum')  # 总和
    file_sum_df = pd.DataFrame({'cst_id': file_sum.index, 'sum_'+ac_bal: file_sum.values})
    df_list.append(file_sum_df)
    print("get sum_*")

    file_mean = file_sum / float(len(file[acg_dt].drop_duplicates().tolist()))  # 均值
    file_mean_df = pd.DataFrame({'cst_id': file_mean.index, 'mean_'+ac_bal: file_mean.values})
    df_list.append(file_mean_df)
    print("get mean_*")

    file_max = file.groupby(cst_id)[ac_bal].agg('max')  # 最大值
    file_max_df = pd.DataFrame({'cst_id': file_max.index, 'max_'+ac_bal: file_max.values})
    df_list.append(file_max_df)
    print("get max_*")

    file_min = file.groupby(cst_id)[ac_bal].agg('min')  # 最小值
    file_min_df = pd.DataFrame({'cst_id': file_min.index, 'min_'+ac_bal: file_min.values})
    df_list.append(file_min_df)
    print("get min_*")

    file_std = file.groupby(cst_id)[ac_bal].agg('std')  # 标准差
    file_std_df = pd.DataFrame({'cst_id': file_std.index, 'std_'+ac_bal: file_std.values})
    df_list.append(file_std_df)
    print("get std_*")

    file_count = file.groupby(cst_id)[ac_bal].count()  # 记录数
    file_count_df = pd.DataFrame({'cst_id': file_count.index, ac_bal+'_num': file_count.values})
    df_list.append(file_count_df)
    print("get count_*")

    file_ac_bal_by_mounth = pd.pivot_table(file, values=ac_bal, index=cst_id, columns=acg_dt, aggfunc="sum")  # 分月余额
    rename_columns = {column: column + '_'+ac_bal for column in file_ac_bal_by_mounth.columns}
    file_ac_bal_by_mounth.rename(columns=rename_columns, inplace=True)
    file_ac_bal_by_mounth[cst_id] = file_ac_bal_by_mounth.index
    df_list.append(file_ac_bal_by_mounth)
    print("get *01,*02,…")

    # 最大余额月份
    user_max_mounth = {cst_id: [], "man_"+acg_dt: []}
    for name, group in tqdm(file[[cst_id, acg_dt, ac_bal]].groupby([cst_id])):
        user_max_mounth[cst_id].append(name)
        user_max_mounth["man_"+acg_dt].append(group.xs(group[ac_bal].argmax())[acg_dt])
    user_max_mounth_df = pd.DataFrame(user_max_mounth)
    df_list.append(user_max_mounth_df)
    print("get mam_*")

    # 前三个月总和
    # 前六个月总和
    # 前九个月总和
    # 十二个月总和
    tmp = pd.pivot_table(file, values=ac_bal, index=cst_id, columns=acg_dt, aggfunc="sum")
    columns_list = list(tmp.columns.sort_values())
    M3 = tmp[columns_list[:3]].apply(lambda x: x.sum(), axis=1)
    M6 = tmp[columns_list[:6]].apply(lambda x: x.sum(), axis=1)
    M9 = tmp[columns_list[:9]].apply(lambda x: x.sum(), axis=1)
    M12 = tmp[columns_list[:12]].apply(lambda x: x.sum(), axis=1)
    M3_df = pd.DataFrame({cst_id: M3.index, "M3_"+ac_bal: M3.values})
    M6_df = pd.DataFrame({cst_id: M6.index, "M6_"+ac_bal: M6.values})
    M9_df = pd.DataFrame({cst_id: M9.index, "M9_"+ac_bal: M9.values})
    M12_df = pd.DataFrame({cst_id: M12.index, "M12_"+ac_bal: M12.values})
    cst_id_Mx = M3_df.merge(M6_df, on=cst_id).merge(M9_df, on=cst_id).merge(M12_df, on=cst_id)
    df_list.append(cst_id_Mx)
    print("get M3_*,M6_*,M9_*,M12_*")

    # file_count_m = file.groupby(cst_id)[acg_dt].agg('count')#有数据的月份
    file_count_m_df = pd.DataFrame({cst_id: file_count.index, 'm_'+ac_bal: file_count.values})
    df_list.append(file_count_m_df)
    print("get m_*")

    # 拉平均 (总和/总月份数) my_*
    file_mean_la = file_sum / float(len(file["acg_dt"].drop_duplicates().tolist()))  # 均值
    file_mean_la_df = pd.DataFrame({'cst_id': file_mean_la.index, 'my_'+ac_bal: file_mean_la.values})
    df_list.append(file_mean_la_df)
    print("get my_*")

    # 中间计算值1 sp_
    file['acg_dt_month'] = map(lambda x: int(x.split("-")[1]), file[acg_dt])
    file['acg_dt_month_X_ac_bal'] = file['acg_dt_month'] * file[ac_bal]
    sum_mouth_by_id = file.groupby([cst_id])['acg_dt_month'].agg('sum')
    sum_acg_dt_mounth_X_dep_ac_bal_by_id = file.groupby([cst_id])['acg_dt_month_X_ac_bal'].agg('sum')
    value1 = sum_acg_dt_mounth_X_dep_ac_bal_by_id * sum_mouth_by_id
    value1_df = pd.DataFrame({'cst_id': value1.index, 'sp_'+ac_bal: value1.values})
    df_list.append(value1_df)
    print("get sp_*")

    # 中间计算值2 mx_
    mouth_count = file.groupby([cst_id])[acg_dt].agg('count')
    value2 = sum_mouth_by_id / mouth_count
    value2_df = pd.DataFrame({cst_id: value2.index, 'mx_'+ac_bal: value2.values})
    df_list.append(value2_df)
    print("get mx_*")

    # 中间计算值3
    value3 = mouth_count ** 2 * file_mean_la * value2
    value3_df = pd.DataFrame({'cst_id': value3.index, 'ps_'+ac_bal: value3.values})
    df_list.append(value3_df)
    print("get ps_*")

    # 中间计算值 4 sqx_
    value4 = (mouth_count * value2) ** 2
    value4_df = pd.DataFrame({'cst_id': value4.index, 'sqx_'+ac_bal: value4.values})
    df_list.append(value4_df)
    print("get sqx_*")

    # 中间计算值 5 sxq_
    file['acg_dt_month_sqrt_2'] = file['acg_dt_month'] ** 2
    value5 = mouth_count * file.groupby(['cst_id'])['acg_dt_month_sqrt_2'].agg('sum')
    value5_df = pd.DataFrame({cst_id: value5.index, 'sxq_'+ac_bal: value5.values})
    df_list.append(value5_df)
    print("get sxq_*")

    # B1
    B1 = (value1 - value3) / (value5 - value4)
    B1_df = pd.DataFrame({'cst_id': B1.index, 'B1_'+ac_bal: B1.values})
    df_list.append(B1_df)
    print("get B1_*")

    # B0
    B0 = file_mean_la - B1 * value2
    B0_df = pd.DataFrame({'cst_id': B0.index, 'B0_'+ac_bal: B0.values})
    df_list.append(B0_df)
    print("get B0_*")

    # join dataframe
    df_result = df_list[0]
    for item in df_list[1:]:
        df_result = df_result.merge(item, on=cst_id)

    file_name = file_type + "_result.csv"
    df_result.to_csv(file_name)
    print("End of mission")

    pass


def main(args):
    args = args
    file_path = args.file_path
    ac_bal = args.ac_bal
    cst_id = args.cst_id
    acg_dt = args.acg_dt
    withdraw = args.withdraw
    file_type = args.file_type

    if file_type == "his":
        deposit = ac_bal
        his_paser(file_path, deposit, withdraw, cst_id, acg_dt, file_type)
    else:
        file_paser(file_path, ac_bal, cst_id, acg_dt, file_type)
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=" data analyse pipeline ",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--file_path", type=str, default=None, help="file path")
    parser.add_argument("--ac_bal", type=str, default=None, help="ac bal columns name")
    parser.add_argument("--cst_id", type=str, default=None, help="user id columns name")
    parser.add_argument("--acg_dt", type=str, default=None, help="update time columns name")
    parser.add_argument("--withdraw", type=str, default=None, help="withdraw columns name")
    parser.add_argument("--file_type", type=str, default=None, help="file type like file,fnc,his,loan")
    args = parser.parse_args()
    sys.exit(main(args))
