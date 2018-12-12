import argparse
import pandas as pd
import sys

import tqdm


# 总和
def df_sum_fun(dep, param):
    dep_sum = dep.groupby('cst_id')[param].agg('sum')
    return pd.DataFrame({'cst_id': dep_sum.index, 'sum_*': dep_sum.values})


# 均值
def df_mean_fun(dep, param):
    dep_sum = dep.groupby('cst_id')[param].agg('sum')
    dep_mean = dep_sum / float(len(dep[param].drop_duplicates().tolist()))
    return pd.DataFrame({'cst_id': dep_mean.index, 'mean_*': dep_mean.values})


# 最大值
def df_max_fun(dep, param):
    dep_max = dep.groupby('cst_id')[param].agg('max')
    return pd.DataFrame({'cst_id': dep_max.index, 'max_*': dep_max.values})


def df_min_fun(dep, param):
    dep_min = dep.groupby('cst_id')[param].agg('min')  # 最小值
    return pd.DataFrame({'cst_id': dep_min.index, 'min_*': dep_min.values})


# 标准差
def df_std_fun(dep, param):
    dep_std = dep.groupby('cst_id')[param].agg('std')
    return pd.DataFrame({'cst_id': dep_std.index, 'std_*': dep_std.values})
    pass


# 记录数
def df_count_fun(dep, param):
    dep_count = dep.groupby('cst_id')[param].count()
    return pd.DataFrame({'cst_id': dep_count.index, '*_num': dep_count.values})
    pass

 # 分月余额
def ac_bal_by_mounth_fun(dep, param):
    acg_dt = dep["acg_dt"].drop_duplicates().tolist()
    cst_id = dep["cst_id"].drop_duplicates().tolist()
    dep_ac_bal_by_mounth = {i: [] for i in acg_dt}
    dep_ac_bal_by_mounth.update({"cst_id": []})

    for cstid in tqdm.tqdm(cst_id):
        group = dep[dep['cst_id'] == cstid].groupby("acg_dt")[param].agg('sum')
        for mounth in acg_dt:
            if mounth in group.index:
                dep_ac_bal_by_mounth[mounth].append(group.xs(mounth))
            else:
                dep_ac_bal_by_mounth[mounth].append(0)
        dep_ac_bal_by_mounth["cst_id"].append(cstid)
    return pd.DataFrame(dep_ac_bal_by_mounth)

# 最大余额月份
def user_max_mounth_fum(dep, param):
    user_max_mounth = {"cst_id": [], "acg_dt": []}
    for name, group in tqdm.tqdm(dep[['cst_id', 'acg_dt', param]].groupby(['cst_id'])):
        user_max_mounth['cst_id'].append(name)
        user_max_mounth['acg_dt'].append(group.xs(group[param].argmax())['acg_dt'])
    return pd.DataFrame(user_max_mounth)

# 前三、六、九、十二个月总和
def cst_id_Mx_fun(dep, param):
    cst_id_Mx = {"cst_id": [], "M3": [], "M6": [], "M9": [], "M12": []}
    for name, group in tqdm.tqdm(dep.groupby('cst_id')):
        acg_dt_list = group.sort_values('acg_dt')['acg_dt'].drop_duplicates().tolist()
        if (len(acg_dt_list) / 3 == 1):
            mouth_3 = acg_dt_list[2]
            M3 = sum(group.query("acg_dt < " + mouth_3)[param])
            cst_id_Mx['cst_id'].append(name)
            cst_id_Mx['M3'].append(M3)
            cst_id_Mx['M6'].append(-1)
            cst_id_Mx['M9'].append(-1)
            cst_id_Mx['M12'].append(-1)
        elif (len(acg_dt_list) / 3 == 2):
            mouth_3 = acg_dt_list[2]
            mouth_6 = acg_dt_list[5]
            M3 = sum(group.query("acg_dt < " + mouth_3)[param])
            M6 = sum(group.query("acg_dt < " + mouth_6)[param])
            cst_id_Mx['cst_id'].append(name)
            cst_id_Mx['M3'].append(M3)
            cst_id_Mx['M6'].append(M6)
            cst_id_Mx['M9'].append(-1)
            cst_id_Mx['M12'].append(-1)
        elif (len(acg_dt_list) / 3 == 3):
            mouth_3 = acg_dt_list[2]
            mouth_6 = acg_dt_list[5]
            mouth_9 = acg_dt_list[8]
            M3 = sum(group.query("acg_dt < " + mouth_3)[param])
            M6 = sum(group.query("acg_dt < " + mouth_6)[param])
            M9 = sum(group.query("acg_dt < " + mouth_9)[param])
            cst_id_Mx['cst_id'].append(name)
            cst_id_Mx['M3'].append(M3)
            cst_id_Mx['M6'].append(M6)
            cst_id_Mx['M9'].append(M9)
            cst_id_Mx['M12'].append(-1)
        elif (len(acg_dt_list) / 3 == 4):
            mouth_3 = acg_dt_list[2]
            mouth_6 = acg_dt_list[5]
            mouth_9 = acg_dt_list[8]
            mouth_12 = acg_dt_list[11]
            M3 = sum(group.query("acg_dt < " + mouth_3)[param])
            M6 = sum(group.query("acg_dt < " + mouth_6)[param])
            M9 = sum(group.query("acg_dt < " + mouth_9)[param])
            M12 = sum(group.query("acg_dt < " + mouth_12)[param])
            cst_id_Mx['cst_id'].append(name)
            cst_id_Mx['M3'].append(M3)
            cst_id_Mx['M6'].append(M6)
            cst_id_Mx['M9'].append(M9)
            cst_id_Mx['M12'].append(M12)
        else:
            cst_id_Mx['cst_id'].append(name)
            cst_id_Mx['M3'].append(-1)
            cst_id_Mx['M6'].append(-1)
            cst_id_Mx['M9'].append(-1)
            cst_id_Mx['M12'].append(-1)
    return pd.DataFrame(cst_id_Mx)

 # 有数据的月份
def count_m_fun(dep):
    dep_count_m = dep.groupby('cst_id')['acg_dt'].agg('count')
    return pd.DataFrame({'cst_id': dep_count_m.index, 'm_*': dep_count_m.values})

# 拉平均 (总和/总月份数) my_*
def mean_la_fun(dep, param):
    dep_sum = dep.groupby('cst_id')[param].agg('sum')
    dep_mean_la = dep_sum / float(len(dep["acg_dt"].drop_duplicates().tolist()))  # 均值
    return dep_mean_la


def middle_value(dep,mean_la, param):
    # 中间计算值1 sp_
    dep['acg_dt_mounth'] = map(lambda x: int(x.split("-")[1]), dep['acg_dt'])
    dep['acg_dt_mounth_X_dep_ac_bal'] = dep['acg_dt_mounth'] * dep[param]
    sum_mouth_by_id = dep.groupby(['cst_id'])['acg_dt_mounth'].agg('sum')
    sum_acg_dt_mounth_X_dep_ac_bal_by_id = dep.groupby(['cst_id'])['acg_dt_mounth_X_dep_ac_bal'].agg('sum')
    value1 = sum_acg_dt_mounth_X_dep_ac_bal_by_id * sum_mouth_by_id
    value1_df = pd.DataFrame({'cst_id': value1.index, 'sp_': value1.values})

    # 中间计算值2 mx_
    mouth_count = dep.groupby(['cst_id'])['acg_dt'].agg('count')
    value2 = sum_mouth_by_id / mouth_count
    value2_df = pd.DataFrame({'cst_id': value2.index, 'mx_': value2.values})

    # 中间计算值3
    value3 = mouth_count ** 2 * mean_la * value2
    value3_df = pd.DataFrame({'cst_id': value3.index, 'ps_': value3.values})

    # 中间计算值 4 sqx_
    value4 = (mouth_count * value2) ** 2
    value4_df = pd.DataFrame({'cst_id': value4.index, 'sqx_': value4.values})

    # 中间计算值 5 sxq_
    dep['acg_dt_mounth_sqrt_2'] = dep['acg_dt_mounth'] ** 2
    value5 = mouth_count * dep.groupby(['cst_id'])['acg_dt_mounth_sqrt_2'].agg('sum')
    value5_df = pd.DataFrame({'cst_id': value5.index, 'sxq_': value5.values})

    # B1
    B1 = (value1 - value3) / (value5 - value4)
    B1_df = pd.DataFrame({'cst_id': B1.index, 'B1': B1.values})

    # B0
    B0 = mean_la - B1 * value2
    B0_df = pd.DataFrame({'cst_id': B0.index, 'B0': B0.values})

    return value1_df,value2_df,value3_df,value4_df,value5_df,B1_df,B0_df

def main():
    args = parse_arg()
    dep_path = args.dep_path
    fnc_path = args.fnc_path
    his_path = args.his_path
    loan_path = args.loan_path

    dep = pd.read_csv(dep_path)  # dep 存款表
    fnc = pd.read_csv(fnc_path)  # fnc 理财表
    his = pd.read_csv(his_path)  # his 流水表
    loan = pd.read_csv(loan_path)  # loan 贷款表

    acg_dt = map(lambda x: "20" + x.split("M")[0] + "-" + x.split("M")[1], dep['acg_dt'])
    dep["acg_dt"] = acg_dt

    dep_sum_df = df_sum_fun(dep, 'dep_ac_bal')
    dep_mean_df = df_mean_fun(dep, 'dep_ac_bal')
    dep_max_df = df_max_fun(dep, 'dep_ac_bal')
    dep_min_df = df_min_fun(dep, 'dep_ac_bal')
    dep_std_df = df_std_fun(dep, 'dep_ac_bal')
    dep_count_df = df_count_fun(dep, 'dep_ac_bal')

    dep_ac_bal_by_mounth_df = ac_bal_by_mounth_fun(dep,'dep_ac_bal')
    dep_user_max_mounth_df = user_max_mounth_fum(dep,'dep_ac_bal')

    dep_cst_id_Mx_df = cst_id_Mx_fun(dep,"dep_ac_bal")

    dep_count_m_df = count_m_fun(dep,"dep_ac_bal")

    dep_mean_la = mean_la_fun(dep,"dep_ac_bal")
    dep_mean_la_df = pd.DataFrame({'cst_id': dep_mean_la.index, 'my_*': dep_mean_la.values})

    value1_df, value2_df, value3_df, value4_df, value5_df, B1_df, B0_df = middle_value(dep,dep_mean_la,"dep_ac_bal")

    # join dataframe
    print "join middle Dataframe"
    dep_tmp1 = dep_sum_df.merge(dep_mean_df, on='cst_id').merge(dep_max_df, on='cst_id').merge(dep_min_df,
                                                                                           on='cst_id').merge(
        dep_std_df, on='cst_id').merge(dep_count_df, on='cst_id').merge(dep_count_m_df, on='cst_id').merge(
        dep_mean_la_df, on='cst_id').merge(value1_df, on='cst_id').merge(value2_df, on='cst_id').merge(value3_df,
                                                                                                       on='cst_id').merge(
        value4_df, on='cst_id').merge(value5_df, on='cst_id').merge(B0_df, on='cst_id').merge(B1_df, on='cst_id')
    dep_result = dep_tmp1.merge(dep_ac_bal_by_mounth_df, on='cst_id').merge(dep_user_max_mounth_df, on='cst_id').merge(dep_cst_id_Mx_df,
                                                                                                           on='cst_id')

    dep_result.to_csv('dep_result.csv')

    pass


def parse_arg():
    parser = argparse.ArgumentParser(description=" data analyse pipeline ",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--dep_path", type=str, default=None, help="dep path")
    parser.add_argument("--fnc_path", type=str, default=None, help="fnc path")
    parser.add_argument("--his_path", type=str, default=None, help="his path")
    parser.add_argument("--loan_path", type=str, default=None, help="loan path")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    sys.exit(main)
