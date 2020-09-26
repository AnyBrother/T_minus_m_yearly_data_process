# -*- coding: utf-8 -*-
"""
@Time    : 2018/11/29 21:59
@Software: PyCharm
@Author  : 308-11
version 1.0: 将t_1, t_2,...,t_5的时间数据-全部导出并存储
"""
import pandas as pd
import time
import copy


def determine_whether_continuous_span_ykp(list_for_check, continuous_list, con_num_span):
    # 检验list_for_check中是否存在如continuous_list连续的序列,连续的长度为con_num_span+1
    """
    Input variables:
        list_for_check    ——  为检验用的list
        continuous_list   ——  标准的连续list
        con_num_span      ——  连续的长度
    Output variables:
        bool_check        ——  是否存在连续的长度为con_num_span+1的子列
        max_value         ——  如果存在,连续长度的末尾值的最大年份值;否则,为9999
        max_value_index   ——  如果存在,连续长度的末尾值的最大年份值索引;否则,为9999
    To use this function, coding:
        output = determine_whether_continuous_span_ykp(list_for_check, continuous_list, con_num_span)
    """
    from itertools import groupby
    list_for_check.sort()
    continuous_list.sort()
    index_list = [continuous_list.index(x) for x in list_for_check]
    function_for_check = lambda x: x[1] - x[0]
    bool_check = False
    max_value_index = 9999
    max_value = 9999
    for k, g in groupby(enumerate(index_list), function_for_check):
        l1 = [j for i, j in g]  # 连续数字的列表
        if len(l1) > 1:
            scope = str(min(l1)) + '-' + str(max(l1))  # 将连续数字范围用"-"连接
            if max(l1)-min(l1) >= con_num_span:
                bool_check = True
                max_value_index = max(l1)
                max_value = continuous_list[max_value_index]
                # print(max_value)
                break
        else:
            scope = l1[0]
            # print("连续数字范围：{}".format(scope))
    return bool_check, max_value, max_value_index


def determine_whether_continuous_span_default_ykp(list_for_check, continuous_list, con_num_span, y_list):
    # 检验list_for_check中是否存在:以1为末尾值的,如continuous_list连续的序列,连续的长度为con_num_span+1
    """
    Input variables:
        list_for_check    ——  为检验用的list
        continuous_list   ——  标准的连续list
        con_num_span      ——  连续的长度
        y_list            ——  与list_for_check对应的0-1的序列
    Output variables:
        bool_check        ——  是否存在以1为末尾值的连续的长度为con_num_span+1的子列
        max_value         ——  如果存在,连续长度的末尾值的最大年份值;否则,为9999
        max_value_index   ——  如果存在,连续长度的末尾值的最大年份值索引;否则,为9999
    To use this function, coding:
        output = determine_whether_continuous_span_ykp(list_for_check, continuous_list, con_num_span)
    """
    index_1_list = [y_list.index(x) for x in y_list if x == 1]
    year_1_list = [list_for_check[x] for x in index_1_list]
    list_for_check.sort()
    list_for_check_sort = list_for_check
    bool_check = False
    max_value_index = 9999
    max_value = 9999
    for i in year_1_list:
        index_temp = list_for_check_sort.index(i)
        index_temp_before = index_temp - con_num_span
        index_temp_con = continuous_list.index(i)
        index_temp_before_con = index_temp_con - con_num_span
        if index_temp_before < 0:
            break
        elif list_for_check_sort[index_temp_before] == continuous_list[index_temp_before_con] and \
                list_for_check_sort[index_temp] == continuous_list[index_temp_con]:
            bool_check = True
            max_value_index = index_temp
            max_value = list_for_check_sort[index_temp]
            break
    return bool_check, max_value, max_value_index


def data_lag_span_process_ykp(df, y_name, yearly_name, code_name, lag_time_span, all_year_sort_list, need_change_name):
    # 整理出所有满足t_m时间跨度的样本.
    # 在'df'中,按照'y_name'的标签,将'lag_time_list'对应的跨度为'lag_time_span'的样本一一对应提取
    """
    Input variables:
        df                 ——  所有原始标准化数据文件
        y_name             ——  "违约状态"的列名
        yearly_name        ——  年份列的列名
        code_name          ——  代码列的列名
        lag_time_span      ——  预期得到t-m的m值
        all_year_sort_list ——  年份列从小到大排序的list,eg:[2000,2001,2002]
        need_change_name   ——  需要与"违约状态"同时变化的列名list
    To use this function, coding:
        data_output = data_lag_span_process_ykp(df, y_name, yearly_name, code_name, lag_time_span, all_year_sort_list, need_change_name)
    """
    all_default_num = sum(list(df[y_name]))  # 所有违约的数量
    code_only_list = list(set(df[code_name]))  # 所有不重复的代码List
    temp_df_all_default = df.loc[df[y_name].isin([1]), :]  # 所有违约的样本
    default_num_all_only = len(list(set(temp_df_all_default[code_name])))  # 违约唯一标识企业的数量
    data_lag_df = pd.DataFrame()
    data_lag_del_df = pd.DataFrame()
    data_un_lag_df = pd.DataFrame()
    # 储存每年最大数量
    check_good_year_num = dict()
    check_good_year_num[str(all_year_sort_list[-1])] = 0
    check_good_year_num[str(all_year_sort_list[-1]) + "_max"] = 9999999
    for t in all_year_sort_list[:-1]:
        temp_t_df = df.loc[df[yearly_name] == t, :]
        check_good_year_num[str(t)] = 0
        default_num = sum(list(temp_t_df[y_name]))
        check_good_year_num[str(t) + "_max"] = int(default_num/all_default_num * (len(code_only_list) - default_num_all_only))
        # check_good_year_num[str(t) + "_max"] = 241  # 每年平均241个非违约的
    data_out = dict()
    for k in range(lag_time_span+1):
        data_out['t_{}_df'.format(k)] = pd.DataFrame()
    for i in code_only_list:
        # print(i)
        temp_df_i = df.loc[df[code_name] == i, :]  # 返回第i个客户的所有指标数据的时间序列
        temp_df_i.sort_values(yearly_name, inplace=True)
        year_full_list = list(temp_df_i[yearly_name])  # 返回第i个客户的年份时间序列
        label_full_list = list(temp_df_i[y_name])  # 返回第i个客户的违约标签时间序列
        max_year_index = year_full_list.index(max(year_full_list))  # 返回年份最大值的索引
        min_year_index = year_full_list.index(min(year_full_list))  # 返回年份最小值的索引
        # 检查是否有空缺值
        if 1 in set(label_full_list):
            index_1_temp = [ind for ind, x in enumerate(label_full_list) if x == 1]
            max_year_index = year_full_list.index(max([year_full_list[x] for x in index_1_temp]))  # 返回年份最大值的索引
            max_year_sort_index = all_year_sort_list.index(max([year_full_list[x] for x in index_1_temp]))  # 返回年份最大值的索引
            min_year_sort_index = all_year_sort_list.index(year_full_list[min_year_index])
        else:
            max_year_sort_index = all_year_sort_list.index(max(year_full_list))  # 返回年份最大值的索引
            min_year_sort_index = all_year_sort_list.index(year_full_list[min_year_index])
        start_year_sort_index = max_year_sort_index-lag_time_span  # 返回开始年份的索引
        bool_retain = (max_year_index - min_year_index) >= lag_time_span \
                      and all_year_sort_list[start_year_sort_index] == year_full_list[max_year_index-lag_time_span] \
                      and all_year_sort_list[start_year_sort_index] in year_full_list \
                      and max_year_sort_index-min_year_sort_index >= lag_time_span  # 返回是否满足对应时间跨度
        if bool_retain:
            if 1 in set(label_full_list):
                index_1 = [ind for ind, x in enumerate(label_full_list) if x == 1]
                max_year_sort_index = all_year_sort_list.index(max([year_full_list[x] for x in index_1]))  # 返回年份最大值的索引
                start_year_sort_index = max_year_sort_index-lag_time_span  # 返回t-m年份开始的索引
            else:
                # start
                n_times = 0
                temp_list = all_year_sort_list[:-lag_time_span]
                year_full_list_temp = copy.deepcopy([x for x in year_full_list if x in temp_list])
                while n_times <= len(temp_list) - 1:
                    temp_year = temp_list[n_times]
                    index_temp_t_m = all_year_sort_list.index(temp_year)
                    temp_year_check = all_year_sort_list[index_temp_t_m + lag_time_span]
                    if min(year_full_list_temp) == temp_year and check_good_year_num[str(temp_year_check)] <= \
                            check_good_year_num[str(temp_year_check) + "_max"]:
                        start_year_sort_index = all_year_sort_list.index(temp_year)  # 返回年份最小值的索引
                        max_year_sort_index = start_year_sort_index + lag_time_span  # 返回t-m年份开始的索引
                        if all_year_sort_list[max_year_sort_index] in year_full_list:
                            max_year_index = year_full_list.index(all_year_sort_list[max_year_sort_index])
                        else:
                            pass
                        check_good_year_num[str(temp_year_check)] += 1
                        break
                    elif min(year_full_list_temp) == temp_year and check_good_year_num[str(temp_year_check)] > \
                            check_good_year_num[str(temp_year_check) + "_max"]:
                        year_full_list_temp.remove(min(year_full_list_temp))
                    else:
                        n_times += 1
                # end
            lag_year_sort_retain = [all_year_sort_list[x] for x in range(max_year_sort_index, max_year_sort_index-lag_time_span-1, -1)]  # 满足条件的年份
            lag_year_retain = [year_full_list[x] for x in range(max_year_index, max_year_index - lag_time_span - 1, -1)]
            lag_year_del = [x for x in year_full_list if x not in lag_year_sort_retain]  # 不满足条件的年份
            temp_lag_df = copy.deepcopy(temp_df_i.loc[temp_df_i[yearly_name].isin(lag_year_sort_retain), :])  # 第i个客户满足条件的序列
            if temp_lag_df.shape[0] < lag_time_span + 1:
                data_un_lag_df = pd.concat([data_un_lag_df, temp_df_i])
            else:
                # 将标签重新贴
                y_t = temp_lag_df.loc[temp_lag_df[yearly_name] == all_year_sort_list[max_year_sort_index], need_change_name]
                temp_need_change_df = pd.concat([y_t] * temp_lag_df.shape[0], ignore_index=True)
                temp_lag_df = temp_lag_df.drop(need_change_name, 1)
                temp_lag_df = temp_lag_df.reset_index(drop=True)
                temp_lag_df = temp_lag_df.join(temp_need_change_df)
                for j in range(lag_time_span+1):
                    # print(j)
                    temp_lag_t_i = temp_lag_df.loc[temp_lag_df[yearly_name] == all_year_sort_list[max_year_sort_index-j], :]
                    data_out['t_{}_df'.format(j)] = pd.concat([data_out['t_{}_df'.format(j)], temp_lag_t_i])
                # 排序+拼接
                temp_lag_df.sort_values(yearly_name, inplace=True)
                data_lag_df = pd.concat([data_lag_df, temp_lag_df])  # 将满足条件的部分保存
                # print(data_lag_df.shape, i)
                temp_lag_del_df = temp_df_i.loc[temp_df_i[yearly_name].isin(lag_year_del), :]  # 第i个客户不满足条件的序列
                data_lag_del_df = pd.concat([data_lag_del_df, temp_lag_del_df])  # 将不满足条件的剔除部分保存
        else:
            data_un_lag_df = pd.concat([data_un_lag_df, temp_df_i])
    # 在剔除的证券代码中,找回满足条件但被误删的非违约客户
    data_un_lag_df_non_default = data_un_lag_df.loc[data_un_lag_df[y_name].isin([0]), :]
    code_for_check_list = list(set(data_un_lag_df_non_default[code_name]))  # 所有删去的非违约中不重复的代码List
    for i in code_for_check_list:
        temp_df_i = data_un_lag_df.loc[data_un_lag_df[code_name] == i, :]  # 返回第i个客户的所有指标数据的时间序列
        list_for_check = list(temp_df_i[yearly_name])
        continuous_list = all_year_sort_list
        con_num_span = lag_time_span
        temp_out = determine_whether_continuous_span_ykp(list_for_check, continuous_list, con_num_span, )
        if temp_out[0]:
            temp_add = data_un_lag_df.loc[data_un_lag_df[code_name].isin([i]), :]
            data_un_lag_df = data_un_lag_df[~data_un_lag_df[code_name].isin([i])]
            max_year_add = temp_out[1]
            year_add_index = all_year_sort_list.index(max_year_add)
            continuous_year_list = all_year_sort_list[year_add_index-lag_time_span:year_add_index+1]
            temp_add_lag = copy.deepcopy(temp_add[temp_add[yearly_name].isin(continuous_year_list)])
            temp_add_lag_del = temp_add[~temp_add[yearly_name].isin(continuous_year_list)]
            # 将标签重新贴
            y_t = temp_add.loc[temp_add[yearly_name] == max_year_add, need_change_name]
            temp_need_change_add_df = pd.concat([y_t] * temp_add_lag.shape[0], ignore_index=True)
            temp_add_lag = temp_add_lag.drop(need_change_name, 1)
            temp_add_lag = temp_add_lag.reset_index(drop=True)
            temp_add_lag = temp_add_lag.join(temp_need_change_add_df)
            for j in range(lag_time_span + 1):
                # print(j)
                temp_lag_t_i = temp_add_lag.loc[temp_add_lag[yearly_name] == all_year_sort_list[year_add_index - j], :]
                data_out['t_{}_df'.format(j)] = pd.concat([data_out['t_{}_df'.format(j)], temp_lag_t_i])
            # 排序+拼接
            temp_add_lag.sort_values(yearly_name, inplace=True)
            data_lag_df = pd.concat([data_lag_df, temp_add_lag])  # 将满足条件的部分保存
            data_lag_del_df = pd.concat([data_lag_del_df, temp_add_lag_del])  # 将不满足条件的剔除部分保存
    # 在剔除的证券代码中,找回满足条件但被误删的违约客户
    data_un_lag_df_default = data_un_lag_df.loc[data_un_lag_df[y_name].isin([1]), :]
    code_for_check_list = list(set(data_un_lag_df_default[code_name]))  # 所有删去的非违约中不重复的代码List
    for i in code_for_check_list:
        temp_df_i = data_un_lag_df.loc[data_un_lag_df[code_name] == i, :]  # 返回第i个客户的所有指标数据的时间序列
        list_for_check = list(temp_df_i[yearly_name])
        continuous_list = all_year_sort_list
        con_num_span = lag_time_span
        y_list = list(temp_df_i[y_name])
        temp_out = determine_whether_continuous_span_default_ykp(list_for_check, continuous_list, con_num_span, y_list)
        if temp_out[0]:
            temp_add = data_un_lag_df.loc[data_un_lag_df[code_name].isin([i]), :]
            data_un_lag_df = data_un_lag_df[~data_un_lag_df[code_name].isin([i])]
            max_year_add = temp_out[1]
            year_add_index = all_year_sort_list.index(max_year_add)
            continuous_year_list = all_year_sort_list[year_add_index - lag_time_span:year_add_index + 1]
            temp_add_lag = temp_add[temp_add[yearly_name].isin(continuous_year_list)]
            temp_add_lag_del = temp_add[~temp_add[yearly_name].isin(continuous_year_list)]
            # 将标签重新贴
            y_t = temp_add.loc[temp_add[yearly_name] == max_year_add, need_change_name]
            temp_need_change_add_df = pd.concat([y_t] * temp_add.shape[0], ignore_index=True)
            temp_add_lag = temp_add_lag.drop(need_change_name, 1)
            temp_add_lag = temp_add_lag.reset_index(drop=True)
            temp_add_lag = temp_add_lag.join(temp_need_change_add_df)
            for j in range(lag_time_span + 1):
                # print(j)
                temp_lag_t_i = temp_add_lag.loc[temp_add_lag[yearly_name] == all_year_sort_list[year_add_index - j],
                               :]
                data_out['t_{}_df'.format(j)] = pd.concat([data_out['t_{}_df'.format(j)], temp_lag_t_i])
            # 排序+拼接
            temp_add_lag.sort_values(yearly_name, inplace=True)
            data_lag_df = pd.concat([data_lag_df, temp_add_lag])  # 将满足条件的部分保存
            data_lag_del_df = pd.concat([data_lag_del_df, temp_add_lag_del])  # 将不满足条件的剔除部分保存
    return data_lag_df, data_lag_del_df, data_un_lag_df, data_out, check_good_year_num


def main():
    # 参数设置
    file_in_name = "上市基输入样例.xlsx"  # 读取excel的文件名称
    sheet_in_name = "Sheet1"  # 读取excel的子表名称
    y_name = "(612)违约状态"  # 数据的"违约状态"列名
    need_change_name = ["(612)违约状态"]  # 数据的需要与"违约状态"同时变化的列名list
    yearly_name = "年份"  # 数据的"年份"列名
    code_name = "证券代码"  # 数据的企业编码"证券代码"列名
    lag_time_span = 5  # 所想要的t-m中的m值
    all_year_sort_list = []  # 标准的年份(由小到大排序)
    for y in range(2000, 2018, 1):
        all_year_sort_list.append(str(y))
    all_year_sort_list = [int(x) for x in all_year_sort_list]
    all_year_sort_list.sort()
    all_lag_company_same = True  # True-输出"t-m数据(m=1,2,...的企业数量一样)", False-输出"t-m时刻数据(m=1,2,...的企业数量依次递减)"
    file_out_name = "上市t-m数据"  # 读取excel的文件名称
    # 数据读取
    import os
    path_in = os.getcwd() + "\\"  # 获取当前工作目录路径
    df = pd.read_excel(path_in + file_in_name, sheet_name=sheet_in_name)
    # 进行t-m数据处理
    if all_lag_company_same:
        # 输出每个年份企业数量相同的版本,进行t-m拼接
        Data_output = data_lag_span_process_ykp(df, y_name, yearly_name, code_name, lag_time_span, all_year_sort_list, need_change_name)
        # 输出excel
        writer = pd.ExcelWriter(path_in + file_out_name + '(所有年份企业数量一样)[平均抽].xlsx')
        Data_output[0].to_excel(writer, sheet_name='所有滞后期数据', index=True)
        Data_output[1].to_excel(writer, sheet_name='符合要求但删去的其他年份数据', index=True)
        Data_output[2].to_excel(writer, sheet_name='不符合要求的删去的数据', index=True)
        for key in Data_output[3]:
            # print(j)
            Data_output[3][key].to_excel(writer, sheet_name=str(key)+'数据', index=True)
        writer.save()
    else:
        # 输出每个年份企业数量依次递减的版本,进行t-m拼接
        writer = pd.ExcelWriter(path_in + file_out_name + '(所有年份企业数量依次递减)[平均抽].xlsx')
        for i in range(lag_time_span):
            locals()["Data_output_"+str(i+1)] = data_lag_span_process_ykp(df, y_name, yearly_name, code_name, i+1, all_year_sort_list, need_change_name)
        for i in range(lag_time_span):
            # 输出excel
            locals()["Data_output_" + str(i + 1)][3]['t_{}_df'.format(i + 1)].to_excel(writer,
                                                                                       sheet_name='t_{}_数据'.format(i+1),
                                                                                       index=True)
        for i in range(lag_time_span):
            locals()["Data_output_" + str(i + 1)][0].to_excel(writer,
                                                              sheet_name='所有滞后期数据t_{}'.format(i+1),
                                                              index=True)
            locals()["Data_output_" + str(i + 1)][1].to_excel(writer,
                                                              sheet_name='符合要求但删去的其他年份数据t_{}'.format(i+1),
                                                              index=True)
            locals()["Data_output_" + str(i + 1)][2].to_excel(writer,
                                                              sheet_name='不符合要求的删去的数据t_{}'.format(i+1),
                                                              index=True)
        Data_output = locals()["Data_output_5"]
        writer.save()
    for t in all_year_sort_list[:-1]:
        print(str(t), Data_output[4][str(t)], Data_output[4][str(t) + "_max"])
    return Data_output


if __name__ == "__main__":
    bool_run = True
    while bool_run:
        try:
            import warnings
            warnings.filterwarnings('ignore')  # 忽略所有代码警告提示
            time_start_all = time.time()
            data_output = main()
            # 保存实验结果
            time_end_all = time.time()
            print('\n 所有程序运行成功,并保存完毕,  cost time: ', (time_end_all - time_start_all) / 60, ' 分钟.\n')
            bool_run = False
        except TypeError:
            print('\n 程序运行有误，请终止本程序')
            pass
        else:
            break
