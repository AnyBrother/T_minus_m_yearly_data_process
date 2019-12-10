# T_minus_m_yearly_data_process
This is the python codes that convert panel data into t-m data.

## 中国上市企业t-m数据库构建的代码操作说明.2019.07.01.
**可参考操作说明"2019.07.01.t-m数据库构建的代码操作说明.doc"**
### 一、Python_3.6版本安装及工具包完善
#### 1.1下载“Anaconda_3.6_version”
下载方式1：详见网站’ https://www.anaconda.com/download/’；

下载方式2：也可直接运行文件夹中的’ Anaconda3-5.0.0-Windows-x86_64.exe’，完成安装。

#### 1.2配置对应的Tools package
(1)按组合键‘Ctrl+R’，输入“Anaconda Prompt”，右键“管理员方式打开”，打开命令cmd窗口。

(2)接下来,配置相应工具包,分别在cmd界面,输入“pip install pandas”/“pip install time”/“pip install copy”,完成pandas包/time包和copy包这3个工具包的安装.


### 二、程序运行
(1)将程序代码"t_minus_m_yearly_data_process_ykp.py"、数据"上市基输入样例.xlsx"，放入同一个文件夹下.

(2)打开“Spyder”软件，打开“yearly_data_process_ykp_v4.0(平均抽).py”程序，按F5运行即可，结果将以“.xlsx”格式输出excel文件。

比如“上市t-m数据(所有年份企业数量一样)[平均抽].xlsx”的excel文件，excel文件将出现在“yearly_data_process_ykp_v4.0(平均抽).py”程序所在的文件夹下。

(3)打开代码后,设置相应参数.

数据要形成和"上市基输入样例.xlsx"一致的格式,并设置好以下参数,包括:sheet名/违约状态列名/年份列名/证券代码列名/t-m中m的取值/数据起始终止时间.
```python
file_in_name = "上市基输入.xlsx"  # 读取excel的文件名称
sheet_in_name = "Sheet1"  # 读取excel的子表名称
y_name = "(612)违约状态"  # 数据的"违约状态"列名
need_change_name = ["(612)违约状态"]  # 数据的需要与"违约状态"同时变化的列名list
yearly_name = "年份"  # 数据的"年份"列名
code_name = "证券代码"  # 数据的企业编码"证券代码"列名
lag_time_span = 5  # 所想要的t-m中的m值
all_year_sort_list = []  # 标准的年份(由小到大排序)
for y in range(2000, 2018, 1): # 数据年份填写为[起始时间,最终时间+1],如果是[2000,2017]则填写(2000,2018,1).
    all_year_sort_list.append(str(y))
    all_year_sort_list = [int(x) for x in all_year_sort_list]
    all_year_sort_list.sort()
all_lag_company_same = True  # True-输出"t-m数据(m=1,2,...的企业数量一样)", False-输出"t-m时刻数据(m=1,2,...的企业数量依次递减)"
file_out_name = "上市t-m数据"  # 读取excel的文件名称
```
至此等待程序运行完毕即可。
