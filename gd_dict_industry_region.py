# coding: utf-8
import jieba
import pandas as pd
import jieba.analyse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
import warnings
warnings.filterwarnings("ignore")

# 读取行业词袋
def iteralFile(dir):
    import os
    wordsbags_names={}
    if os.path.exists(dir) :
        for (root, dirs, files) in os.walk(dir):
            for filename in files:
                with open(os.path.join(root,filename), 'r',encoding='utf-8') as f:
                    wordsbags_names[filename.split('.')[0]] = [a.strip('\n') for a in f.readlines()]
    return wordsbags_names

# 导入数据
def search_gxzfcg_content():
    print('输入100条数据')
    sql = 'select id,rep_project_name from rep_source_data\
			where rep_project_name  != "" and rep_project_name is not null \
        and status = 0 and rep_site_source="企查查招标信息" limit 100;'
    df = pd.read_sql(sql, con=engine)
    df_columns = ['id', 'rep_project_name']
    df_columns.extend(hangye_bags.keys())
    return pd.DataFrame(df, columns=df_columns)

# 更新数据库，为已经提取的数据打上标签
def update_read_label_new(update_df):
    db_session=sessionmaker(bind=engine)
    session=db_session()
    a = "','"
    b = "','".join([str(i) for i in update_df.id])
    b = "'"+b+"'"
    session.execute('UPDATE rep_source_data SET status = 1 where id in (%s);'%b)
    session.commit()
    session.close()
    print('已更新100条数据')

#文本类型打分，返回的是一个字典。
def score_text(text):
    keywords = jieba.analyse.extract_tags(text, topK=10, withWeight=True)
    k = pd.DataFrame(pd.DataFrame(keywords),columns = [0,1,2])
    df_label_result={}
    tmp = pd.DataFrame(pd.DataFrame(k[2]))
    for key, value in hangye_bags.items():
        tmp[2] = 0
        tmp[2][k[0].isin(value)] = k[1]
        df_label_result[key] =0
        if tmp[2].sum()>0:
            df_label_result[key] = tmp[2].sum()/len(k[0])+0.5
            if df_label_result[key] >1:
                df_label_result[key] = 1
    return df_label_result

#获取项目类型的标签
def get_hangye_label(df):
    print('开始计算行业得分')
    df.iloc[:,2:] = pd.DataFrame(list(map(lambda x:score_text(x), df.iloc[:,1])))
    df['hangye'] = df.iloc[:, 2:22].idxmax(axis=1) #
    df['hangye'][df.iloc[:, 2:22].sum(axis=1)==0] = '其他'
    df = df[['id', 'rep_project_name', 'hangye']]
    return df

# 提取广东市区域
def get_district(text):
    dic={}
    keywords = jieba.lcut(text,cut_all = True)
    for row in guangdZone.itertuples(index = True,name = 'Pandas'):
        #求每行title和区域表地区的交集
        exist = set(row.district.split('、')) & set(keywords)
        if len(exist)> 0:
            dic['district'] = '，'.join(exist)
            dic['city'] = row.city
        exist1 = set([row.city])& set(keywords)
        if len(exist1)> 0:
            dic['city'] = row.city
    return dic

# 获取项目类型的标签
def get_quyu_label(df):
    print('计算所属区域')
    quyu_df_columns = ['id', 'rep_project_name', 'city', 'district']
    quyu_df = pd.DataFrame(df[['id', 'rep_project_name']], columns=quyu_df_columns)
    quyu_df.iloc[:, 2:] = pd.DataFrame(list(map(lambda x: get_district(x), quyu_df.iloc[:, 1])))
    return quyu_df

def concat(hangye_df, quyu_df):
    quyu_df = quyu_df.drop(['id', 'rep_project_name'], 1)
    result_df  = pd.concat([hangye_df, quyu_df], axis=1)
    return result_df

#输出结果
def LableToSql(eData,table_name):
    print('输入到mysql')
    eData.to_sql(name=table_name, con=engine, if_exists='append', index=False, index_label=False)

if __name__ == '__main__':
    # 读取行业词袋
    hangye_bags = iteralFile('wd3/hangye20_chinese')
    # 广东区域
    guangdZone = pd.read_csv("wd3/quxian.csv")
    # 新词加载
    jieba.load_userdict('wd3/addwords.csv')
    # 添加停用词列表
    jieba.analyse.set_stop_words('wd3/stopwords.txt')
    # 生成含有项目类型列名称的dataframe
    engine = create_engine("mysql+pymysql://root:root123@192.168.1.172:3306/rep_assistant?charset=utf8")
    df = search_gxzfcg_content()
    num = 0
    while not df.empty:
        start = time.clock()
        hangye_df = get_hangye_label(df)
        quyu_df = get_quyu_label(df)
        result_df = concat(hangye_df,quyu_df)
        print(result_df[:5])
        LableToSql(result_df, 'rep_source_data_industry_region') # 结果输出最终表
        update_read_label_new(df)
        time.sleep(5)
        df = search_gxzfcg_content()
        num += 100
        end = time.clock()
        mid = end-start
        print ('%s to %s...'%(str(num-100),str(num)),mid)
    print('end')