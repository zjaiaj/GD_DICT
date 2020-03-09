# coding: utf-8
import pymysql
import time
import warnings
warnings.filterwarnings('ignore')

client = pymysql.connect(host='192.168.1.172',
                        port=3306,
                        user='root',
                        password='root123',
                        database = 'rep_assistant',
                        charset='utf8')
cursor=client.cursor(pymysql.cursors.DictCursor)

def main():
    res1 = 1
    a = 0
    while res1:
        # 调用存储过程
        start = time.clock()
        print("1.调用解析项目名称的存储过程：")
        proc1 = cursor.callproc('zk_cgxmmc')
        time.sleep(10)
        print("2.调用解析项目编号的存储过程：")
        proc2 = cursor.callproc('zk_cgxmbh')
        time.sleep(10)
        print("3.调用解析采购方式的存储过程：")
        proc3 = cursor.callproc('zk_cgcglx')
        time.sleep(10)
        print("4.调用解析客户名称的存储过程：")
        proc4 = cursor.callproc('zk_cgkhmc')
        time.sleep(10)
        #  更新已有数据
        upd = cursor.execute('update proc_data set rep_label =1 where 1=1 and rep_label is null')
        client.commit()
        end = time.clock()
        print("-----------已解析%s条数据----------" % str(a+upd))
        print("所需%s秒时间" % str(end - start))
        a += upd
        time.sleep(5)
        # 查看数据
        print("-----------读取1000条数据------------------")
        res1 = cursor.execute('insert into proc_data(id, rep_project_name, rep_project_num, \
                   rep_region, rep_page_url, rep_publish_time, rep_text, \
                   rep_site_source, rep_site_city, create_time, extends_1, extends_2, extends_3, \
                   ccgp_url, ccgp_html, status, update_time, rep_label, read_status) \
                   select id, rep_project_name, rep_project_num, \
                   rep_region, rep_page_url, rep_publish_time, rep_text, \
                   rep_site_source, rep_site_city, create_time, extends_1, extends_2, extends_3, \
                   ccgp_url, ccgp_html, status, update_time, rep_label, read_status from \
                   rep_source_data where 1=1 and rep_label is null limit 1000;')
        client.commit()
        res3 = cursor.execute('update rep_source_data set rep_label =1 where 1=1 and rep_label is null limit 1000')
        client.commit()
        #print("-------------已读取%s条数据---------------" % str(res3))
    #res4 = cursor.execute('update proc_data set rep_label =1 where rep_label is null')
    cursor.close()
    print("END")

if __name__ == '__main__':
    main()