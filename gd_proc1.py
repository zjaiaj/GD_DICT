# coding: utf-8
import pymysql
import time
import warnings
warnings.filterwarnings('ignore')

client = pymysql.connect(host='192.168.1.172',
                         port=3306,
                         user='root',
                         password='root123',
                         database='rep_assistant',
                         charset='utf8')
cursor = client.cursor(pymysql.cursors.DictCursor)

def main():
    res1 = 1
    a = 0
    while res1:
        # 调用存储过程
        start = time.clock()
        print("5.调用解析中标厂家的存储过程：")
        proc5 = cursor.callproc('zk_cgzbcj')
        time.sleep(10)
        print("6.调用解析预算金额的存储过程：")
        proc6 = cursor.callproc('zk_cgysje')
        time.sleep(10)
        print("7.调用解析中标日期的存储过程：")
        proc7 = cursor.callproc('zk_deal_date')
        time.sleep(10)
        print("8.调用解析中标金额的存储过程：")
        proc8 = cursor.callproc('zk_cgzbje')
        time.sleep(10)
        # 更新已有数据
        upd = cursor.execute('update proc_data1 set read_status =1 where read_status=0')
        client.commit()
        print("-----------已解析%s条数据----------" % str(a + upd))
        end = time.clock()
        print("所需%s秒时间" % str(end - start))
        a += upd
        time.sleep(5)
        # 查看数据
        print("-----------读取1000条数据------------------")
        res1 = cursor.execute('insert into proc_data1(id, rep_project_name, rep_project_num, \
                   rep_region, rep_page_url, rep_publish_time, rep_text, \
                   rep_site_source, rep_site_city, create_time, extends_1, extends_2, extends_3, \
                   ccgp_url, ccgp_html, status, update_time, rep_label, read_status) \
                   select id, rep_project_name, rep_project_num, \
                   rep_region, rep_page_url, rep_publish_time, rep_text, \
                   rep_site_source, rep_site_city, create_time, extends_1, extends_2, extends_3, \
                   ccgp_url, ccgp_html, status, update_time, rep_label, read_status from \
                   rep_source_data where read_status=0 limit 1000;')
        client.commit()
        res3 = cursor.execute('update rep_source_data set read_status =1 where read_status=0 limit 1000')
        client.commit()
    cursor.close()
    print("END")

if __name__ == '__main__':
    main()


