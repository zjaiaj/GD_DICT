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

# 调用存储过程
def main():
    cursor=client.cursor(pymysql.cursors.DictCursor)
    # 调用p_to_result存储过程
    cursor.callproc('p_to_result')
    client.commit()
    cursor.close()

if __name__ == '__main__':
    main()