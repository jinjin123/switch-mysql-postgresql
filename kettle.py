#!/usr/bin/python
#coding: utf-8
import pymysql
import pymysql.cursors
import datetime
import sys
import logging
from logging.handlers import RotatingFileHandler
from subprocess import check_output
import os
from lxml import etree
try:
    conn = pymysql.connect(host='192.168.0.117',port=3306,user='root',password='jinjin123',database='testing',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
except Exception,e:
    print str(e)

def ExecNonQuery(sql):
    cur.execute(sql)
    conn.commit()
    conn.close()

def ExecQuery(sql):
    cur.execute(sql)
    resList = cur.fetchall()
    # conn.close()
    return resList


class ColoredFormatter(logging.Formatter):
    '''A colorful formatter.'''

    def __init__(self, fmt = None, datefmt = None):
        logging.Formatter.__init__(self, fmt, datefmt)
        # Color escape string
        COLOR_RED='\033[1;31m'
        COLOR_GREEN='\033[1;32m'
        COLOR_YELLOW='\033[1;33m'
        COLOR_BLUE='\033[1;34m'
        COLOR_PURPLE='\033[1;35m'
        COLOR_CYAN='\033[1;36m'
        COLOR_GRAY='\033[1;37m'
        COLOR_WHITE='\033[1;38m'
        COLOR_RESET='\033[1;0m'

        # Define log color
        self.LOG_COLORS = {
            'DEBUG': '%s',
            'INFO': COLOR_GREEN + '%s' + COLOR_RESET,
            'WARNING': COLOR_YELLOW + '%s' + COLOR_RESET,
            'ERROR': COLOR_RED + '%s' + COLOR_RESET,
            'CRITICAL': COLOR_RED + '%s' + COLOR_RESET,
            'EXCEPTION': COLOR_RED + '%s' + COLOR_RESET,
        }

    def format(self, record):
        level_name = record.levelname
        msg = logging.Formatter.format(self, record)

        return self.LOG_COLORS.get(level_name, '%s') % msg

class Log(object):

    '''
    log
    '''
    def __init__(self, filename, level="debug", logid="qiueer", mbs=20, count=10, is_console=True):
        '''
        mbs: how many MB
        count: the count of remain
        '''
        try:
            self._level = level
            #print "init,level:",level,"\t","get_map_level:",self._level
            self._filename = filename
            self._logid = logid

            self._logger = logging.getLogger(self._logid)
            file_path = os.path.split(self._filename)[0]
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            if not len(self._logger.handlers):
                self._logger.setLevel(self.get_map_level(self._level))

                fmt = '[%(asctime)s] %(levelname)s\n%(message)s'
                datefmt = '%Y-%m-%d %H:%M:%S'
                formatter = logging.Formatter(fmt, datefmt)

                maxBytes = int(mbs) * 1024 * 1024
                file_handler = RotatingFileHandler(self._filename, mode='a',maxBytes=maxBytes,backupCount=count)
                self._logger.setLevel(self.get_map_level(self._level))
                file_handler.setFormatter(formatter)
                self._logger.addHandler(file_handler)

                if is_console == True:
                    stream_handler = logging.StreamHandler(sys.stderr)
                    console_formatter = ColoredFormatter(fmt, datefmt)
                    stream_handler.setFormatter(console_formatter)
                    self._logger.addHandler(stream_handler)

        except Exception as expt:
            print expt

    def tolog(self, msg, level=None):
        try:
            level = level if level else self._level
            level = str(level).lower()
            level = self.get_map_level(level)
            if level == logging.DEBUG:
                self._logger.debug(msg)
            if level == logging.INFO:
                self._logger.info(msg)
            if level == logging.WARN:
                self._logger.warn(msg)
            if level == logging.ERROR:
                self._logger.error(msg)
            if level == logging.CRITICAL:
                self._logger.critical(msg)
        except Exception as expt:
            print expt

    def debug(self,msg):
        self.tolog(msg, level="debug")

    def info(self,msg):
        self.tolog(msg, level="info")

    def warn(self,msg):
        self.tolog(msg, level="warn")

    def error(self,msg):
        self.tolog(msg, level="error")

    def critical(self,msg):
        self.tolog(msg, level="critical")

    def get_map_level(self,level="debug"):
        level = str(level).lower()
        #print "get_map_level:",level
        if level == "debug":
            return logging.DEBUG
        if level == "info":
            return logging.INFO
        if level == "warn":
            return logging.WARN
        if level == "error":
            return logging.ERROR
        if level == "critical":
            return logging.CRITICAL

class MYSQL:
    def __init__(self,host,user,pwd,port,db):
        self.host =  host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"not set db info")
        self.conn = pymysql.connect(host=self.host,user=self.user,password=self.pwd,port=self.port,database=self.db,charset='utf8',cursorclass=pymysql.cursors.DictCursor)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"connect faild")
        else:
            return cur
    def ExecNonQuery(self,sql):
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
    def ExecQuery(self,sql):
		cur = self.__GetConnect()
		cur.execute(sql)
		resList = cur.fetchall()
                self.conn.close()
                return resList


def logwrite(sendstatus, content):
    senderr = ''
    logpath = '/tmp'
    if not sendstatus:
        content = senderr
    t = datetime.datetime.now()
    daytime = t.strftime('%Y-%m-%d')
    daylogfile = logpath+'/'+str(daytime)+'stc'+'.log'
    logger = Log(daylogfile, level="info", is_console=False, mbs=5, count=5)
    logger.info(content)


## get the max id & max unixtimetag
def get_last_id():
     senderr = ''
     sendstatus = False
     sql="select max(id),max(unixtimetag) from s_tc_middle_table"
     try:
         data = ExecQuery(sql)
         ID,TIME = data[0]['max(id)'],data[0]['max(unixtimetag)']
         update_stc_seq(ID,TIME)
         change_job(TIME)
     except Exception, e:
         senderr = str(e)
         sendstatus = False
     logwrite(sendstatus,senderr)
     return ID

 ## insert  the stc MT  last  record
def update_stc_seq(ID,TIME):
     sql="insert into  stc_log (id,unixtimetag) values (%d,%d)"% (ID,TIME)
     ExecNonQuery(sql)

def change_job(TIME):
    sendcontent = ''
    sendstatus = False
    ##full path
    sql = "select id,storeid,dates,start_time,end_time,consume_type,sale_s,net_s,tc,unixtimetag from s_tc_middle_table where  unixtimetag  = %d" %  TIME
    try:
        tree = etree.parse('1.ktr')
        root = tree.getroot()
        root.find("step").find("sql").text = sql
        tree.write('1.ktr')
        cmd = "/home/jin/data-integration/kitchen.sh  /file:/home/jin/data-integration/samples/jobs/run_all/check.kjb"
        log = check_output(cmd,shell=True)
        logwrite(True,log)
    except Exception, e:
         sendcontent = str(e)
         sendstatus = False
    logwrite(sendstatus,sendcontent)

def main(start_day,end_day,interval):
    sendcontent = ''
    sendstatus = False
    start_day=start_day
    end_day=end_day
    interval=interval
    # start_day='2017-09-01 00:00:00';
    # interval = 15
    # end_day='2017-09-02 23:59:59';
    mysql = MYSQL(host='192.168.0.117',port=3306,user='root',pwd='jinjin123',db='testing')
    # start_day = ((datetime.datetime.now()-datetime.timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M"))
    # end_day = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    storeid="'CN757023','CN010001','CN010002','CN010003','CN010004'";
    sql="""
insert into  s_tc_middle_table (dates,storeid,start_time,end_time,consume_type,sale_s,net_s,tc,unixtimetag)
  select * from (

select
                s.date as dates,
                s.storeId as storeid,
                times.start_time,
                times.end_time,
                plat as  consume_type,
                ROUND(SUM(s.sale_s),2)AS sale_s,
                ROUND(SUM(s.net_s),2) AS net_s,
                sum(s.tc) as tc,
                unix_timestamp() as unixtimetag
from (

select
      storeId,
      m.orderId,
      DATE(m.addTime) as date,
      ceil(TIME_TO_SEC(m.addTime) / %(interval)d / 60) as time_group,
      case when  orderSource = 'st' and needDelivery = 0 and isTakeOut = 0  then 'st'
           when  orderSource = 'st' and needDelivery = 0 and isTakeOut = 1 then 'wd'
           when orderSource = 'gfs' and needDelivery = 1 and isTakeOut = 0 then 'gfs'
           else null
      end as plat,
      ifnull(((totalAmount-freight)/100 - ifnull(shou,0) - ifnull(packing_charges,0) - ifnull(meals_charges,0)),0.00) as  sale_s,
      ifnull(((totalAmount-freight)/100 - ifnull(jin,0) - ifnull(packing_charges,0) - ifnull(meals_charges,0)),0.00) as  net_s,
      (if(gift=(totalAmount/100-freight/100) or diet=(totalAmount/100-freight/100),0,1))  as tc

      from
    (
      select
        orderId,addTime,storeId,totalAmount,freight,payAmount,
        payType,orderSource,needDelivery,isTakeout,orderType,
        0 other_tc,0 other_s
      from oc.order_master m
      where (m.addTime) between "%(start_day)s" and "%(end_day)s"
      and storeId in (%(storeid)s)
      and orderstatus = 4 and paystatus = 1 and orderType in (0)
      union all
      (
        select
          orderId,addTime,storeId,
          0 totalAmount,
          0 freight,
          payAmount,
          payType,orderSource,needDelivery,isTakeout,orderType,
          0 other_tc,0 other_s
        from oc.order_master m
        where (m.addTime) between "%(start_day)s" and "%(end_day)s"
        and storeId in (%(storeid)s)
        and orderstatus = 4 and paystatus = 1 and orderType in (101,102,103,104)
      )
      union all
      (
       select
         m.orderId,
         m.addTime,
         m.storeId,
         m2.payAmount totalAmount,
         m2.freight,
         m2.payAmount,
         '' payType,
         m.orderSource,
         m.needDelivery,
         m.isTakeout,
         m.orderType,
         1 other_tc,
         m2.payAmount other_s
       from oc.order_master m
       left join datacache.sales_voucher_list vl1 on m.orderId = vl1.orderId
       left join
       (
         select *
         from datacache.sales_voucher_list
         where orderType in (101,102,103,104)
       )vl2 on vl1.voucher = vl2.voucher
       left join
       (
         select
           orderId,
           payAmount,
           freight
         from oc.order_master
         where storeId in (@storeid)
         and orderStatus = 4 and paystatus = 1 and orderType in (101,102,103,104)
       )m2 on m2.orderId = vl2.orderId
       where (m.addTime) between "%(start_day)s" and "%(end_day)s"
       and m.storeId in (%(storeid)s)
       and m.orderstatus = 4 and m.paystatus = 1 and m.orderType in (105,106)
      )
    )m
    left join(
      select
        d.orderId,
        sum(if(discountId in (59,73,77,91),d.discountAmount,0)/100) as gift,
        sum(if(discountId in (75),d.discountAmount,0)/100) as diet,
        sum(if(discountId in (59,73,75,77,91,315,433,551,565,889,891,893,895,899,901,903,905,907,909,911,913,915,917,919,921,923,925,927,929,931,933,935,937,939,941,943,945,947,949,951,953,955,957,959,961,963,965,967,995,997,999,1301,1303,1305,1307,1309,1313,1315,1317,1321,1323,1339,1341,897,1311,1319,1351,1353,1355,1364,1391,1392,1393,1394,1395,1396,1397,1398,1399,1400,1402,1443,1444,1445,1446,1447,1448,1449,1450,1451,1452,1453,1460,1461,1462,1463,1464,1465,1466,1467,1468,1469,1470,1471,1472,1473),d.discountAmount,0)/100) as shou,
        sum(if(discountId in (1201,1202,1415,1428,37,38,40,42,0,307,10001,309,311,971,973,975,977,979,981,983,985,1111,1113,1325,1327,1329,1331,1333,1335,1337,1343,1345,1347,1349,35,44,45,46,47,59,81,83,1474,1475,1476,1477,1478,1479,1480,1481,1482,1483,1484,1485,1486,91,71,1414,73,75,77,315,433,551,565,889,891,893,895,899,901,903,905,907,909,911,913,915,917,919,921,923,925,927,929,931,933,935,937,939,941,943,945,947,949,951,953,955,957,959,961,963,965,967,995,997,999,1301,1303,1305,1307,1309,1313,1315,1317,1321,1323,1339,1341,897,1311,1319,1351,1353,1355,1364,1391,1392,1393,1394,1395,1396,1397,1398,1399,1400,1402,1443,1444,1445,1446,1447,1448,1449,1450,1451,1452,1453,1460,1461,1462,1463,1464,1465,1466,1467,1468,1469,1470,1471,1472,1473),d.discountAmount,0)/100) as jin
      from oc.order_discount d
      left join oc.order_master m on m.orderId = d.orderId
      where (m.addTime) between "%(start_day)s" and "%(end_day)s"
      and m.storeId in (%(storeid)s)
      group by d.orderId
    )d on m.orderId = d.orderId
    left join
    (
      select
        m.orderId,
        sum(if(basic_category_tid in ('3533'),totalPrice/100,0)) packing_charges,
        sum(if(basic_category_tid in ('4711'),totalPrice/100,0)) meals_charges
      from oc.order_detail d
      left join oc.order_master m on m.orderId = d.orderId
      right join
      (
        select sku,basic_category_tid
        from datacache.de_product
        where basic_category_tid in ('3533','4711')
      )procate on procate.sku = d.productId
      where (m.addTime) between "%(start_day)s" and "%(end_day)s"
      and m.storeId in (%(storeid)s)
      group by d.orderId
    )det on m.orderId = det.orderId
  ) as s
  left join
  (
  select
                    time.start_time, time.end_time, time.timeframe
                FROM
                    (select @S_ACCUM:=0) S_ACCUM
                JOIN (select @TC_ACCUM:=0) TC_ACCUM
                JOIN (select
                    SEC_TO_TIME((seq.timeframe - 1) * %(interval)d * 60) AS start_time,
                        SEC_TO_TIME((seq.timeframe) * %(interval)d * 60) AS end_time,
                        seq.timeframe
                FROM
                    (select
                    SeqValue AS timeframe
                FROM
                    (select
                    (TWO_1.SeqValue + TWO_2.SeqValue + TWO_4.SeqValue + TWO_8.SeqValue + TWO_16.SeqValue + TWO_32.SeqValue+TWO_64.SeqValue) SeqValue
                FROM
                    (select 0 SeqValue UNION ALL select 1 SeqValue) TWO_1
                      CROSS JOIN (select 0 SeqValue UNION ALL select 2 SeqValue) TWO_2
                     CROSS JOIN (select 0 SeqValue UNION ALL select 4 SeqValue) TWO_4
                     CROSS JOIN (select 0 SeqValue UNION ALL select 8 SeqValue) TWO_8
                      CROSS JOIN (select 0 SeqValue UNION ALL select 16 SeqValue) TWO_16
                     CROSS JOIN (select 0 SeqValue UNION ALL select 32 SeqValue) TWO_32
					CROSS JOIN (select 0 SeqValue UNION ALL select 64 SeqValue) TWO_64
                     ) AS SeqValue
                  WHERE
                    SeqValue <= (1440 / %(interval)d)
                        AND SeqValue != 0) AS seq
                ORDER BY start_time , end_time ASC) AS time
                ) as times on s.time_group=times.timeframe
                  GROUP BY date , storeId , plat,start_time )a;
"""% {"start_day": start_day ,"end_day": end_day, 'storeid': storeid,'interval': interval}
    try:
        # print sql
        mysql.ExecNonQuery(sql)
        ID = get_last_id()
        sendcontent = "the last record is %d" ID
        logwrite (True,sendcontent)
    except Exception ,e:
         senderr = str(e)
         sendstatus = False
         logwrite(sendstatus,senderr)



if __name__ == "__main__":
    if len(sys.argv) > 1:
        start_day = sys.argv[1]
        end_day = sys.argv[2]
        interval = sys.argv[3]
        main(start_day,end_day,interval)
    else:
        start_day = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        interval = 15
        end_day = datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
        main(start_day,end_day,interval)
