from datetime import datetime 
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.context import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pyspark.sql.functions as F
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

hv = HiveContext(spark)

# ===============================>LOAD DATA
hv.sql('CREATE TABLE IF NOT EXISTS base.source_rcg_hh(timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,delta_balance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,provider_id string,source_ip string,user_id string,result_code string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,case_id string,crmuser_id string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,file_id string,load_ts string,load_user string,site_name string,event_date string,filename string,rec_id string,retry_count string,retry_ts string) STORED AS PARQUET;')
spark.read.parquet('file:///home/hdoop/datayunita/SAUsageOCS/HH/*/*/*').write.mode('overwrite').saveAsTable("base.source_rcg_hh")

start_date = datetime.now() + timedelta(hours=7)
run_date = datetime.now().strftime('%Y%m%d%H%M%S')

# READ DATA SA_USAGE_OCS_RCGm
df_RCG = hv.table("base.source_rcg_hh")\
                .withColumn('trx', lit("1"))\
                .withColumn('event_date', lit("2023-02-22"))\
                .withColumn('load_date', lit("20230222"))\
                .fillna("0",['recharge_amount'])\
                .groupby(['trx_date','trx_hour','subs_id','msisdn','cust_type_desc','cust_subtype_desc','provider_id','lacci_id','lac','ci','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','lacci_closing_flag','credit_indicator','split_code','brand','load_ts','load_user','load_ts','indicator_4g','event_date','load_date']).agg(sum('trx').alias("trx"), sum('recharge_amount').alias("recharge_amount"))\
                .select('trx_date','trx_hour','subs_id','msisdn','cust_type_desc','cust_subtype_desc','provider_id','lacci_id','lac','ci','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','lacci_closing_flag','credit_indicator','split_code','brand','trx','recharge_amount','load_ts','load_user','load_ts','indicator_4g','event_date','load_date')\
                .toDF('trx_date','trx_hour','subs_id','msisdn','cust_type_desc','cust_subtype_desc','offer_id','lacci_id','lac','ci','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','lacci_closing_flag','credit_debit_code','split_code','brand','trx','recharge_amount','load_ts','load_user','job_id','indicator_4g','event_date','load_date')\
                .write.mode('overwrite').saveAsTable("output.sa_usage_rcg_hh")

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)

# CREATE FILELOG
app = "rcg_hh"
nama_file = "/home/hdoop/BelajarPyspark/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.close()

