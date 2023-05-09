from pyspark.sql import SparkSession
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from functools import reduce
from datetime import datetime, timedelta
from pyspark.sql.functions import monotonically_increasing_id,row_number
from pyspark.sql.window import Window 
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
start_date = datetime.now() + timedelta(hours=7)
run_date = datetime.now().strftime('%Y%m%d%H%M%S')
# ===============================>READ DATA
#hv.sql('CREATE TABLE IF NOT EXISTS base.source_rcg(timestamp string,msisdn string,account string,recharge_channel string,expiration_date string,serial_number string,delta_balance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,provider_id string,source_ip string,user_id string,result_code string,bank_code string,a_number_location string,balance_before string,adjustment_reason string,case_id string,crmuser_id string,old_expiration_date string,split_code string,recharge_amount string,future_string_2 string,future_string_3 string,future_string_1 string,indicator_4g string)row format delimited fields terminated by "|"')
#hv.sql("load data local inpath 'file:///home/hdoop/datayunita/SAUsageOCS/TC_RCG' overwrite into table base.source_rcg;")

# READ DATA SA_USAGE_OCS_RCG
#df_TC_RCG = spark.read.csv("file:///home/hdoop/datayunita/SAUsageOCS/TC_RCG", sep ='|', header = False)
#df_TC_RCG = df_TC_RCG.toDF('timestamp','msisdn','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_2','future_string_3','future_string_1','indicator_4g')
df_TC_RCG = hv.table('base.source_rcg')

# Read data SUB_DIM
df_SUB_DIM = hv.table('base.subs_dim')
df_SUB_DIM = df_SUB_DIM.select('msisdn','subs_id','cust_type_desc','cust_subtype_desc')
df_SUB_DIM = df_SUB_DIM.toDF('msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc')

# READ MTD
df_MTD = hv.table('base.chg_prep_mtd')
df_MTD = df_MTD.toDF('trx_date','subs_id_mtd','lacci_id','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','trx','dur','load_ts','load_user','event_date','job_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')
df_MTD = df_MTD.select('subs_id_mtd','msisdn','lacci_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')

# READ MM
df_MM = hv.table('base.chg_prep_mm')
df_MM = df_MM.toDF('trx_date','subs_id_mm','lacci_id_mm','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','trx','dur','load_ts','load_user','event_date','job_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')
df_MM = df_MM.select('subs_id_mm','msisdn','lacci_id_mm','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')

# READ DATA LACCIMA
df_LACCIMA = hv.table('base.chg_laccima')
df_LACCIMA = df_LACCIMA.toDF('lac','ci','cell_name','vendor','site_id','ne_id','site_name','msc_name','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','file_date','file_id','load_ts','load_user','event_date')
df_LACCIMA = df_LACCIMA.select('lac','ci','cell_name','vendor','site_id','ne_id','msc_name','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','file_date','file_id')
df_LACCIMA = df_LACCIMA.fillna("UNKNOWN",['area_sales'])

# READ SA_PRODLINE
df_SAProdline = hv.table('base.chg_SAProdline')
df_SAProdline = df_SAProdline.toDF('product_name','price','l1','l2','l3','l4','note','source')\
                             .withColumn('source', upper(col('source')))\
                             .withColumn('trim_note', trim(col('note')))
                             
df_SAProdline = df_SAProdline.filter(col('source')  == 'PAYD')

# CHECK bad data
df_TC_RCG = df_TC_RCG.withColumn('Flag',when(trim(col('timestamp')).isNull() & trim(col('delta_balance')).isNull() & trim(col('msisdn')).isNull(),'BAD').otherwise('GOOD'))
df_TC_RCGgood  = df_TC_RCG.filter(col('Flag')  == 'GOOD')
df_TC_RCGbad = df_TC_RCG.filter(col('Flag') == 'BAD')

# ADD COLUMN with lit
df_TC_RCG = df_TC_RCG.withColumn('filename', lit("RCG_JKT_Pre_20230222015959_00006238_51.1"))\
                     .withColumn('rec_id',row_number().over(Window.orderBy(monotonically_increasing_id())))\
                     .withColumn('rundate', lit("20230222015959"))\
                     .withColumn('load_user', lit("solusi247"))\
                     .withColumn('map_expiration_date', to_timestamp(df_TC_RCG.timestamp, "yyyyMMddHHmmss"))\
                     .withColumn('trim_a',trim(col('a_number_location')))\
                     .withColumn('substr11_7_a_number_location', substring('trim_a',11,7))\
                     .withColumn('substr18_3_a_number_location', substring('trim_a',18,3))\
                     .withColumn('substr6_5_a_number_location', substring('trim_a',6,5))\
                     .withColumn('substr11_5_a_number_location', substring('trim_a',11,5))
# CONCAT
df_RCG = df_TC_RCG.withColumn('c1',F.concat(F.col('substr11_7_a_number_location'),F.lit('|'), F.col('substr18_3_a_number_location')))\
  .withColumn('c2',F.concat(F.col('substr6_5_a_number_location'),F.lit('|'), F.col('substr11_5_a_number_location')))

df_RCG = df_RCG.withColumn('t1', F.regexp_replace('c1', r'^0+', ''))\
               .withColumn('t2', F.regexp_replace('c2', r'^0+', ''))\
               .withColumn('credit_indicator', upper(col('credit_indicator')))\
               .withColumn('recharge_method', upper(col('recharge_method')))\
               .fillna("00000000000000",['old_expiration_date'])\
               .fillna("0",['recharge_amount'])\
               .withColumn('timestamp_r', to_timestamp(df_RCG.timestamp, "yyyyMMddHHmmss"))\
               .withColumn('trx_hour', substring('timestamp_r', 12,2))\
               .withColumn('trx_date', substring('timestamp_r', 1,10))\
               .fillna("U",['case_id'])\
               .fillna("U",['crmuser_id'])\
               .withColumn('brand', lit("byU"))\
               .withColumn('site_name', lit("JKT"))\
               .withColumn('file_id', lit(""))\
               .withColumn('status_data', lit(""))\
               .withColumn('retry_count', lit("0"))\
               .withColumn('status', lit("NODUP"))
               
# SWITCH CASE
df_RCG = df_RCG.withColumn('expiration_date',
                    F.when(trim(col('timestamp')).isNull() | col('expiration_date').isNull(),"2999-12-31 23:59:59")
                     .otherwise(col('map_expiration_date')))\
                .withColumn('recharge_amount',
                    F.when((col('recharge_method') == "BTSOURCE") & (col('credit_indicator') == "D"),(col('recharge_amount')*-1))
                     .otherwise(col('recharge_amount')))\
                .withColumn('trx_lacci', 
                      F.when((trim(col('indicator_4g')) == "129") | (trim(col('indicator_4g')) == "130"), F.col('t1'))
                       .when((trim(col('indicator_4g')) == "128") | (trim(col('indicator_4g')) == "131"), F.col('a_number_location'))
                          .otherwise(F.col('t2')))

df_RCG = df_RCG.select('timestamp_r','trx_hour','trx_date','msisdn','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','rundate','load_user','site_name','timestamp_r','filename','rec_id','retry_count','rundate','status_data','status','trx_lacci')              
df_RCG = df_RCG.toDF('timestamp_r','trx_hour','trx_date','msisdn','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

# CEK
#df_RCG.select('trx_lacci').distinct().show()


# JOIN RCG & SUB_DIM
dfRCG_SUB = df_RCG.join(df_SUB_DIM,df_RCG.msisdn ==  df_SUB_DIM.msisdn_sub, how = 'left')

# SWITHCASE 
dfRCG_SUB = dfRCG_SUB.withColumn('subs_id', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('subs_id'))
                                  .otherwise("-99"))\
                      .withColumn('cust_type_desc', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('cust_type_desc'))
                                  .otherwise("NQ"))\
                      .withColumn('cust_subtype_desc', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('cust_subtype_desc'))
                                  .otherwise("NQ"))

dfRCG_SUB = dfRCG_SUB.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')
dfRCG_SUB = dfRCG_SUB.toDF('timestamp_r','trx_hour','trx_date','msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')



# Using multiple columns on join expression
dfJOIN_MTD = dfRCG_SUB.join(df_MTD, (dfRCG_SUB["subs_id"] == df_MTD["subs_id_mtd"]) &
   ( dfRCG_SUB["msisdn_sub"] == df_MTD["msisdn"]),"left")

# SWITHCASE 
dfJOIN_MTD = dfJOIN_MTD.withColumn('mtd_lacci_id', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_id'))
                                  .otherwise("-99"))\
                      .withColumn('mtd_area_name', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_area_name'))
                                  .otherwise("UNKNOWN"))\
                      .withColumn('mtd_region_network', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_region_network'))
                                  .otherwise(""))\
                      .withColumn('mtd_cluster_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_cluster_sales'))
                                  .otherwise(""))\
                      .withColumn('mtd_lac', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_lac'))
                                  .otherwise(""))\
                      .withColumn('mtd_ci', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_ci'))
                                  .otherwise(""))\
                      .withColumn('mtd_node', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_node'))
                                  .otherwise(""))\
                      .withColumn('mtd_region_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_region_sales'))
                                  .otherwise(""))\
                      .withColumn('mtd_branch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_branch'))
                                  .otherwise(""))\
                      .withColumn('mtd_subbranch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_subbranch'))
                                  .otherwise(""))\
                      .withColumn('mtd_provinsi', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_provinsi'))
                                  .otherwise("-99"))\
                      .withColumn('mtd_kabupaten', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_kabupaten'))
                                  .otherwise(""))\
                      .withColumn('mtd_kecamatan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_kecamatan'))
                                  .otherwise(""))\
                      .withColumn('mtd_kelurahan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_kelurahan'))
                                  .otherwise(""))\
                      .withColumn('mtd_flag', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), "1")
                                  .otherwise("0"))

dfJOIN_MTD = dfJOIN_MTD.select('timestamp_r','trx_hour','trx_date','msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag')



# Using multiple columns on join expression
dfJOIN_MM = dfJOIN_MTD.join(df_MM, (dfJOIN_MTD["subs_id"] == df_MM["subs_id_mm"]) &
   ( dfJOIN_MTD["msisdn_sub"] == df_MM["msisdn"]),"left")


# CEK
#print("JOIN SUB DIM=",dfRCG_SUB.count(), " JOIN MTD=", dfJOIN_MTD.count(),"JOIN MM=", dfJOIN_MM.count())

dfJOIN_MM = dfJOIN_MM.withColumn('mm_lacci_id', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_id_mm'))
                                  .otherwise("-99"))\
                      .withColumn('mm_area_name', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_area_name'))
                                  .otherwise("UNKNOWN"))\
                      .withColumn('mm_region_network', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_region_network'))
                                  .otherwise(""))\
                      .withColumn('mm_cluster_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_cluster_sales'))
                                  .otherwise(""))\
                      .withColumn('mm_lac', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_lac'))
                                  .otherwise(""))\
                      .withColumn('mm_ci', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_ci'))
                                  .otherwise(""))\
                      .withColumn('mm_node', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_node'))
                                  .otherwise(""))\
                      .withColumn('mm_region_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_region_sales'))
                                  .otherwise(""))\
                      .withColumn('mm_branch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_branch'))
                                  .otherwise(""))\
                      .withColumn('mm_subbranch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_subbranch'))
                                  .otherwise(""))\
                      .withColumn('mm_provinsi', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_provinsi'))
                                  .otherwise("-99"))\
                      .withColumn('mm_kabupaten', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_kabupaten'))
                                  .otherwise(""))\
                      .withColumn('mm_kecamatan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_kecamatan'))
                                  .otherwise(""))\
                      .withColumn('mm_kelurahan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_kelurahan'))
                                  .otherwise(""))\
                      .withColumn('mm_flag', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), "1")
                                  .otherwise("0"))\
                      .withColumn('counter',row_number().over(Window.orderBy(monotonically_increasing_id())))

dfJOIN_MM = dfJOIN_MM.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag','mm_lacci_id','mm_area_name','mm_region_network','mm_cluster_sales','mm_lac','mm_ci','mm_node','mm_region_sales','mm_branch','mm_subbranch','mm_provinsi','mm_kabupaten','mm_kecamatan','mm_kelurahan','mm_flag')


# SPLIT FILTER 
dfJOIN_MM = dfJOIN_MM.withColumn('Flag',
                                F.when((col('indicator_4g') == "129") | (col('indicator_4g') == "130"),'r1')
                                 .when((col('indicator_4g') == "128") | (col('indicator_4g') == "131"),'r2')
                                 .otherwise('r3'))

# CEK
#dfJOIN_MM.select('trx_lacci').distinct().show()

USAGE_OCS_RCG_01 = dfJOIN_MM.filter(col('Flag')  == 'r1')
USAGE_OCS_RCG_02 = dfJOIN_MM.filter(col('Flag')  == 'r2')
USAGE_OCS_RCG_03 = dfJOIN_MM.filter(col('Flag')  == 'r3')

# CEK
#USAGE_OCS_RCG_01.show()
#USAGE_OCS_RCG_02.show()
#USAGE_OCS_RCG_03.show()


# CEK
#df_LACCIMA.select('eci').distinct().show()

# JOIN ECI
dfJOINeci = USAGE_OCS_RCG_01.join(df_LACCIMA,USAGE_OCS_RCG_01.trx_lacci ==  df_LACCIMA.eci, how = 'left')

dfJOINeci = dfJOINeci.withColumn('lacci_id', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('lacci_id'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lacci_id'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lacci_id'))
                                  .otherwise("-99"))\
                    .withColumn('lacci_closing_flag', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), "0")
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), "1")
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), "2")
                                  .otherwise("-1"))\
                    .withColumn('lac', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('lac'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lac'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lac'))
                                  .otherwise("NQ"))\
                    .withColumn('ci', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('ci'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_ci'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_ci'))
                                  .otherwise("NQ"))\
                    .withColumn('node_type', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('node'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_node'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_node'))
                                  .otherwise("NQ"))\
                    .withColumn('area_sales', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('area_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_area_name'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_area_name'))
                                  .otherwise("NQ"))\
                    .withColumn('region_sales', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('region_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_region_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_region_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('branch', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('branch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_branch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_branch'))
                                  .otherwise("NQ"))\
                    .withColumn('subbranch', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('subbranch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_subbranch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_subbranch'))
                                  .otherwise("NQ"))\
                    .withColumn('cluster_sales', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('cluster_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_cluster_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_cluster_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('provinsi', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('provinsi'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_provinsi'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_provinsi'))
                                  .otherwise("NQ"))\
                    .withColumn('kabupaten', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('kabupaten'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kabupaten'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kabupaten'))
                                  .otherwise("NQ"))\
                    .withColumn('kecamatan', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('kecamatan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kecamatan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kecamatan'))
                                  .otherwise("NQ"))\
                    .withColumn('kelurahan', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('kelurahan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kelurahan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kelurahan'))
                                  .otherwise("NQ"))


# JOIN CGI_POST
dfJOINcgi = USAGE_OCS_RCG_02.join(df_LACCIMA,USAGE_OCS_RCG_02.trx_lacci ==  df_LACCIMA.cgi_post, how = 'left')
dfJOINcgi = dfJOINcgi.withColumn('cgi_post', 
                              F.when((F.col('trx_lacci') == F.col('cgi_post')) & (F.col('area_sales') != "UNKNOWN"), F.col('cgi_post'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lacci_id'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lacci_id'))
                                  .otherwise("-99"))\
                    .withColumn('lacci_closing_flag', 
                              F.when((F.col('trx_lacci') == F.col('cgi_post')) & (F.col('area_sales') != "UNKNOWN"), "0")
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), "1")
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), "2")
                                  .otherwise("-1"))\
                    .withColumn('lac', 
                              F.when((F.col('trx_lacci') == F.col('cgi_post')) & (F.col('area_sales') != "UNKNOWN"), F.col('lac'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lac'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lac'))
                                  .otherwise("NQ"))\
                    .withColumn('ci', 
                              F.when((F.col('trx_lacci') == F.col('cgi_post')) & (F.col('area_sales') != "UNKNOWN"), F.col('ci'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_ci'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_ci'))
                                  .otherwise("NQ"))\
                    .withColumn('node_type', 
                              F.when((F.col('trx_lacci') == F.col('cgi_post')) & (F.col('area_sales') != "UNKNOWN"), F.col('node'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_node'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_node'))
                                  .otherwise("NQ"))\
                    .withColumn('area_sales', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('area_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_area_name'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_area_name'))
                                  .otherwise("NQ"))\
                    .withColumn('region_sales', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('region_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_region_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_region_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('branch', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('branch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_branch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_branch'))
                                  .otherwise("NQ"))\
                    .withColumn('subbranch', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('subbranch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_subbranch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_subbranch'))
                                  .otherwise("NQ"))\
                    .withColumn('cluster_sales', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('cluster_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_cluster_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_cluster_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('provinsi', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('provinsi'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_provinsi'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_provinsi'))
                                  .otherwise("NQ"))\
                    .withColumn('kabupaten', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('kabupaten'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kabupaten'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kabupaten'))
                                  .otherwise("NQ"))\
                    .withColumn('kecamatan', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('kecamatan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kecamatan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kecamatan'))
                                  .otherwise("NQ"))\
                    .withColumn('kelurahan', 
                              F.when(F.col('trx_lacci') == F.col('cgi_post'), F.col('kelurahan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kelurahan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kelurahan'))
                                  .otherwise("NQ"))


# JOIN LACCI
dfJOINlacci = USAGE_OCS_RCG_03.join(df_LACCIMA,USAGE_OCS_RCG_03.trx_lacci ==  df_LACCIMA.lacci_id, how = 'left')
dfJOINlacci = dfJOINlacci.withColumn('lacci_id', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('lacci_id'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lacci_id'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lacci_id'))
                                  .otherwise("-99"))\
                    .withColumn('lacci_closing_flag', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), "0")
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), "1")
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), "2")
                                  .otherwise("-1"))\
                    .withColumn('lac', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('lac'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lac'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lac'))
                                  .otherwise("NQ"))\
                    .withColumn('ci', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('ci'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_ci'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_ci'))
                                  .otherwise("NQ"))\
                    .withColumn('node_type', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('node'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_node'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_node'))
                                  .otherwise("NQ"))\
                    .withColumn('area_sales', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('area_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_area_name'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_area_name'))
                                  .otherwise("NQ"))\
                    .withColumn('region_sales', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('region_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_region_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_region_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('branch', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('branch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_branch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_branch'))
                                  .otherwise("NQ"))\
                    .withColumn('subbranch', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('subbranch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_subbranch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_subbranch'))
                                  .otherwise("NQ"))\
                    .withColumn('cluster_sales', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('cluster_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_cluster_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_cluster_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('provinsi', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('provinsi'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_provinsi'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_provinsi'))
                                  .otherwise("NQ"))\
                    .withColumn('kabupaten', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('kabupaten'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kabupaten'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kabupaten'))
                                  .otherwise("NQ"))\
                    .withColumn('kecamatan', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('kecamatan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kecamatan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kecamatan'))
                                  .otherwise("NQ"))\
                    .withColumn('kelurahan', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('kelurahan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kelurahan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kelurahan'))
                                  .otherwise("NQ"))


dfJOINeci = dfJOINeci.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status')
dfJOINcgi = dfJOINcgi.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status')
dfJOINlacci = dfJOINlacci.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status')

# CEK 
#print("JOIN ECI=", dfJOINeci.count(), "JOIN CGI=", dfJOINcgi.count(), "JOIN LACCI=", dfJOINlacci.count())

# MERGE 
df = [ dfJOINeci,dfJOINcgi,dfJOINlacci ]
dfMerge = reduce(DataFrame.unionAll, df)

# SPLIT FILTER
df_RCG_Part1 = dfMerge.withColumn('flag_status', 
                              F.when(F.col('status') == "DUP", "DUP01")
                                  .otherwise("NODUP"))

df_DUP01 = df_RCG_Part1.filter(col('flag_status')  == 'DUP01')
df_RCG_Part1 = df_RCG_Part1.filter(col('flag_status')  == 'NODUP')

# RANKING
window = Window.partitionBy('filename','rec_id').orderBy('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lac','lacci_closing_flag','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status')
df_RCG_Part1 = df_RCG_Part1.withColumn("ranking", dense_rank().over(window))

# SWITCH CASE
df_RCG_Part1 = df_RCG_Part1.withColumn('status', 
                              F.when(F.col('ranking') > 1, "DUP")
                                  .otherwise(F.col('status')))

# SPLIT FILTER
df_DUP03 = df_RCG_Part1.filter(col('ranking')  > 1)
df_RCG_Part1 = df_RCG_Part1.filter(col('ranking')  == '1')

# RANKING
window = Window.partitionBy('timestamp_r','msisdn','account','delta_balance').orderBy('event_date','filename','rec_id')
df_RCG_Part1 = df_RCG_Part1.withColumn("r", dense_rank().over(window))

df_RCG_Part1 = df_RCG_Part1.withColumn('status', 
                              F.when(F.col('r') > 1, "DUP")
                                  .otherwise(F.col('status')))
# SPLIT FILTER
df_DUP02 = df_RCG_Part1.filter(col('r')  > 1)
df_RCG_Part1 = df_RCG_Part1.filter(col('r')  == '1')

df_RCG_Part1 = df_RCG_Part1.withColumn('status_data',
                            F.when((col('subs_id').isNull()) | (trim(col('subs_id')) == "") | (col('subs_id') == "-99")\
                                    | (col('cust_type_desc').isNull()) | (trim(col('cust_type_desc')) == "") | (col('cust_type_desc') == "-99")\
                                    | (col('lacci_id').isNull()) | (trim(col('lacci_id')) == "") | (col('lacci_id') == "-99")\
                                    | (col('lacci_closing_flag').isNull()) | (trim(col('lacci_closing_flag')) == "") | (col('lacci_closing_flag') == "-1")\
                                    | (col('lac').isNull()) | (trim(col('lac')) == "") | (col('lac') == "NQ")\
                                    | (col('ci').isNull()) | (trim(col('ci')) == "") | (col('ci') == "NQ")\
                                    | (col('node_type').isNull()) | (trim(col('node_type')) == "") | (col('node_type') == "NQ")\
                                    | (col('area_sales').isNull()) | (trim(col('area_sales')) == "") | (col('area_sales') == "NQ")\
                                    | (col('region_sales').isNull()) | (trim(col('region_sales')) == "") | (col('region_sales') == "NQ")\
                                    | (col('branch').isNull()) | (trim(col('branch')) == "") | (col('branch') == "NQ")\
                                    | (col('subbranch').isNull()) | (trim(col('subbranch')) == "") | (col('subbranch') == "NQ")\
                                    | (col('cluster_sales').isNull()) | (trim(col('cluster_sales')) == "") | (col('cluster_sales') == "NQ")\
                                    | (col('provinsi').isNull()) | (trim(col('provinsi')) == "") | (col('provinsi') == "NQ")\
                                    | (col('kabupaten').isNull()) | (trim(col('kabupaten')) == "") | (col('kabupaten') == "NQ")\
                                    | (col('kecamatan').isNull()) | (trim(col('kecamatan')) == "") | (col('kecamatan') == "NQ")\
                                    | (col('kelurahan').isNull()) | (trim(col('kelurahan')) == "") | (col('kelurahan') == "NQ"),'REJECT')
                                .otherwise('GOOD'))

df_RCG_Part1 = df_RCG_Part1.withColumn('offer_id',
                                       F.when(col('status_data') == "GOOD", col('provider_id'))
                                        .otherwise(""))

# KE PRODLINE
df_RCG_v1 = df_RCG_Part1.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','ranking','offer_id')
df_RCG_v1 = df_RCG_Part1.filter(col('recharge_method')  == 'TPFEE')
df_RCG_v1.show()

df_RCG_v1 = df_RCG_Part1.withColumn('mm_closing_flag', lit("1"))\
                        .withColumn('l1_name', lit("Digital Services"))\
                        .withColumn('l2_name', lit("Transfer Pulsa"))\
                        .withColumn('l3_name', lit("Simpati"))\
                        .withColumn('l4_name', lit("SA_50007"))\
                        .withColumn('subtype', lit("CONSUMER"))\
                        .withColumn('payment_type', lit(""))\
                        .withColumn('offer_name', lit(""))\
                        .withColumn('substr_offer_id_3_5', substring('offer_id',3,5))



# SWITCH CASE
df_SAProdline = df_SAProdline.withColumn('l1', 
                              F.when(F.col('l1') == "UNKNOWN", "Others")
                               .when(F.col('l1') == "Digital Service", "Digital Services")
                                  .otherwise(F.col('l1')))\
                             .withColumn('l2', 
                              F.when(F.col('l2') == "UNKNOWN", "Others")
                               .when(F.col('l2') == "Other VAS Services", "Other VAS Service")
                               .when(F.col('l2') == "SMS Non P to P", "SMS Non P2P")
                                  .otherwise(F.col('l2')))\
                             .withColumn('l3', 
                              F.when((F.col('l1') == "Voice P2P") & (F.col('l2') == "Internal MO Call") & (F.col('l3') == "Local"), "Local (On-Net)")
                               .when((F.col('l3') == "Other VAS Services"), "Other VAS Service")
                               .when((F.col('l3') == "Volume Based"), "Volume based")
                                  .otherwise(F.col('l3')))\
                             .withColumn('l4', 
                              F.when((F.col('l1') == "UNKNOWN") & (F.col('l1') == "Others"), "Others")
                                  .otherwise(F.col('l4')))

# JOIN PRODLINE
df_JOINprod = df_RCG_v1.join(df_SAProdline,df_RCG_v1.substr_offer_id_3_5 ==  df_SAProdline.trim_note, how = 'left')
#df_JOINprod = df_JOINprod.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status','ranking','substr_offer_id_3_5','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_name','subtype','payment_type','offer_id','product_name','price','l1','l2','l3','l4','note','source','trim_note')
df_JOINprod = df_JOINprod.withColumn('lacci_id', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('lacci_id')))\
                             .withColumn('node_type', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('node_type')))\
                             .withColumn('area_sales', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('area_sales')))\
                             .withColumn('region_sales', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('region_sales')))\
                             .withColumn('branch', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('branch')))\
                             .withColumn('subbranch', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('subbranch')))\
                             .withColumn('cluster_sales', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('cluster_sales')))\
                             .withColumn('provinsi', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('provinsi')))\
                             .withColumn('kabupaten', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('kabupaten')))\
                             .withColumn('kecamatan', 
                              F.when(F.col('region_sales') == "UNKNOWN", "UNKNOWN")
                                  .otherwise(F.col('kecamatan')))\
                             .withColumn('kelurahan', 
                              F.when((F.col('region_sales') == "UNKNOWN"), "UNKNOWN")
                                  .otherwise(F.col('kelurahan')))\
                             .withColumn('l1', 
                              F.when((F.col('substr_offer_id_3_5') == F.col('trim_note')), F.col('l1'))
                                  .otherwise("Others"))\
                             .withColumn('l2', 
                              F.when((F.col('substr_offer_id_3_5') == F.col('trim_note')), F.col('l2'))
                                  .otherwise("Others"))\
                             .withColumn('l3', 
                              F.when((F.col('substr_offer_id_3_5') == F.col('trim_note')), F.col('l3'))
                                  .otherwise("Others"))\
                             .withColumn('l4', 
                              F.when((F.col('substr_offer_id_3_5') == F.col('trim_note')), F.col('l4'))
                                  .otherwise("Others"))\
                             .withColumn('source_name', 
                              F.when((F.col('status_data') == "GOOD"), "RCG_GOOD")
                                  .otherwise("RCG_REJECT"))\
                             .withColumn('source', 
                              F.when((F.col('status_data') == "GOOD"), "RCG_GOOD")
                                  .otherwise("RCG_REJECT"))\
                             .withColumn('event_date', 
                              F.when((F.col('status_data') == "GOOD"), F.col('trx_date'))
                                  .otherwise(F.col('event_date')))

df_JOINprod = df_JOINprod.withColumn('mm_closing_flag', lit("1"))\
                         .withColumn('cust_type_desc', lit("B2C"))\
                         .withColumn('cust_subtype_desc', lit("NON HVC"))\
                         .withColumn('service_filter', lit(""))\
                         .withColumn('payment_channel', lit(""))\
                         .withColumn('trx', lit("1"))\
                         .withColumn('dur', lit("0"))\
                         .withColumn('vol', lit("0"))\
                         .withColumn('load_date', lit("20230222"))

df_JOINprod = df_JOINprod.select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','recharge_amount','dur','vol','source_name','load_ts','source','load_ts','event_date','load_date')
df_JOINprod = df_JOINprod.toDF('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','source','job_id','event_date','load_date')

# SPLIT FILTER
MERGE_SA_RCG_GOOD = df_JOINprod.filter(col('source')  == 'RCG_GOOD')
MERGE_SA_RCG_REJECT = df_JOINprod.filter(col('source')  == 'RCG_REJECT')

MERGE_SA_RCG_GOOD = MERGE_SA_RCG_GOOD.groupby(['trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','source_name','load_ts','source','job_id','event_date','load_date']).agg(count('trx').alias("trx"), sum('rev').alias("rev"), sum('dur').alias("dur"), sum('vol').alias("vol"))
MERGE_SA_RCG_GOOD = MERGE_SA_RCG_GOOD.select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','event_date','load_date')

MERGE_SA_RCG_REJECT = MERGE_SA_RCG_REJECT.groupby(['trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','source_name','load_ts','source','job_id','event_date','load_date']).agg(count('trx').alias("trx"), sum('rev').alias("rev"), sum('dur').alias("dur"), sum('vol').alias("vol"))
MERGE_SA_RCG_REJECT = MERGE_SA_RCG_REJECT.select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','event_date','load_date')

# USAGE_SA_RCG_REJECT
df_RCG_v2 = df_RCG_Part1.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','status_data','status')
USAGE_SA_RCG_REJECT = df_RCG_v2.filter(col('status_data')  == 'REJECT')
USAGE_SA_RCG_REJECT = USAGE_SA_RCG_REJECT.withColumn('load_date', lit("20230222"))
USAGE_SA_RCG_REJECT = USAGE_SA_RCG_REJECT.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','load_date','event_date')
USAGE_SA_RCG_REJECT = USAGE_SA_RCG_REJECT.toDF('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','load_date','p_event_date')


# USAGE_SA_RCG_GOOD
USAGE_SA_RCG_GOOD = df_RCG_v2.filter(col('status_data')  == 'GOOD')
USAGE_SA_RCG_GOOD = USAGE_SA_RCG_GOOD.withColumn('load_date', lit("20230222"))\
                                     .withColumn('retry_count', (col('rec_id')+1))
USAGE_SA_RCG_GOOD = USAGE_SA_RCG_GOOD.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','load_date','event_date')
USAGE_SA_RCG_GOOD = USAGE_SA_RCG_GOOD.toDF('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','delta_balance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','provider_id','source_ip','user_id','result_code','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','case_id','crmuser_id','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','file_id','load_ts','load_user','site_name','event_date','filename','rec_id','retry_count','retry_ts','load_date','p_event_date')


# USAGE_SA_RCG_SUM
df_RCG_v3 = df_RCG_Part1.select('event_date','filename','status_data','status','recharge_amount')\
                 .withColumn('job_id', lit("20230223144600"))\
                 .withColumn('group_process', lit("2"))\
                 .withColumn('qty', lit("1"))\
                 .withColumn('filetype', lit("SA_USAGE_OCS_RCG"))\
                 .withColumn('periode', F.regexp_replace('event_date', r'^-^', ''))
df_RCG_v3 = df_RCG_v3.groupby(['periode','filename','job_id','group_process','status_data','status','filetype']).agg(count('qty').alias("qty"), sum('recharge_amount').alias("rev"))
USAGE_SA_RCG_SUM = df_RCG_v3.select('periode','filename','job_id','group_process','status_data','status','qty','rev','filetype')


# WRITE HASIL
'''
df_TC_RCGbad.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_BAD", sep ='~')
df_DUP01.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_DUP01", sep ='~')
df_DUP02.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_DUP02", sep ='~')
df_DUP03.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_DUP03", sep ='~')
USAGE_SA_RCG_GOOD.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_GOOD", sep ='~')
USAGE_SA_RCG_REJECT.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_REJECT", sep ='~')
USAGE_SA_RCG_SUM.write.csv("file:///home/hdoop/BelajarPyspark/hasilUSAGE_SA_RCG_SUM", sep ='~')
MERGE_SA_RCG_GOOD.write.csv("file:///home/hdoop/BelajarPyspark/hasilMERGE_SA_RCG_GOOD", sep ='~')
MERGE_SA_RCG_REJECT.write.csv("file:///home/hdoop/BelajarPyspark/hasilMERGE_SA_RCG_REJECT", sep ='~')
'''
df_TC_RCGbad.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_BAD")
df_DUP01.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_DUP01")
df_DUP02.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_DUP02")
df_DUP03.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_DUP03")
USAGE_SA_RCG_GOOD.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_GOOD")
USAGE_SA_RCG_REJECT.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_REJECT")
USAGE_SA_RCG_SUM.write.mode('overwrite').saveAsTable("output.hasilUSAGE_SA_RCG_SUM")
MERGE_SA_RCG_GOOD.write.mode('overwrite').saveAsTable("output.hasilMERGE_SA_RCG_GOOD")
MERGE_SA_RCG_REJECT.write.mode('overwrite').saveAsTable("output.hasilMERGE_SA_RCG_REJECT")


end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)

# CREATE FILELOG
app = "rcg_hive_nominimalisir"
nama_file = "/home/hdoop/BelajarPyspark/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.close()
