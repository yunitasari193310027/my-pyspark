from pyspark.sql import SparkSession
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce
from datetime import datetime, timedelta
import pyspark.sql.functions as F 
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

hv = HiveContext(spark)
#hv.sql('CREATE TABLE IF NOT EXISTS base.prep_mm(mm_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,vol string,trx string,dur string,load_ts string,load_user string,job_id string) STORED AS PARQUET;')
#spark.read.parquet('file:///home/hdoop/datayunita/dom_mm').write.mode('overwrite').saveAsTable("base.prep_mm")
spark.sql("CREATE DATABASE IF NOT EXISTS output")

start_date = datetime.now() + timedelta(hours=7)
run_date = datetime.now().strftime('%Y%m%d%H%M%S')
# =============================> Read data 
dfPAYJ1 = hv.table('output.sa_payj1')
df_MTD = hv.table('base.prep_mtd')\
            .toDF('trx_date','subs_id_mtd','lacci_id_mtd','msisdn','cust_type_desc','cust_subtype_desc','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','vol','trx','dur','load_ts','load_user','job_id')\
            .select('subs_id_mtd','lacci_id_mtd').dropDuplicates(['subs_id_mtd'])
df_MM = hv.table('base.prep_mm')\
            .toDF('trx_date','subs_id_mm','lacci_id_mm','msisdn','cust_type_desc','cust_subtype_desc','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','vol','trx','dur','load_ts','load_user','job_id')\
            .select('subs_id_mm','lacci_id_mm').dropDuplicates(['subs_id_mm'])
df_LACCIMA = hv.table('base.laccima')\
                .select('lacci_id','eci','cgi_post','area_sales','lac','ci','node','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')\
                .toDF('lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')
df_SAproduct = hv.table('base.sa_product')\
                .withColumn('rkey', 
                    F.when((F.col('mapping_key_type').isNull()) | (F.col('mapping_key_type') == ""), F.col('rkey'))
                        .otherwise(F.col('mapping_key_type')))\
                .select('rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')

# =============================> Proses
PAYJ_Part2 = dfPAYJ1.join(df_MTD,col('subs_id') == col('subs_id_mtd'), how = 'left')\
                    .withColumn("trx_lacci", when(dfJOIN_MTD.trx_lacci == "|","").otherwise(dfJOIN_MTD.trx_lacci))\
                    .withColumn('trx_lacci', 
                        F.when((F.col('trx_lacci').isNull()) & (F.col('subs_id') == F.col('subs_id_mtd')), F.col('lacci_id_mtd'))
                            .otherwise(F.col('trx_lacci')))\
                    .join(df_MM,col('subs_id') == col('subs_id_mm'), how = 'left')\
                    .withColumn('trx_lacci', 
                        F.when((F.col('trx_lacci').isNull()) & (F.col('subs_id') == F.col('subs_id_mm')), F.col('lacci_id_mm'))
                            .otherwise(F.col('trx_lacci')))\
                    .withColumn('find_flag', 
                        F.when((F.col('subs_id') == F.col('subs_id_mtd')) & (F.col('lacci_id_mtd') == F.col('trx_lacci')), '1#')
                        .when((F.col('subs_id') == F.col('subs_id_mm')) & (F.col('lacci_id_mm') == F.col('trx_lacci')), '2#')
                            .otherwise("0"))\
                    .fillna("-99",["trx_lacci"])\
                    .join(df_LACCIMA,col('trx_lacci') == col('lacci_id_laccima'), how = 'left')\
                    .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')\
                    .withColumn('Flag',when(col('trx_lacci') == col('lacci_id_laccima'),'SAME').otherwise('DIFF'))

# SPLIT FILTER
df_goodLACCI = PAYJ_Part2.filter(col('Flag') == 'SAME')\
                    .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag') 

df_rejectLACCI = PAYJ_Part2.filter(col('Flag') == 'DIFF')\
                    .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','find_flag')\
                    .join(df_LACCIMA.dropDuplicates(['eci']),col('trx_lacci') == col('eci'), how = 'left')\
                    .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')\
                    .withColumn('Flag',when(col('trx_lacci') == col('eci'),'SAME').otherwise('DIFF'))
# SPLIT FILTER
df_goodECI = df_rejectLACCI.filter(col('Flag') == 'SAME')\
                .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')
df_rejectECI = df_rejectLACCI.filter(col('Flag') == 'DIFF')\
                .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','find_flag')\
                .join(df_LACCIMA.dropDuplicates(['cgi_post']),col('trx_lacci') == col('cgi_post'), how = 'left')\
                .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# MERGE HASIL
df = [ df_goodLACCI,df_goodECI,df_rejectECI ]
df_statusREJECT = reduce(DataFrame.unionAll, df)\
                .withColumn('flag', 
                    F.when((F.col('trx_lacci') == F.col('lacci_id_laccima')) | (F.col('trx_lacci') == F.col('eci')) | (F.col('trx_lacci') == F.col('cgi_post')), "y")
                        .otherwise("x"))\
                .withColumn('lacci_id_flag', 
                    F.when((F.col('trx_lacci') != "-99") & (F.col('flag') == "y") & (F.col('area_name') != "UNKNOWN") & (F.col('find_flag') == (0)), (concat(lit('0#'),col('lacci_id_laccima'))))
                    .when((F.col('trx_lacci') != "-99") & (F.col('flag') == "y") & (F.col('area_name') != "UNKNOWN") & (F.col('find_flag').isNotNull()), (concat(col('find_flag'),col('trx_lacci'))))
                        .otherwise("-1#-99 "))\
                .withColumn('lacci_closing_flag', split(col('lacci_id_flag'),'#').getItem(0))\
                .withColumn('lacci_id', split(col('lacci_id_flag'),'#').getItem(1))\
                .fillna("NQ",["lac"]).fillna("NQ",["ci"]).fillna("NQ",["node_type"]).fillna("NQ",["area_name"]).fillna("NQ",["region_sales"]).fillna("NQ",["branch"]).fillna("NQ",["subbranch"]).fillna("NQ",["cluster_sales"]).fillna("NQ",["provinsi"]).fillna("NQ",["kabupaten"]).fillna("NQ",["kecamatan"]).fillna("NQ",["kelurahan"])\
                .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','lacci_id_flag','lacci_id','lacci_closing_flag')\
                .join(df_SAproduct,col('offer_id ') == col('rkey'), how = 'left')\
                .select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_flag','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','l4_name','l3_name','l2_name','l1_name','retry_count')\
                .fillna("NQ",["l4_name"]).fillna("NQ",["l3_name"]).fillna("NQ",["l2_name"]).fillna("NQ",["l1_name"])\
                .withColumn("file_id", lit("")).withColumn("load_ts", lit(cur_time)).withColumn("load_user", lit('solusi247'))\
                .select('activation_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count')\
                .withColumn('status_reject', F.when((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan').isNull())\
                                                | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "NQ"))\
                                                | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "-99")), 'YES').otherwise('NO'))

# SPLIT FILTER
df_hasilGOOD = df_statusREJECT.filter(col('status_reject') == 'NO')\
                    .select('activation_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10')\
                    .write.mode('overwrite').saveAsTable("output.sa_payj_good")
df_hasilREJECT = df_statusREJECT.filter(col('status_reject') == 'YES').write.mode('overwrite').saveAsTable("output.sa_payj_reject") 

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)

# CREATE FILELOG
app = "payj2_hive"
nama_file = "/home/hdoop/BelajarPyspark/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.writelines('\nINPUT_COUNT={}'.format(dfPAYJ1.count()))
f.writelines('\nOUTPUT1_COUNT={}'.format(df_hasilGOOD.count()))
f.writelines('\nOUTPUT1=2_COUNT={}'.format(df_hasilREJECT.count()))
f.close()

