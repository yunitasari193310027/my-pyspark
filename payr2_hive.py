from pyspark.sql import SparkSession
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from functools import reduce
from datetime import datetime, timedelta 
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
# READ DATA MTD
df_MTD = hv.table('base.prep_mtd')\
            .dropDuplicates(['subs_id'])\
            .select('subs_id','lacci_id').toDF('subs_id_mtd','lacci_id_mtd')

# READ DATA MM
df_MM = hv.table('base.prep_mm')\
        .dropDuplicates(['subs_id'])\
        .select('subs_id','lacci_id').toDF('subs_id_mm','lacci_id_mm')

# READ DATA LACCIMA
df_LACCIMA = hv.table('base.laccima')\
            .select('lacci_id','eci','cgi_post','area_sales','lac','ci','node','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')\
            .toDF('lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')

# READ DATA SA_PRODUCT
df_SAproduct = hv.table('base.sa_product')\
                .withColumn('rkey', 
                    F.when((F.col('mapping_key_type').isNull()) | (F.col('mapping_key_type') == ""), F.col('rkey'))
                        .otherwise(F.col('mapping_key_type')))\
                .select('rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')

# PROCCESING
dfPAYR_Part2 = hv.table('output.sa_payr1')\
                .toDF('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc')\
                .join(df_MTD.alias('b'),col('subs_id') == col('subs_id_mtd'), how = 'left')\
                .withColumn("trx_lacci", when(col('trx_lacci') == "|","").otherwise(col('trx_lacci')))\
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
            .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')\
            .withColumn('Flag',when(col('trx_lacci') == col('lacci_id_laccima'),'SAME').otherwise('DIFF'))

# SPLIT FILTER
df_goodLACCI = dfPAYR_Part2.filter(col('Flag') == 'SAME')\
                .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag') 

df_rejectLACCI = dfPAYR_Part2.filter(col('Flag') == 'DIFF')\
                .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','find_flag')\
                .join(df_LACCIMA,col('trx_lacci') == col('eci'), how = 'left')\
                .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')\
                .withColumn('Flag',when(col('trx_lacci') == col('eci'),'SAME').otherwise('DIFF'))

# SPLIT FILTER
df_goodECI = df_rejectLACCI.filter(col('Flag') == 'SAME').select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

df_rejectECI = df_rejectLACCI.filter(col('Flag') == 'DIFF')\
                 .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','find_flag')\
                 .join(df_LACCIMA.dropDuplicates(['cgi_post']),col('trx_lacci') == col('cgi_post'), how = 'left')\
                 .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# MERGE HASIL 
df = [ df_goodLACCI,df_goodECI,df_rejectECI ]
dfMerge = reduce(DataFrame.unionAll, df)\
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
            .join(df_SAproduct,col('offer_id') == col('rkey'), how = 'left')\
            .fillna("NQ",["l4_name"]).fillna("NQ",["l3_name"]).fillna("NQ",["l2_name"]).fillna("NQ",["l1_name"])\
            .withColumn("file_id", lit("")).withColumn("load_ts", lit(cur_time)).withColumn("load_user", lit('solusi247'))\
            .select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count')\
            .withColumn('status_reject', F.when((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan').isNull())\
                                                    | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "NQ"))\
                                                    | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "-99")), 'YES').otherwise('NO'))

# GET hasil GOOD dan REJECT
df_hasilGOOD = dfMerge.filter(col('status_reject') == 'NO').select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10')\
                .write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_Payr_GOOD", sep ='~')
df_hasilREJECT = dfMerge.filter(col('status_reject') == 'YES')\
                .write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_Payr_REJECT", sep ='~')
'''
# GET hasil GOOD dan REJECT
df_hasilGOOD = dfMerge.filter(col('status_reject') == 'NO').select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10')\
                 .write.mode('overwrite').saveAsTable("output.sa_payr_good_hive")
df_hasilREJECT = dfMerge.filter(col('status_reject') == 'YES')\
                 .write.mode('overwrite').saveAsTable("output.sa_payr_reject_hive")
'''
end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)