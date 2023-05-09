from pyspark.sql import SparkSession
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from functools import reduce

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

hv = HiveContext(spark)
#spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")
#spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

# READ DATA SA_PAYJ part1
dfPAYJ_SUB = hv.table('output.sa_payj1')

# READ DATA MTD
#hv.sql('CREATE TABLE IF NOT EXISTS base.prep_mtd(trx_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_name string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,vol string,trx string,dur string,load_ts string,load_user string,job_id string) STORED AS PARQUET;')
#hv.sql("load data local inpath 'file:///home/hdoop/datayunita/dom_mtd' overwrite into table base.prep_mtd;")
df_MTD = hv.table('base.prep_mtd')

# CEK
#print("JUMLAH KOLOM MTD",df_MTD.count())

df_MTD = df_MTD.toDF('trx_date','subs_id_mtd','lacci_id_mtd','msisdn','cust_type_desc','cust_subtype_desc','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','vol','trx','dur','load_ts','load_user','job_id')
df_MTD = df_MTD.select('subs_id_mtd','lacci_id_mtd')
df_MTD = df_MTD.dropDuplicates(['subs_id_mtd'])

# JOIN ke MTD
dfJOIN_MTD = dfPAYJ_SUB.join(df_MTD,dfPAYJ_SUB.subs_id ==  df_MTD.subs_id_mtd, how = 'left')
dfJOIN_MTD = dfJOIN_MTD.withColumn("trx_lacci", when(dfJOIN_MTD.trx_lacci == "|","").otherwise(dfJOIN_MTD.trx_lacci))

# CEK
print("Total SA_PAYJ part1=",dfPAYJ_SUB.count(),"Total MTD=",df_MTD.count(),"Total JOIN MTD=",dfJOIN_MTD.count())

# SWITCH CASE trx_lacci
dfJOIN_MTD = dfJOIN_MTD.withColumn('trx_lacci', 
                      F.when((F.col('trx_lacci').isNull()) & (F.col('subs_id') == F.col('subs_id_mtd')), F.col('lacci_id_mtd'))
                          .otherwise(F.col('trx_lacci')))

# READ DATA MM
#hv.sql('drop table base.prep_mm;')
#hv.sql('CREATE TABLE IF NOT EXISTS base.prep_mm(mm_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,vol string,trx string,dur string,load_ts string,load_user string,job_id string) STORED AS PARQUET;')
#spark.read.parquet('file:///home/hdoop/datayunita/dom_mm').write.mode('overwrite').saveAsTable("base.prep_mm")
df_MM = hv.table('base.prep_mm')
df_MM = df_MM.toDF('trx_date','subs_id_mm','lacci_id_mm','msisdn','cust_type_desc','cust_subtype_desc','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','vol','trx','dur','load_ts','load_user','job_id')
df_MM = df_MM.select('subs_id_mm','lacci_id_mm')
df_MM = df_MM.dropDuplicates(['subs_id_mm'])

# JOIN ke MM
dfJOIN_MM = dfJOIN_MTD.join(df_MM,dfJOIN_MTD.subs_id ==  df_MM.subs_id_mm, how = 'left')

# CEK
print('JUMLAH KOLOM MM',df_MM.count(),'JUMLAH KOLOM JOIN MM',dfJOIN_MM.count())

# SWITCH CASE trx_lacci
dfJOIN_MM = dfJOIN_MM.withColumn('trx_lacci', 
                      F.when((F.col('trx_lacci').isNull()) & (F.col('subs_id') == F.col('subs_id_mm')), F.col('lacci_id_mm'))
                          .otherwise(F.col('trx_lacci')))

dfJOIN_MM = dfJOIN_MM.withColumn('find_flag', 
                      F.when((F.col('subs_id') == F.col('subs_id_mtd')) & (F.col('lacci_id_mtd') == F.col('trx_lacci')), '1#')
                        .when((F.col('subs_id') == F.col('subs_id_mm')) & (F.col('lacci_id_mm') == F.col('trx_lacci')), '2#')
                          .otherwise("0"))

dfJOIN_MM = dfJOIN_MM.fillna("-99",["trx_lacci"])

# READ DATA LACCIMA
df_LACCIMA = hv.table('base.laccima')
df_LACCIMA = df_LACCIMA.select('lacci_id','eci','cgi_post','area_sales','lac','ci','node','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')
df_LACCIMA = df_LACCIMA.toDF('lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')

# JOIN ke LACCI_ID LACCIMA
dfJOIN_LACCI = dfJOIN_MM.join(df_LACCIMA,dfJOIN_MM.trx_lacci ==  df_LACCIMA.lacci_id_laccima, how = 'left')
dfJOIN_LACCI = dfJOIN_LACCI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# CEK
print("laccima=",df_LACCIMA.count(),"join laccima=",dfJOIN_LACCI.count())

# CREATE FLAG for split filter
data_flagLACCI = dfJOIN_LACCI.withColumn('Flag',when(col('trx_lacci') == col('lacci_id_laccima'),'SAME').otherwise('DIFF'))
df_goodLACCI  = data_flagLACCI.filter(col('Flag')  == 'SAME')
df_rejectLACCI = data_flagLACCI.filter(col('Flag') == 'DIFF')
df_goodLACCI  = df_goodLACCI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag') 
df_rejectLACCI = df_rejectLACCI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','find_flag')

# ket = masalah ada disini JOIN ECI= 772157367 ECI GOOD= 772151757  
#JOIN ke ECI LACCIMA
df_LACCIMA = df_LACCIMA.dropDuplicates(['eci'])
dfJOIN_ECI = df_rejectLACCI.join(df_LACCIMA,df_rejectLACCI.trx_lacci ==  df_LACCIMA.eci, how = 'left')
dfJOIN_ECI = dfJOIN_ECI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# CEK
print("data_flagLACCI=",data_flagLACCI.count(),"df_goodLACCI=",df_goodLACCI.count(),"df_rejectLACCI=",df_rejectLACCI.count(),"dfJOIN_ECI=",dfJOIN_ECI.count())

# CREATE FLAG for split filter
data_flagECI = dfJOIN_ECI.withColumn('Flag',when(col('trx_lacci') == col('eci'),'SAME').otherwise('DIFF'))
df_goodECI = data_flagECI.filter(col('Flag')  == 'SAME')
df_rejectECI = data_flagECI.filter(col('Flag') == 'DIFF')
df_goodECI = df_goodECI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')
df_rejectECI = df_rejectECI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','find_flag')

# CEK
print("JOIN ECI=", dfJOIN_ECI.count(),"ECI GOOD=", df_goodECI.count())

# JOIN ke cgi_post LACCIMA
df_LACCIMA = df_LACCIMA.dropDuplicates(['cgi_post'])
dfJOIN_CGI = df_rejectECI.join(df_LACCIMA,df_rejectECI.trx_lacci ==  df_LACCIMA.cgi_post, how = 'left')
df_CGI = dfJOIN_CGI.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# CREATE FLAG for split filter to cek good&reject cgi 
data_flagCGI = dfJOIN_CGI.withColumn('Flag',when(col('trx_lacci') == col('cgi_post'),'SAME').otherwise('DIFF'))
df_goodCGI = data_flagCGI.filter(col('Flag')  == 'SAME')
df_rejectCGI = data_flagCGI.filter(col('Flag') == 'DIFF')

# CEK
print("Total row hasil Join df_goodLACCI=",df_goodLACCI.count(),"df_goodECI=",df_goodECI.count(),"dan CGI=",df_CGI.count())
#print("jumlah kolom sampai dengan laccima=",df_untilLACCIMA.count())

# MERGE HASIL JOIN lacci_id, eci, cgi_post from LACCIMA
df = [ df_goodLACCI,df_goodECI,df_CGI ]
dfMerge = reduce(DataFrame.unionAll, df)

# CEK
#print("Total row hasil MERGE=",dfMerge.count())

# SWITCH CASE lacci_id
dfMerge = dfMerge.withColumn('flag', 
                      F.when((F.col('trx_lacci') == F.col('lacci_id_laccima')) | (F.col('trx_lacci') == F.col('eci')) | (F.col('trx_lacci') == F.col('cgi_post')), "y")
                          .otherwise("x"))

df1 = dfMerge.withColumn('lacci_id_flag', 
                      F.when((F.col('trx_lacci') != "-99") & (F.col('flag') == "y") & (F.col('area_name') != "UNKNOWN") & (F.col('find_flag') == (0)), (concat(lit('0#'),col('lacci_id_laccima'))))
                        .when((F.col('trx_lacci') != "-99") & (F.col('flag') == "y") & (F.col('area_name') != "UNKNOWN") & (F.col('find_flag').isNotNull()), (concat(col('find_flag'),col('trx_lacci'))))
                          .otherwise("-1#-99 "))

# GET lacci_id and lacci_closing_flag from lacci_id_flag
df_untilLACCIMA = df1.withColumn('lacci_closing_flag', split(col('lacci_id_flag'),'#').getItem(0))\
                        .withColumn('lacci_id', split(col('lacci_id_flag'),'#').getItem(1))

# NVL
df_untilLACCIMA = df_untilLACCIMA.fillna("NQ",["lac"]).fillna("NQ",["ci"]).fillna("NQ",["node_type"]).fillna("NQ",["area_name"]).fillna("NQ",["region_sales"]).fillna("NQ",["branch"]).fillna("NQ",["subbranch"]).fillna("NQ",["cluster_sales"]).fillna("NQ",["provinsi"]).fillna("NQ",["kabupaten"]).fillna("NQ",["kecamatan"]).fillna("NQ",["kelurahan"])
df_untilLACCIMA = df_untilLACCIMA.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','lacci_id_flag','lacci_id','lacci_closing_flag')
df_untilLACCIMA.show()
# CEK
df_untilLACCIMA.filter("lacci_closing_flag == '0'").show()

# READ DATA SA_PRODUCT
df_SAproduct = hv.table('base.sa_product')

# SWITCH CASE rkey
df_SAproduct = df_SAproduct.withColumn('rkey', 
                      F.when((F.col('mapping_key_type').isNull()) | (F.col('mapping_key_type') == ""), F.col('rkey'))
                          .otherwise(F.col('mapping_key_type')))

df_SAproduct = df_SAproduct.select('rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')

# JOIN ke SA_PRODUCT
dfJOIN_SAproduct = df_untilLACCIMA.join(df_SAproduct,df_untilLACCIMA.offer_id ==  df_SAproduct.rkey, how = 'left')

df_untilSAproduct = dfJOIN_SAproduct.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_flag','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','l4_name','l3_name','l2_name','l1_name','retry_count')

# NVL
df_untilSAproduct = df_untilSAproduct.fillna("NQ",["l4_name"]).fillna("NQ",["l3_name"]).fillna("NQ",["l2_name"]).fillna("NQ",["l1_name"])

from datetime import datetime 
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
df_untilSAproduct = df_untilSAproduct.withColumn("file_id", lit("")).withColumn("load_ts", lit(cur_time)).withColumn("load_user", lit('solusi247'))

df_untilSAproduct = df_untilSAproduct.select('activation_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count')

# GET status_reject
df_statusREJECT  = df_untilSAproduct.withColumn('status_reject', F.when((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan').isNull())\
                                                                         | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "NQ"))\
                                                                            | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "-99")), 'YES').otherwise('NO'))
# CEK
#print("Cek hasil status reject bervalue YES= ", df_statusREJECT.filter("status_reject =='YES'").show())

# GET hasil GOOD dan REJECT
df_hasilGOOD = df_statusREJECT.filter(col('status_reject')  == 'NO')
df_hasilGOOD = df_hasilGOOD.select('activation_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10')
df_hasilREJECT = df_statusREJECT.filter(col('status_reject') == 'YES')

# CEK
print("Total row hasil GOOD=",df_hasilGOOD.count()," dan hasil REJECT=", df_hasilREJECT.count())

spark.sql("CREATE DATABASE IF NOT EXISTS output")

# Create Hive Internal table
df_hasilGOOD.write.mode('overwrite') \
          .saveAsTable("output.sa_payj_good")
df_hasilREJECT.write.mode('overwrite') \
          .saveAsTable("output.sa_payj_reject")          

