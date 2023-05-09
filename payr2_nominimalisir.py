import datetime
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from functools import reduce
from datetime import datetime, timedelta 

sc=SparkContext()
spark = SparkSession(sparkContext=sc)
#spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
start_date = datetime.now() + timedelta(hours=7)

# READ DATA SA_PAYR
dfPAYR_SUB = spark.read.csv("file:///home/hdoop/BelajarPyspark/hasilSA_Payr_Part1", sep ='~', header = False)
dfPAYR_SUB = dfPAYR_SUB.toDF('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc')

# READ DATA MTD
df_MTD = spark.read.parquet("file:///home/hdoop/datayunita/dom_mtd")
df_MTD = df_MTD.toDF('trx_date','subs_id_mtd','lacci_id_mtd','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','vol','trx','dur','load_ts','load_user','job_id')
df_MTD = df_MTD.select('subs_id_mtd','lacci_id_mtd')
df_MTD = df_MTD.dropDuplicates(['subs_id_mtd'])

# JOIN ke MTD
dfJOIN_MTD = dfPAYR_SUB.join(df_MTD,dfPAYR_SUB.subs_id ==  df_MTD.subs_id_mtd, how = 'left')
dfJOIN_MTD = dfJOIN_MTD.withColumn("trx_lacci", when(dfJOIN_MTD.trx_lacci == "|","").otherwise(dfJOIN_MTD.trx_lacci))

# SWITCH CASE trx_lacci
dfJOIN_MTD = dfJOIN_MTD.withColumn('trx_lacci', 
                      F.when((F.col('trx_lacci').isNull()) & (F.col('subs_id') == F.col('subs_id_mtd')), F.col('lacci_id_mtd'))
                          .otherwise(F.col('trx_lacci')))

# READ DATA MM
df_MM = spark.read.parquet("file:///home/hdoop/datayunita/dom_mm")
df_MM = df_MM.toDF('trx_date','subs_id_mm','lacci_id_mm','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','vol','trx','dur','load_ts','load_user','job_id')
df_MM = df_MM.select('subs_id_mm','lacci_id_mm')
df_MM = df_MM.dropDuplicates(['subs_id_mm'])

# JOIN ke MM
dfJOIN_MM = dfJOIN_MTD.join(df_MM,dfJOIN_MTD.subs_id ==  df_MM.subs_id_mm, how = 'left')

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
df_LACCIMA = spark.read.csv("file:///home/hdoop/datayunita/USAGECHG/LACCIMA_DIM.csv",sep=';',header=True)
df_LACCIMA = df_LACCIMA.select('lacci_id','eci','cgi_post','area_sales','lac','ci','node','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')
df_LACCIMA = df_LACCIMA.toDF('lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan')

# JOIN ke LACCI_ID LACCIMA
dfJOIN_LACCI = dfJOIN_MM.join(df_LACCIMA,dfJOIN_MM.trx_lacci ==  df_LACCIMA.lacci_id_laccima, how = 'left')
dfJOIN_LACCI = dfJOIN_LACCI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# CREATE FLAG for split filter
data_flagLACCI = dfJOIN_LACCI.withColumn('Flag',when(col('trx_lacci') == col('lacci_id_laccima'),'SAME').otherwise('DIFF'))
df_goodLACCI  = data_flagLACCI.filter(col('Flag')  == 'SAME')
df_rejectLACCI = data_flagLACCI.filter(col('Flag') == 'DIFF')
df_goodLACCI  = df_goodLACCI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag') 
df_rejectLACCI = df_rejectLACCI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','find_flag')

#JOIN ke ECI LACCIMA
dfJOIN_ECI = df_rejectLACCI.join(df_LACCIMA,df_rejectLACCI.trx_lacci ==  df_LACCIMA.eci, how = 'left')
dfJOIN_ECI = dfJOIN_ECI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# CREATE FLAG for split filter
data_flagECI = dfJOIN_ECI.withColumn('Flag',when(col('trx_lacci') == col('eci'),'SAME').otherwise('DIFF'))
df_goodECI = data_flagECI.filter(col('Flag')  == 'SAME')
df_rejectECI = data_flagECI.filter(col('Flag') == 'DIFF')
df_goodECI = df_goodECI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')
df_rejectECI = df_rejectECI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','find_flag')

# JOIN ke cgi_post LACCIMA
df_LACCIMA = df_LACCIMA.dropDuplicates(['cgi_post'])
dfJOIN_CGI = df_rejectECI.join(df_LACCIMA,df_rejectECI.trx_lacci ==  df_LACCIMA.cgi_post, how = 'left')
df_CGI = dfJOIN_CGI.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','find_flag')

# CREATE FLAG for split filter to cek good&reject cgi 
data_flagCGI = dfJOIN_CGI.withColumn('Flag',when(col('trx_lacci') == col('cgi_post'),'SAME').otherwise('DIFF'))
df_goodCGI = data_flagCGI.filter(col('Flag')  == 'SAME')
df_rejectCGI = data_flagCGI.filter(col('Flag') == 'DIFF')
# CEK
#print("Total row hasil Join CGI good=",df_goodCGI.count(),"dan reject=",df_rejectCGI.count())

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
df1.show()

# GET lacci_id and lacci_closing_flag from lacci_id_flag
df_untilLACCIMA = df1.withColumn('lacci_closing_flag', split(col('lacci_id_flag'),'#').getItem(0))\
                        .withColumn('lacci_id', split(col('lacci_id_flag'),'#').getItem(1))

# NVL
df_untilLACCIMA = df_untilLACCIMA.fillna("NQ",["lac"]).fillna("NQ",["ci"]).fillna("NQ",["node_type"]).fillna("NQ",["area_name"]).fillna("NQ",["region_sales"]).fillna("NQ",["branch"]).fillna("NQ",["subbranch"]).fillna("NQ",["cluster_sales"]).fillna("NQ",["provinsi"]).fillna("NQ",["kabupaten"]).fillna("NQ",["kecamatan"]).fillna("NQ",["kelurahan"])
df_untilLACCIMA = df_untilLACCIMA.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_laccima','eci','cgi_post','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','lacci_id_flag','lacci_id','lacci_closing_flag')

# READ DATA SA_PRODUCT
df_SAproduct = spark.read.parquet("file:///home/hdoop/datayunita/sa_product")
df_SAproduct = df_SAproduct.toDF('trx_date','rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')

# SWITCH CASE rkey
df_SAproduct = df_SAproduct.withColumn('rkey', 
                      F.when((F.col('mapping_key_type').isNull()) | (F.col('mapping_key_type') == ""), F.col('rkey'))
                          .otherwise(F.col('mapping_key_type')))

df_SAproduct = df_SAproduct.select('rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')

# JOIN ke SA_PRODUCT
dfJOIN_SAproduct = df_untilLACCIMA.join(df_SAproduct,df_untilLACCIMA.offer_id ==  df_SAproduct.rkey, how = 'left')

df_untilSAproduct = dfJOIN_SAproduct.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_flag','lacci_id','lacci_closing_flag','area_name','lac','ci','node_type','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','l4_name','l3_name','l2_name','l1_name')

# NVL
df_untilSAproduct = df_untilSAproduct.fillna("NQ",["l4_name"]).fillna("NQ",["l3_name"]).fillna("NQ",["l2_name"]).fillna("NQ",["l1_name"])

from datetime import datetime 
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
df_untilSAproduct = df_untilSAproduct.withColumn("file_id", lit("")).withColumn("load_ts", lit(cur_time)).withColumn("load_user", lit('solusi247'))

df_untilSAproduct = df_untilSAproduct.select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count')

# GET status_reject
df_statusREJECT  = df_untilSAproduct.withColumn('status_reject', F.when((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan').isNull())\
                                                                         | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "NQ"))\
                                                                            | ((F.coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == "-99")), 'YES').otherwise('NO'))
# CEK
#print("Cek hasil status reject bervalue YES= ", df_statusREJECT.filter("status_reject =='YES'").show())

# GET hasil GOOD dan REJECT
df_hasilGOOD = df_statusREJECT.filter(col('status_reject')  == 'NO')
df_hasilGOOD = df_hasilGOOD.select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_name','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10')

df_hasilREJECT = df_statusREJECT.filter(col('status_reject') == 'YES')

# CEK
#print("Total row hasil GOOD=",df_hasilGOOD.count()," dan hasil REJECT=", df_hasilREJECT.count())

# WRITE SA_PAYR GOOD and REJECT to csv
df_hasilGOOD.write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_Payr_GOOD", sep ='~')
df_hasilREJECT.write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_Payr_REJECT", sep ='~')

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)