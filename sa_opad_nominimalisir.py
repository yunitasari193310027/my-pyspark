import datetime
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

sc=SparkContext()
spark = SparkSession(sparkContext=sc)

# Read data SA_PAYR
df_SAOPAD = spark.read.csv("file:///home/hdoop/datayunita/SA_OPAD/OPAD_JKT_Pre_20221220235959_00000534_51.1.dat", sep ='|', header = False)
df_SAOPAD = df_SAOPAD.toDF('timestamp_r','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','payment_channel')

# get filename trx_date trx_hour event_date brand site_name pre_post_flag
df_SAOPAD = df_SAOPAD.withColumn('file_id', lit("OPAD_JKT_Pre_20221220235959_00000534_51.1"))\
    .withColumn('trx_date', substring('timestamp_r', 0,8))\
    .withColumn('trx_hour', substring('timestamp_r', 8,6))\
    .withColumn('brand', lit("byU")).withColumn('pre_post_flag', lit("1"))

# sc offer_id
df_SAOPAD = df_SAOPAD.withColumn('offer_id', 
                F.when(F.col('main_plan_id').isNull(), F.col('topping_plan_id'))
                    .otherwise(F.col('main_plan_id')))

# Read data IFRS
df_IFRS = spark.read.parquet("file:///home/hdoop/datayunita/SA_OPAD/IFRS")
df_IFRS = df_IFRS.toDF('trx_date','rkey','mapping_key_type','service_type','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')
df_IFRS = df_IFRS.select('l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','service_type','rkey')
df_IFRS = df_IFRS.filter(df_IFRS.service_type == '1_')

# JOIN ke IFRS
dfOPAD_IFRS = df_SAOPAD.join(df_IFRS,df_SAOPAD.offer_id == df_IFRS.rkey, how = 'left')
dfOPAD_IFRS = dfOPAD_IFRS.select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','offer_id','pre_post_flag','payment_channel','file_id','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product')

# Read data PRODUCT_LINE
df_PRODUCT_LINE = spark.read.parquet("file:///home/hdoop/datayunita/SA_OPAD/LINEDIM")
df_PRODUCT_LINE = df_PRODUCT_LINE.toDF('trx_date','rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')
df_PRODUCT_LINE = df_PRODUCT_LINE.select('l1_name','l2_name','l3_name','l4_name','rkey','service_type')
df_PRODUCT_LINE = df_PRODUCT_LINE.filter(df_PRODUCT_LINE.service_type == '1_')

# JOIN ke PRODUCT_LINE
dfJoin_PRODUCT_LINE = dfOPAD_IFRS.join(df_PRODUCT_LINE,dfOPAD_IFRS.offer_id == df_PRODUCT_LINE.rkey, how = 'left')
df_Part1 = dfJoin_PRODUCT_LINE.select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','offer_id','pre_post_flag','payment_channel','file_id','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','l1_name','l2_name','l3_name','l4_name')

from datetime import datetime 
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

df_Part1 = df_Part1.withColumn("load_ts", lit(cur_time))\
    .withColumn("load_user", lit('solusi247'))\
    .withColumn("load_date", lit(cur_date))\
    .withColumn("event_date", lit(F.col('trx_date')))

df_statusREJECT = df_Part1.withColumn('status_reject',when((col('l1_payu').isNotNull()) & (col('l2_service_type').isNotNull()) & (col('l3_allowance_type').isNotNull()) & (col('l4_product_category').isNotNull()) & (col('l5_product').isNotNull()) & (col('l1_name').isNotNull()) & (col('l2_name').isNotNull()) & (col('l3_name').isNotNull()) & (col('l4_name').isNotNull()),'GOOD').otherwise('REJECT'))
df_GOOD = df_statusREJECT.filter(col('status_reject') == 'GOOD')

df_GOOD = df_GOOD.select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','file_id','load_ts','load_user','offer_id','pre_post_flag','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','l1_name','l2_name','l3_name','l4_name','payment_channel')

df_REJECT = df_statusREJECT.filter(col('status_reject') == 'REJECT')
df_REJECT = df_REJECT.select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','file_id','load_ts','load_user','offer_id','pre_post_flag','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','l1_name','l2_name','l3_name','l4_name','payment_channel')

# CEK
df_GOOD.show()
print("Total row hasil GOOD=",df_GOOD.count()," dan hasil REJECT=", df_REJECT.count())

# WRITE SA_OPAD GOOD & REJECT to csv
df_GOOD.write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_OPAD_GOOD", sep ='~')
df_REJECT.write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_OPAD_REJECT", sep ='~')

