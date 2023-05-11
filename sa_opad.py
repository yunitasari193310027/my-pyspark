import datetime
import pyspark
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta 
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

sc=SparkContext()
spark = SparkSession(sparkContext=sc)
start_date = datetime.now() + timedelta(hours=7)
run_date = datetime.now().strftime('%Y%m%d%H%M%S')
# ===============================> READ DATA
df_IFRS = spark.read.parquet("file:///home/hdoop/datayunita/SA_OPAD/IFRS")\
                .toDF('trx_date','rkey','mapping_key_type','service_type','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')\
                .select('l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','service_type','rkey')\
                .filter(col('service_type') == '1_')

df_PRODUCT_LINE = spark.read.parquet("file:///home/hdoop/datayunita/SA_OPAD/LINEDIM")\
                .toDF('trx_date','rkey','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user')\
                .select('l1_name','l2_name','l3_name','l4_name','rkey','service_type')\
                .filter(col('service_type') == '1_')

df_SAOPAD = spark.read.csv("file:///home/hdoop/datayunita/SA_OPAD/OPAD_JKT_Pre_20221220235959_00000534_51.1.dat", sep ='|', header = False)\
                .toDF('timestamp_r','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','payment_channel')\
                .withColumn('file_id', lit("OPAD_JKT_Pre_20221220235959_00000534_51.1"))\
                .withColumn('trx_date', substring('timestamp_r', 0,8))\
                .withColumn('trx_hour', substring('timestamp_r', 8,6))\
                .withColumn('brand', lit("byU")).withColumn('pre_post_flag', lit("1"))\
                .withColumn('offer_id', 
                    F.when(F.col('main_plan_id').isNull(), F.col('topping_plan_id'))
                        .otherwise(F.col('main_plan_id')))

# ===============================> PROSES JOIN
df_statusREJECT = df_SAOPAD.join(df_IFRS,df_SAOPAD.offer_id == df_IFRS.rkey, how = 'left')\
                .select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','offer_id','pre_post_flag','payment_channel','file_id','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product')\
                .join(df_PRODUCT_LINE,col('offer_id') == col('rkey'), how = 'left')\
                .select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','offer_id','pre_post_flag','payment_channel','file_id','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','l1_name','l2_name','l3_name','l4_name')\
                .withColumn("load_ts", lit(cur_time))\
                .withColumn("load_user", lit('solusi247'))\
                .withColumn("load_date", lit(cur_date))\
                .withColumn("event_date", lit(F.col('trx_date')))\
                .withColumn('status_reject',
                    when((col('l1_payu').isNotNull()) & (col('l2_service_type').isNotNull()) & (col('l3_allowance_type').isNotNull()) & (col('l4_product_category').isNotNull()) & (col('l5_product').isNotNull()) & (col('l1_name').isNotNull()) & (col('l2_name').isNotNull()) & (col('l3_name').isNotNull()) & (col('l4_name').isNotNull()),'GOOD')
                        .otherwise('REJECT'))

# SPLIT FILTER
df_GOOD = df_statusREJECT.filter(col('status_reject') == 'GOOD')\
                .select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','file_id','load_ts','load_user','offer_id','pre_post_flag','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','l1_name','l2_name','l3_name','l4_name','payment_channel')
                
df_REJECT = df_statusREJECT.filter(col('status_reject') == 'REJECT')\
                .select('timestamp_r','trx_date','trx_hour','user_name','msisdn','order_id','main_plan_name','main_plan_id','topping_plan_name','topping_plan_id','price','brand','file_id','load_ts','load_user','offer_id','pre_post_flag','l1_payu','l2_service_type','l3_allowance_type','l4_product_category','l5_product','l1_name','l2_name','l3_name','l4_name','payment_channel')
# ===============================> WRITE OUTPUT              
df_GOOD.write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_OPAD_GOOD", sep ='~')
df_REJECT.write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_OPAD_REJECT", sep ='~')

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)

# CREATE FILELOG
app = "sa_opad"
nama_file = "/home/hdoop/BelajarPyspark/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.writelines('\nINPUT_COUNT={}'.format(df_SAOPAD.count()))
f.writelines('\nOUTPUT_GOOD_COUNT={}'.format(df_GOOD.count()))
f.writelines('\nOUTPUT_REJECT_COUNT={}'.format(df_REJECT.count()))
f.close()    


