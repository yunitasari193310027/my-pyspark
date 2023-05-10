from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pyspark.sql.functions as F

sc=SparkContext()
spark = SparkSession(sparkContext=sc)
start_date = datetime.now() + timedelta(hours=7)
# Read data SUB_DIM
df_SUB = spark.read.parquet("file:///home/hdoop/datayunita/subs")\
            .toDF('trx_date','subs_id','msisdn','account_id','status','pre_post_flag','activation_date','deactivation_date','los','price_plan_id','prefix','area_hlr','region_hlr','city_hlr','cust_type_desc','cust_subtype_desc','customer_sub_segment','load_ts','load_user','job_id','migration_date')\
            .select('subs_id','msisdn','price_plan_id','area_hlr','region_hlr','cust_type_desc','cust_subtype_desc','customer_sub_segment','city_hlr')

# Read data SA_PAYR
df_PAYR = spark.read.csv("file:///home/hdoop/datayunita/PAYR_JKT_Pre_20221219235959_00000574_51.1.dat", sep ='|', header = False)\
                .toDF('payment_timestamp','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10')\
                .withColumn('filename', lit("PAYR_JKT_Pre_20221219235959_00000574_51.1.dat")).withColumn('trx_date', substring('payment_timestamp', 0,10)).withColumn('trx_hour', substring('payment_timestamp', 11,8)).withColumn('event_date', substring('payment_timestamp', 0,10)).withColumn('brand', lit("byU")).withColumn('site_name', lit("JKT")).withColumn('pre_post_flag', lit("1"))\
                .withColumn('plan_price', 
                    F.when(F.col('plan_price') == "NQ" , "0")
                        .when(F.col('plan_price').isNull(), "0")
                            .otherwise(F.col('plan_price')))\
                .withColumn('offer_id', 
                    F.when(F.col('plan_id') == "SA10359" , F.col('future_string_1'))
                        .when(F.col('plan_id').isNull(), F.col('topping_id'))
                        .when(F.col('topping_id').isNull(), F.col('plan_id'))
                            .otherwise(F.col('plan_id')))\
                .withColumn('x1', substring('cell_id', 10,7))\
                .withColumn('x2',F.expr("substring(cell_id,17,length(cell_id))"))\
                .withColumn('x3', substring('cell_id', 5,5))\
                .withColumn('x4',F.expr("substring(cell_id,10,length(cell_id))"))\
                .withColumn('sbstr_11_17', F.regexp_replace('x1', r'^0', ''))\
                .withColumn('sbstr_17_end', F.regexp_replace('x2', r'^0', ''))\
                .withColumn('sbstr_6_10', F.regexp_replace('x3', r'^0', ''))\
                .withColumn('sbstr_10_end', F.regexp_replace('x4', r'^0', ''))\
                .withColumn('c1',F.concat(F.col('sbstr_11_17'),F.lit('_'), F.col('sbstr_17_end')))\
                .withColumn('c2',F.concat(F.col('sbstr_6_10'),F.lit('_'), F.col('sbstr_10_end')))\
                .withColumn('cell_id', 
                    F.when((F.col('indicator_4g') == "129") | (F.col('indicator_4g') == "130"), F.col('c1'))
                    .when((F.col('indicator_4g') == "128") | (F.col('indicator_4g') == "131"), F.col('cell_id'))
                        .otherwise(F.col('c2')))\
                .withColumn('t1', F.regexp_replace('c1', r'_', '|'))\
                .withColumn('t2', F.regexp_replace('c2', r'_', '|'))\
                .withColumn('trx_lacci', 
                    F.when((F.col('indicator_4g') == "129") | (F.col('indicator_4g') == "130"), F.col('t1'))
                    .when((F.col('indicator_4g') == "128") | (F.col('indicator_4g') == "131"), F.col('cell_id'))
                        .otherwise(F.col('t2')))\
                .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci').withColumn("retry_count", lit(0))\
                .alias('a').join(df_SUB.alias('b'),col('a.msisdn') == col('b.msisdn'), how = 'left')\
                .fillna("-99",["subs_id"]).fillna("UNKNOWN",["cust_type_desc"]).fillna("UNKNOWN",["cust_subtype_desc"])\
                .select('payment_timestamp','trx_date','trx_hour','a.msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc')\
                .write.csv("file:///home/hdoop/BelajarPyspark/hasilSA_Payr_Part1_minimalisir", sep ='~')

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)