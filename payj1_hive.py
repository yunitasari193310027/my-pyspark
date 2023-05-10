from pyspark.sql import SparkSession
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

hv = HiveContext(spark)

# Read data SA_PAYJ
df_PAYJ = hv.table('base.source_payj')

# get filename trx_date trx_hour event_date brand site_name pre_post_flag
df_PAYJ = df_PAYJ.withColumn('filename', lit("PAYJ_JKT_Pre_20221220235959_00000634_51.1")).withColumn('trx_date', substring('payment_timestamp', 0,10)).withColumn('trx_hour', substring('payment_timestamp', 11,8)).withColumn('event_date', substring('payment_timestamp', 0,10)).withColumn('brand', lit("byU")).withColumn('site_name', lit("JKT")).withColumn('pre_post_flag', lit("1"))

# sc plan_price & offer_id
df_PAYJ = df_PAYJ.withColumn('plan_price', 
                    F.when(F.col('plan_price') == "NQ" , "0")
                        .when(F.col('plan_price').isNull(), "0")
                            .otherwise(F.col('plan_price')))

df_PAYJ = df_PAYJ.withColumn('offer_id', 
                    F.when(F.col('plan_id').isNull(), F.col('topping_id'))
                        .when(F.col('topping_id').isNull(), F.col('plan_id'))
                            .otherwise(F.col('plan_id')))

# PROSES TO get cel_id & trx_lacci

df_PAYJ = df_PAYJ.withColumn('x1', substring('cell_id', 10,7))\
                .withColumn('x2',F.expr("substring(cell_id,17,length(cell_id))"))\
                .withColumn('x3', substring('cell_id', 5,5))\
                .withColumn('x4',F.expr("substring(cell_id,10,length(cell_id))"))

df_PAYJ = df_PAYJ.withColumn('sbstr_11_17', F.regexp_replace('x1', r'^0', ''))\
                .withColumn('sbstr_17_end', F.regexp_replace('x2', r'^0', ''))\
                .withColumn('sbstr_6_10', F.regexp_replace('x3', r'^0', ''))\
                .withColumn('sbstr_10_end', F.regexp_replace('x4', r'^0', ''))

df_PAYJ = df_PAYJ.withColumn('c1',F.concat(F.col('sbstr_11_17'),F.lit('_'), F.col('sbstr_17_end')))\
                .withColumn('c2',F.concat(F.col('sbstr_6_10'),F.lit('_'), F.col('sbstr_10_end')))

# sw cell_id
df_PAYJ = df_PAYJ.withColumn('cell_id', 
                    F.when((F.col('indicator_4g') == "129") | (F.col('indicator_4g') == "130"), F.col('c1'))
                        .when((F.col('indicator_4g') == "128") | (F.col('indicator_4g') == "131"), F.col('cell_id'))
                            .otherwise(F.col('c2')))

df_PAYJ = df_PAYJ.withColumn('t1', F.regexp_replace('c1', r'_', '|'))\
                .withColumn('t2', F.regexp_replace('c2', r'_', '|'))

# sw trx_lacci
df_PAYJ = df_PAYJ.withColumn('trx_lacci', 
                        F.when((F.col('indicator_4g') == "129") | (F.col('indicator_4g') == "130"), F.col('t1'))
                        .when((F.col('indicator_4g') == "128") | (F.col('indicator_4g') == "131"), F.col('cell_id'))
                            .otherwise(F.col('t2')))

df_PAYJ = df_PAYJ.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci').withColumn("retry_count", lit(0))

#df_PAYJ.show()

# Read data SUB_DIM
df_SUB = hv.table('base.subs_dim')
df_SUB = df_SUB.select('msisdn','subs_id','cust_type_desc','cust_subtype_desc')

# JOIN SA_PAYR & SUB_DIM
dfPAYR_SUB = df_PAYJ.join(df_SUB,df_PAYJ.msisdn == df_SUB.msisdn, how = 'left').drop(df_SUB.msisdn)
#nvl
dfPAYR_SUB = dfPAYR_SUB.fillna("-99",["subs_id"]).fillna("UNKNOWN",["cust_type_desc"]).fillna("UNKNOWN",["cust_subtype_desc"])
dfPAYR_SUB = dfPAYR_SUB.select('activation_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','payment_timestamp','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','future_string_4','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','subs_id','cust_type_desc','cust_subtype_desc','retry_count')
dfPAYR_SUB.show()

spark.sql("CREATE DATABASE IF NOT EXISTS output")

# Create Hive Internal table
dfPAYR_SUB.write.mode('overwrite') \
    .saveAsTable("output.sa_payj1")

