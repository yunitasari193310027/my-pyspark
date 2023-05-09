import datetime
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

sc=SparkContext()
spark = SparkSession(sparkContext=sc)

# Read data HLR_PRE_DIM
df_HLR_PreDim = spark.read.parquet("file:///home/hdoop/datayunita/RevenueSA/HLR_PRE_DIM/*")\
                .select('regional','area','prefix','enriched_prefix','area_name','region_id','area_code','regional_code','file_id','load_user')

# Read data MERGE_REVENUE_SA
df_Revenue = spark.read.parquet("file:///home/hdoop/datayunita/RevenueSA/MERGE_REVENUE_SA/*/*/*/*")\
                .toDF('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts')\
                .withColumn('substr_msisdn1_8', substring('msisdn', 0,8))\
                .fillna("0",["dur"]).fillna("0",["vol"])\
                .join(df_HLR_PreDim,col('substr_msisdn1_8') == col('enriched_prefix'), how = 'left')\
                .withColumn('trx_year', substring('trx_date', 0,4))\
                .withColumn('trx_month', substring('trx_date', 5,2))\
                .withColumn('l3_cd', lit(""))\
                .withColumn('city_hlr', lit(""))\
                .withColumn('node', lit(""))\
                .withColumn('tot_user', lit(0))\
                .withColumn('tot_dur', col('vol') + col('dur'))\
                .withColumn('file_date', lit(col('trx_date')))\
                .withColumn('area_hlr', 
                    F.when((F.col('substr_msisdn1_8') == F.col('enriched_prefix')) & (F.col('area_name').isNotNull()) & (F.col('area_name') != "NQ"), F.col('area_name'))
                    .otherwise("UNKNOWN"))\
                .withColumn('region_hlr', 
                F.when((F.col('regional') == "Bali-Nusra") | (F.col('regional') == "BALI NUSRA"), "BALINUSRA")
                    .when((F.col('regional') == "Jabar-Banten"), "JABAR")
                    .when((F.col('regional') == "Jateng-Jogya"), "JATENG")
                    .when((F.col('regional') == "Jawa-Timur"), "JATIM")
                    .when((F.col('regional') == "Kalimantan"), "KALIMANTAN")
                    .when((F.col('regional') == "Papua") | (F.col('regional') == "PUMA"), "PAPUA")
                    .when((F.col('regional') == "SumBagUtara"), "SUMBAGUT")
                    .when((F.col('regional') == "Jabotabek"), "JABOTABEK")
                    .otherwise(F.col('regional')))\
                .withColumn('cust_type',
                    F.when((F.col('cust_subtype_desc') == "UNKNOWN"), "B2C")
                    .otherwise("B2B"))\
                .select('trx_date','trx_year','trx_month','trx_date','brand','l3_cd','area_hlr','region_hlr','city_hlr','l1_name','l2_name','l3_name','area','region_sales','cluster_sales','offer_name','node','cust_type','cust_type_desc','cust_subtype_desc','tot_user','tot_dur','rev','trx','trx_date','substr_msisdn1_8','enriched_prefix','regional','source_name')\
                .toDF('file_date','trx_year','trx_month','trx_date','brand','l3_cd','area_hlr','region_hlr','city_hlr','l1_name','l2_name','l3_name','area','region','cluster','offer_name','node','cust_type','cust_subtype','cust_subsegment','tot_user','tot_dur','tot_rev','tot_trx','event_date','substr_msisdn1_8','enriched_prefix','regional','source_name')\
                .withColumn('region_hlr',
                    F.when((F.col('substr_msisdn1_8') == F.col('enriched_prefix')) & (F.col('regional').isNotNull()) & (F.col('regional') != "NQ"), F.col('region_hlr'))
                    .otherwise("UNKNOWN"))\
                .withColumn('city_hlr',
                    F.when((F.col('regional').isNotNull()) & (F.col('regional') != "NQ"), F.col('regional'))
                    .otherwise("UNKNOWN"))\
                .withColumn('source_name', concat(lit('SA_'),col('source_name')))

# HLR
df_HLR = df_Revenue.groupby(['trx_year','trx_month','trx_date','brand','l3_cd','area_hlr','region_hlr','city_hlr','l1_name','l2_name','l3_name','event_date','source_name']).agg(sum('tot_user').alias('tot_user'), sum('tot_dur').alias('tot_dur'), sum('tot_rev').alias('tot_rev'), sum('tot_trx').alias('tot_trx'))\
                .write.csv("file:///home/hdoop/BelajarPyspark/HLR", sep ='~')

# VLR
df_VLR = df_Revenue.groupby(['file_date','area','region','cluster','brand','l1_name','l2_name','l3_name','offer_name','node','cust_type','cust_subtype','cust_subsegment','event_date','source_name']).agg(sum('tot_rev').alias('tot_rev'), sum('tot_trx').alias('tot_trx'), sum('tot_dur').alias('tot_dur'))\
                .write.csv("file:///home/hdoop/BelajarPyspark/VLR", sep ='~')

