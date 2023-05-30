import datetime
import pyspark
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta 

sc=SparkContext()
spark = SparkSession(sparkContext=sc)
start_date = datetime.now() + timedelta(hours=7)
run_date = datetime.now().strftime('%Y%m%d%H%M%S')

# ===============================> READ DATA
df_LACCI = spark.read.parquet("file:///home/hdoop/datayunita/LACCI/LACCI")\
            .toDF('msisdn','subs_id','chg_lacci_id','chg_area_name','chg_region_network','chg_cluster_sales','chg_site_id','chg_node','chg_provinsi','chg_kabupaten','chg_kecamatan','chg_kelurahan','chg_region_sales','chg_branch','chg_subbranch','chg_flag','usg_lacci_id','usg_area_name','usg_region_network','usg_cluster_sales','usg_site_id','usg_node','usg_provinsi','usg_kabupaten','usg_kecamatan','usg_kelurahan','usg_region_sales','usg_branch','usg_subbranch','usg_flag','ifrs_lacci_id','ifrs_area_name','ifrs_region_network','ifrs_cluster_sales','ifrs_site_id','ifrs_node','ifrs_provinsi','ifrs_kabupaten','ifrs_kecamatan','ifrs_kelurahan','ifrs_region_sales','ifrs_branch','ifrs_subbranch','ifrs_flag','event_date','job_id')

df_CHG = spark.read.parquet("file:///home/hdoop/datayunita/LACCI/CHG")\
            .toDF('trx_date','subs_id','lacci_id','msisdn','brand','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','rev','trx','load_ts','load_user','site_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_site_id','lacci_node','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan','lacci_region_sales','lacci_branch','lacci_subbranch')

# ===============================> PROSES 
CHG_Daily = df_CHG.alias('a').join(df_LACCI.alias('b'), (col('a.subs_id') == col('b.subs_id')) & ( col('a.msisdn') == col('b.msisdn')),"outer")\
            .withColumn('msisdn', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.msisdn'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.msisdn'))
                                  .otherwise(col('b.msisdn')))\
            .withColumn('subs_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.subs_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.subs_id'))
                                  .otherwise(col('b.subs_id')))\
            .withColumn('chg_lacci_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_id'))
                                  .otherwise(col('b.chg_lacci_id')))\
            .withColumn('chg_area_name', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_area_name'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_area_name'))
                                  .otherwise(col('b.chg_area_name')))\
            .withColumn('chg_region_network', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_region_network'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_region_network'))
                                  .otherwise(col('b.chg_region_network')))\
            .withColumn('chg_cluster_sales', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_cluster_sales'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_cluster_sales'))
                                  .otherwise(col('b.chg_cluster_sales')))\
            .withColumn('chg_site_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_site_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_site_id'))
                                  .otherwise(col('b.chg_site_id')))\
            .withColumn('chg_node', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_node'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_node'))
                                  .otherwise(col('b.chg_node')))\
            .withColumn('chg_provinsi', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_provinsi'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_provinsi'))
                                  .otherwise(col('b.chg_provinsi')))\
            .withColumn('chg_kabupaten', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_kabupaten'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_kabupaten'))
                                  .otherwise(col('b.chg_kabupaten')))\
            .withColumn('chg_kecamatan', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_kecamatan'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_kecamatan'))
                                  .otherwise(col('b.chg_kecamatan')))\
            .withColumn('chg_kelurahan', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_kelurahan'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_kelurahan'))
                                  .otherwise(col('b.chg_kelurahan')))\
            .withColumn('chg_region_sales', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_region_sales'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_region_sales'))
                                  .otherwise(col('b.chg_region_sales')))\
            .withColumn('chg_branch', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_branch'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_branch'))
                                  .otherwise(col('b.chg_branch')))\
            .withColumn('chg_subbranch', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_subbranch'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), col('a.lacci_subbranch'))
                                  .otherwise(col('b.chg_subbranch')))\
            .withColumn('chg_flag', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), "1")
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "1")
                                  .otherwise(col('b.chg_flag'))).fillna("0",['chg_flag'])\
            .withColumn('usg_lacci_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_lacci_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_lacci_id')))\
            .withColumn('usg_area_name', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_area_name'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_area_name')))\
            .withColumn('usg_region_network', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_region_network'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_region_network')))\
            .withColumn('usg_cluster_sales', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_cluster_sales'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_cluster_sales')))\
            .withColumn('usg_site_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_site_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_site_id')))\
            .withColumn('usg_node', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_node'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_node')))\
            .withColumn('usg_provinsi', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_provinsi'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_provinsi')))\
            .withColumn('usg_kabupaten', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_kabupaten'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_kabupaten')))\
            .withColumn('usg_kecamatan', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_kecamatan'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_kecamatan')))\
            .withColumn('usg_kelurahan', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_kelurahan'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_kelurahan')))\
            .withColumn('usg_region_sales', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_region_sales'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_region_sales')))\
            .withColumn('usg_branch', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_branch'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_branch')))\
            .withColumn('usg_subbranch', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_subbranch'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.usg_subbranch')))\
            .withColumn('usg_flag', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.usg_flag'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "1")
                                  .otherwise(col('b.usg_flag'))).fillna("0",['usg_flag'])\
            .withColumn('ifrs_lacci_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_lacci_id')))\
            .withColumn('ifrs_area_name', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_area_name'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_area_name')))\
            .withColumn('ifrs_region_network', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_region_network'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_region_network')))\
            .withColumn('ifrs_cluster_sales', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_cluster_sales'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_cluster_sales')))\
            .withColumn('ifrs_site_id', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_site_id'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_site_id')))\
            .withColumn('ifrs_node', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_node'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_node')))\
            .withColumn('ifrs_provinsi', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_provinsi'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_provinsi')))\
            .withColumn('ifrs_kabupaten', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_kabupaten'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_kabupaten')))\
            .withColumn('ifrs_kecamatan', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_kecamatan'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_kecamatan')))\
            .withColumn('ifrs_kelurahan', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_kelurahan'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_kelurahan')))\
            .withColumn('ifrs_region_sales', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_region_sales'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_region_sales')))\
            .withColumn('ifrs_branch', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_branch'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_branch')))\
            .withColumn('ifrs_subbranch', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('a.lacci_subbranch'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "")
                                  .otherwise(col('b.ifrs_subbranch')))\
            .withColumn('ifrs_flag', 
                              F.when((col('a.subs_id') == col('b.subs_id')) & (col('a.msisdn') == col('b.msisdn')), col('b.ifrs_flag'))
                                .when((col('a.subs_id').isNotNull()) & (col('a.msisdn').isNotNull()), "1")
                                  .otherwise(col('b.ifrs_flag'))).fillna("0",['ifrs_flag'])\
            .select('msisdn','subs_id','chg_lacci_id','chg_area_name','chg_region_network','chg_cluster_sales','chg_site_id','chg_node','chg_provinsi','chg_kabupaten','chg_kecamatan','chg_kelurahan','chg_region_sales','chg_branch','chg_subbranch','chg_flag','usg_lacci_id','usg_area_name','usg_region_network','usg_cluster_sales','usg_site_id','usg_node','usg_provinsi','usg_kabupaten','usg_kecamatan','usg_kelurahan','usg_region_sales','usg_branch','usg_subbranch','usg_flag','ifrs_lacci_id','ifrs_area_name','ifrs_region_network','ifrs_cluster_sales','ifrs_site_id','ifrs_node','ifrs_provinsi','ifrs_kabupaten','ifrs_kecamatan','ifrs_kelurahan','ifrs_region_sales','ifrs_branch','ifrs_subbranch','ifrs_flag')\
            .withColumn('event_date', lit("20230120"))\
            .withColumn('event_date', lit("job_id"))\
            .write.csv("file:///home/hdoop/OUTPUT/hasilSA_OPAD_GOOD", sep ='~')

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)

# ===============================> CREATE FILELOG
app = "sa_opad"
nama_file = "/home/hdoop/LOG/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.writelines('\nINPUT_COUNT={}'.format(df_CHG.count()))
f.writelines('\nOUTPUT_COUNT={}'.format(CHG_Daily.count()))
f.close()
                            
                          
                          
