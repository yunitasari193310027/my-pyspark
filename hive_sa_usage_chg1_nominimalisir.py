from pyspark.sql import SparkSession
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from functools import reduce
from datetime import datetime, timedelta
from pyspark.sql.functions import monotonically_increasing_id,row_number
from pyspark.sql.window import Window 
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
run_date = datetime.now().strftime('%Y%m%d%H%M%S')

# Read data SA_PAYR
df_CHG = hv.table('base.source_chg')
df_CHG = df_CHG.toDF('timestamp','a_party','locationnumbertypea','location_number_a','b_party','locationnumbertypeb','location_number_b','nrofreroutings','imsi','nrofnetworkreroutings','redirectingpartyid','precallduration','callduration','chargingid','roamingflag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_delta','account_number','account_owner','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','charge_including_free_allowance','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internalcause','basiccause','time_band','call_type','bonus_consumed','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','credit_indicator','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rateddatetime','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind')

# get filename trx_date trx_hour event_date brand site_name pre_post_flag
df_CHG = df_CHG.withColumn('filename', lit("CHG_JKT_Pre_20230120235634_01172361_51.1_REG"))\
                .withColumn('rec_id',row_number().over(Window.orderBy(monotonically_increasing_id())))\
                 .withColumn('timestamp',trim(col('timestamp')))\
                  .withColumn('a_party',trim(col('a_party')))\
                   .withColumn('account_delta',trim(col('account_delta')))

df_CHG = df_CHG.withColumn('Flag',when(col('timestamp').isNull() & col('a_party').isNull() & col('account_delta').isNull(),'BAD').otherwise('GOOD'))
df_CHGgood  = df_CHG.filter(col('Flag')  == 'GOOD')
df_CHGbad = df_CHG.filter(col('Flag') == 'BAD')

# TRIM 
df_USAGE_CHG = df_CHGgood.withColumn('callduration',trim(col('callduration')))\
                          .withColumn('chargingid',trim(col('chargingid')))\
                           .withColumn('cell_id',trim(col('cell_id')))\
                            .withColumn('account_balance',trim(col('account_balance')))\
                             .withColumn('account_delta',trim(col('account_delta')))\
                              .withColumn('rounded_duration',trim(col('rounded_duration')))\
                               .withColumn('total_volume',trim(col('total_volume')))\
                                .withColumn('rounded_total_volume',trim(col('rounded_total_volume')))\
                                 .withColumn('event_start_period',trim(col('event_start_period')))\
                                  .withColumn('unapplied_amount',trim(col('unapplied_amount')))
# SWITCH CASE COLUMN
df_USAGE_CHG = df_USAGE_CHG.withColumn('b_party', 
                              F.when(F.col('event_type') == "GPRS", "0")
                                .when((F.col('b_party').isNull()) | (F.col('b_party') == ""), "0")
                                  .otherwise(F.col('b_party')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('imsi', 
                              F.when(F.col('imsi').isNull(), "0")
                                  .otherwise(F.col('imsi')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('call_duration', 
                              F.when((F.col('callduration').isNull()) | (F.col('callduration') == ""), "0")
                                  .otherwise(F.col('callduration')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('charging_id', 
                              F.when((F.col('chargingid').isNull()) | (F.col('chargingid') == ""), "0")
                                  .otherwise(F.col('chargingid')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('roaming_flag', 
                              F.when((F.col('roamingflag') == "Y"), "1")
                                  .otherwise("0"))
df_USAGE_CHG = df_USAGE_CHG.withColumn('account_balance', 
                              F.when((F.col('account_balance').isNull()) | (F.col('account_balance') == ""), "0")
                                  .otherwise(F.col('account_balance')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('rounded_duration', 
                              F.when((F.col('rounded_duration').isNull()) | (F.col('rounded_duration') == ""), "0")
                                  .otherwise(F.col('rounded_duration')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('rev', 
                              F.when((F.col('account_delta').isNull()), "0")
                                .when((F.col('credit_indicator') == "C"), F.col('account_delta')*-1)
                                  .otherwise(F.col('account_delta')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('cifa', 
                              F.when((F.col('charge_including_free_allowance').isNull()) | (trim(col('charge_including_free_allowance')) == ""), "0")
                                  .otherwise(F.col('charge_including_free_allowance')))
# SWITCH CASE KEY
df_USAGE_CHG = df_USAGE_CHG.withColumn('key_b_party', 
                              F.when((F.col('service_filter') == "GPRS"), "")
                                  .otherwise(F.col('b_party')))\
                            .withColumn('key_call_duration', 
                              F.when((F.col('service_filter') == "GPRS"), "")
                                  .otherwise(F.col('callduration')))\
                            .withColumn('key_charging_id', 
                              F.when((F.col('service_filter') == "GPRS"), F.col('chargingid'))
                                  .otherwise(""))\
                            .withColumn('key_total_volume', 
                              F.when((F.col('service_filter') == "GPRS"), F.col('total_volume'))
                                  .otherwise(0))

# NVL
df_USAGE_CHG = df_USAGE_CHG.fillna("0",['unapplied_amount'])

# CONSTANT VALUE
df_USAGE_CHG = df_USAGE_CHG.withColumn('brand', lit("byU"))\
                            .withColumn('load_user', lit("solusi247"))\
                             .withColumn('pre_post_flag', lit("1"))\
                              .withColumn('retry_count', lit("0"))
# SUBSTRING
df_USAGE_CHG = df_USAGE_CHG.withColumn('mcc', substring('mcc_mnc', 1,3))\
                            .withColumn('mnc',F.expr("substring(mcc_mnc,4,length(mcc_mnc)-5)"))\
                             .withColumn('offer_id',F.expr("substring(bonus_consumed,0,instr(bonus_consumed,'#')-1)"))

# CONCAT
df_USAGE_CHG = df_USAGE_CHG.withColumn('network_activity_id',F.concat(F.col('timestamp'), F.col('event_id'), F.col('account_delta')))             

# PROSES TRX
df_USAGE_CHG = df_USAGE_CHG.withColumn('trim_cell_id',trim(col('cell_id')))
df_USAGE_CHG = df_USAGE_CHG.withColumn('x1', substring('trim_cell_id', 11,7))\
                            .withColumn('x2', substring('trim_cell_id',18,3))\
                              .withColumn('x3', substring('trim_cell_id', 6,5))\
                                .withColumn('x4', substring('trim_cell_id',11,5))

df_USAGE_CHG = df_USAGE_CHG.withColumn('t1',F.concat(F.col('x1'),F.lit('|'), F.col('x2')))\
  .withColumn('t2',F.concat(F.col('x3'),F.lit('|'), F.col('x4')))

df_USAGE_CHG = df_USAGE_CHG.withColumn('t1', F.regexp_replace('t1', r'^0+', ''))\
                            .withColumn('t2', F.regexp_replace('t2', r'^0+', ''))

df_USAGE_CHG = df_USAGE_CHG.withColumn('trx_lacci', 
                      F.when((F.col('future_string3') == "129") | (F.col('future_string3') == "130"), F.col('t1'))
                          .otherwise(F.col('t2')))
# CEK
#df_USAGE_CHG.select('cell_id').distinct().show()
#df_USAGE_CHG.select('trx_lacci').distinct().show()

# PROSES CELL_ID
df_USAGE_CHG = df_USAGE_CHG.withColumn('x1', substring('cell_id', 11,7))\
                            .withColumn('x2',F.expr("substring(cell_id,18,length(cell_id)-19)"))\
                              .withColumn('x3', substring('cell_id', 6,5))\
                                .withColumn('x4',F.expr("substring(cell_id,11,length(cell_id)-12)"))

df_USAGE_CHG = df_USAGE_CHG.withColumn('c1',F.concat(F.col('x1'),F.lit('_'), F.col('x2')))\
  .withColumn('c2',F.concat(F.col('x3'),F.lit('_'), F.col('x4')))

df_USAGE_CHG = df_USAGE_CHG.withColumn('cell_id', 
                      F.when((F.col('future_string3') == "129") | (F.col('future_string3') == "130"), F.col('c1'))
                          .otherwise(F.col('c2')))

# ADD COLUMN 
cur_date = (datetime.now()).strftime('%Y-%m-%d')
cur_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
df_USAGE_CHG = df_USAGE_CHG.withColumn('timestamp_r', to_timestamp(df_USAGE_CHG.timestamp, "yyyyMMddHHmmss"))\
                            .withColumn('trx_date', substring('timestamp_r',0,10))\
                            .withColumn('trx_hour', substring('timestamp_r',11,2))\
                            .withColumn('event_date', substring('timestamp_r',0,10))\
                            .withColumn('location_number_type_a', lit(""))\
                            .withColumn('number_of_network_reroutings', lit(""))\
                            .withColumn('redirecting_party_id', lit(""))\
                            .withColumn('pre_call_duration', lit(""))\
                            .withColumn('file_id', lit(""))\
                            .withColumn("load_ts", lit(cur_time))\
                            .withColumn('site_name', lit("JKT"))\
                            .withColumn('productline_bonus_l4', lit(""))\
                            .withColumn('retry_ts', lit("20230120"))\
                            .withColumn('status_data', lit(""))\
                            .withColumn('status', lit("NODUP"))\
                            .withColumn('number_of_reroutings', lit("0"))

df_USAGE_CHG = df_USAGE_CHG.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','a_party','location_number_type_a','location_number_a','b_party','locationnumbertypeb','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internalcause','basiccause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rateddatetime','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')
df_USAGE_CHG = df_USAGE_CHG.toDF('timestamp_r','trx_date','trx_hour','msisdn','account_owner','a_party','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

#CEK

#df_USAGE_CHG.select('cell_id','trx_lacci','timestamp_r','trx_date','trx_hour').show()


# Read data SUB_DIM
df_SUB_DIM = hv.table('base.subs_dim')
df_SUB_DIM = df_SUB_DIM.select('msisdn','subs_id','cust_type_desc','cust_subtype_desc')
df_SUB_DIM = df_SUB_DIM.toDF('msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc')
# JOIN RCG & SUB_DIM
dfRCG_SUB = df_USAGE_CHG.join(df_SUB_DIM,df_USAGE_CHG.msisdn ==  df_SUB_DIM.msisdn_sub, how = 'left')

# SWITHCASE 
dfRCG_SUB = dfRCG_SUB.withColumn('subs_id', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('subs_id'))
                                  .otherwise("-99"))\
                      .withColumn('cust_type_desc', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('cust_type_desc'))
                                  .otherwise("-99"))\
                      .withColumn('cust_subtype_desc', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('cust_subtype_desc'))
                                  .otherwise("-99"))

dfRCG_SUB = dfRCG_SUB.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')
dfRCG_SUB = dfRCG_SUB.toDF('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

# READ MTD
df_MTD = hv.table('base.chg_prep_mtd')
df_MTD = df_MTD.toDF('trx_date','subs_id_mtd','lacci_id','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','trx','dur','load_ts','load_user','event_date','job_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')
df_MTD = df_MTD.select('subs_id_mtd','msisdn','lacci_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')

# Using multiple columns on join expression
dfJOIN_MTD = dfRCG_SUB.join(df_MTD, (dfRCG_SUB["subs_id"] == df_MTD["subs_id_mtd"]) &
   ( dfRCG_SUB["msisdn_sub"] == df_MTD["msisdn"]),"left")

# SWITHCASE 
dfJOIN_MTD = dfJOIN_MTD.withColumn('mtd_lacci_id', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_id'))
                                  .otherwise("-99"))\
                      .withColumn('mtd_area_name', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_area_name'))
                                  .otherwise("UNKNOWN"))\
                      .withColumn('mtd_region_network', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_region_network'))
                                  .otherwise(""))\
                      .withColumn('mtd_cluster_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_cluster_sales'))
                                  .otherwise(""))\
                      .withColumn('mtd_lac', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_lac'))
                                  .otherwise(""))\
                      .withColumn('mtd_ci', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_ci'))
                                  .otherwise(""))\
                      .withColumn('mtd_node', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_node'))
                                  .otherwise(""))\
                      .withColumn('mtd_region_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_region_sales'))
                                  .otherwise(""))\
                      .withColumn('mtd_branch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_branch'))
                                  .otherwise(""))\
                      .withColumn('mtd_subbranch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_subbranch'))
                                  .otherwise(""))\
                      .withColumn('mtd_provinsi', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_provinsi'))
                                  .otherwise("-99"))\
                      .withColumn('mtd_kabupaten', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_kabupaten'))
                                  .otherwise(""))\
                      .withColumn('mtd_kecamatan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_kecamatan'))
                                  .otherwise(""))\
                      .withColumn('mtd_kelurahan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), F.col('lacci_kelurahan'))
                                  .otherwise(""))\
                      .withColumn('mtd_flag', 
                              F.when(F.col('subs_id') == F.col('subs_id_mtd'), "1")
                                  .otherwise("0"))


dfJOIN_MTD = dfJOIN_MTD.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag')

# READ MM
df_MM = hv.table('base.chg_prep_mm')
df_MM = df_MM.toDF('trx_date','subs_id_mm','lacci_id_mm','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','trx','dur','load_ts','load_user','event_date','job_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')
df_MM = df_MM.select('subs_id_mm','msisdn','lacci_id_mm','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')

# Using multiple columns on join expression
dfJOIN_MM = dfJOIN_MTD.join(df_MM, (dfJOIN_MTD["subs_id"] == df_MM["subs_id_mm"]) &
   ( dfJOIN_MTD["msisdn_sub"] == df_MM["msisdn"]),"left")

# CEK
print("JOIN SUB DIM=",dfRCG_SUB.count(), " JOIN MTD=", dfJOIN_MTD.count(),"JOIN MM=", dfJOIN_MM.count())

dfJOIN_MM = dfJOIN_MM.withColumn('mm_lacci_id', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_id_mm'))
                                  .otherwise("-99"))\
                      .withColumn('mm_area_name', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_area_name'))
                                  .otherwise("UNKNOWN"))\
                      .withColumn('mm_region_network', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_region_network'))
                                  .otherwise(""))\
                      .withColumn('mm_cluster_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_cluster_sales'))
                                  .otherwise(""))\
                      .withColumn('mm_lac', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_lac'))
                                  .otherwise(""))\
                      .withColumn('mm_ci', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_ci'))
                                  .otherwise(""))\
                      .withColumn('mm_node', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_node'))
                                  .otherwise(""))\
                      .withColumn('mm_region_sales', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_region_sales'))
                                  .otherwise(""))\
                      .withColumn('mm_branch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_branch'))
                                  .otherwise(""))\
                      .withColumn('mm_subbranch', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_subbranch'))
                                  .otherwise(""))\
                      .withColumn('mm_provinsi', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_provinsi'))
                                  .otherwise("-99"))\
                      .withColumn('mm_kabupaten', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_kabupaten'))
                                  .otherwise(""))\
                      .withColumn('mm_kecamatan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_kecamatan'))
                                  .otherwise(""))\
                      .withColumn('mm_kelurahan', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), F.col('lacci_kelurahan'))
                                  .otherwise(""))\
                      .withColumn('mm_flag', 
                              F.when(F.col('subs_id') == F.col('subs_id_mm'), "1")
                                  .otherwise("0"))\
                      .withColumn('counter',row_number().over(Window.orderBy(monotonically_increasing_id())))\
                      .withColumn('trx_lacci', 
                              F.when((F.col('trx_lacci').isNull()) | (F.col('trx_lacci') == "|") | (F.col('trx_lacci') == ""), (concat(lit('a'),col('counter'))))
                                  .otherwise(F.col('trx_lacci')))
dfJOIN_MM = dfJOIN_MM.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag','mm_lacci_id','mm_area_name','mm_region_network','mm_cluster_sales','mm_lac','mm_ci','mm_node','mm_region_sales','mm_branch','mm_subbranch','mm_provinsi','mm_kabupaten','mm_kecamatan','mm_kelurahan','mm_flag')\
                      .withColumn('substr_trx_lacci_4', substring('trx_lacci', -4,4))\
                      .withColumn('concat_trx_lacci',F.concat(F.col('trx_lacci'), F.lit('_'), F.col('substr_trx_lacci_4')))

dfJOIN_MM = dfJOIN_MM.withColumn('Flag',when((col('future_string3') == "129") | (col('future_string3') == "130"),'Y').otherwise('X'))
df_GOODfuture  = dfJOIN_MM.filter(col('Flag')  == 'Y')
df_REJECTfuture = dfJOIN_MM.filter(col('Flag') == 'X')

# READ DATA LACCIMA
df_LACCIMA = hv.table('base.chg_laccima')
df_LACCIMA = df_LACCIMA.toDF('lac','ci','cell_name','vendor','site_id','ne_id','site_name','msc_name','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','file_date','file_id','load_ts','load_user','event_date')

df_LACCIMA = df_LACCIMA.withColumn('substr_lacci_id_4', substring('lacci_id', -4,4))\
                        .withColumn('substr_eci_4', substring('eci', -4,4))

df_ECI = df_LACCIMA.select('lac','ci','cell_name','vendor','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','substr_eci_4')\
                   .withColumn('concat_eci',F.concat(F.col('eci'), F.lit('_'), F.col('substr_eci_4')))

df_LACCI = df_LACCIMA.select('lac','ci','cell_name','vendor','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','substr_lacci_id_4')\
                     .withColumn('concat_lacci_id',F.concat(F.col('lacci_id'), F.lit('_'), F.col('substr_lacci_id_4')))
df_LACCI = df_LACCI.fillna("UNKNOWN",['area_sales'])

# JOIN ECI
dfJOINeci = df_GOODfuture.join(df_ECI,df_GOODfuture.concat_trx_lacci ==  df_ECI.concat_eci, how = 'left')
dfJOINeci = dfJOINeci.withColumn('lacci_id', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('lacci_id'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lacci_id'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lacci_id'))
                                  .otherwise("-99"))\
                    .withColumn('lacci_closing_flag', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), "0")
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), "1")
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), "2")
                                  .otherwise("-1"))\
                    .withColumn('lac', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('lac'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lac'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lac'))
                                  .otherwise("NQ"))\
                    .withColumn('ci', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('ci'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_ci'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_ci'))
                                  .otherwise("NQ"))\
                    .withColumn('node_type', 
                              F.when((F.col('trx_lacci') == F.col('eci')) & (F.col('area_sales') != "UNKNOWN"), F.col('node'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_node'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_node'))
                                  .otherwise("NQ"))\
                    .withColumn('area_sales', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('area_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_area_name'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_area_name'))
                                  .otherwise("NQ"))\
                    .withColumn('region_sales', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('region_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_region_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_region_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('branch', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('branch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_branch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_branch'))
                                  .otherwise("NQ"))\
                    .withColumn('subbranch', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('subbranch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_subbranch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_subbranch'))
                                  .otherwise("NQ"))\
                    .withColumn('cluster_sales', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('cluster_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_cluster_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_cluster_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('provinsi', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('provinsi'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_provinsi'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_provinsi'))
                                  .otherwise("NQ"))\
                    .withColumn('kabupaten', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('kabupaten'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kabupaten'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kabupaten'))
                                  .otherwise("NQ"))\
                    .withColumn('kecamatan', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('kecamatan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kecamatan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kecamatan'))
                                  .otherwise("NQ"))\
                    .withColumn('kelurahan', 
                              F.when(F.col('trx_lacci') == F.col('eci'), F.col('kelurahan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kelurahan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kelurahan'))
                                  .otherwise("NQ"))

# JOIN LACCI
dfJOINlacci = df_REJECTfuture.join(df_LACCI,df_REJECTfuture.concat_trx_lacci ==  df_LACCI.concat_lacci_id, how = 'left')
dfJOINlacci = dfJOINlacci.withColumn('lacci_id', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('lacci_id'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lacci_id'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lacci_id'))
                                  .otherwise("-99"))\
                    .withColumn('lacci_closing_flag', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), "0")
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), "1")
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), "2")
                                  .otherwise("-1"))\
                    .withColumn('lac', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('lac'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_lac'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_lac'))
                                  .otherwise("NQ"))\
                    .withColumn('ci', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('ci'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_ci'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_ci'))
                                  .otherwise("NQ"))\
                    .withColumn('node_type', 
                              F.when((F.col('trx_lacci') == F.col('lacci_id')) & (F.col('area_sales') != "UNKNOWN"), F.col('node'))
                               .when((F.col('mtd_flag') == "1") & (F.col('mtd_area_name') != "UNKNOWN"), F.col('mtd_node'))
                               .when((F.col('mm_flag') == "1") & (F.col('mm_area_name') != "UNKNOWN"), F.col('mm_node'))
                                  .otherwise("NQ"))\
                    .withColumn('area_sales', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('area_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_area_name'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_area_name'))
                                  .otherwise("NQ"))\
                    .withColumn('region_sales', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('region_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_region_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_region_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('branch', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('branch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_branch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_branch'))
                                  .otherwise("NQ"))\
                    .withColumn('subbranch', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('subbranch'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_subbranch'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_subbranch'))
                                  .otherwise("NQ"))\
                    .withColumn('cluster_sales', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('cluster_sales'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_cluster_sales'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_cluster_sales'))
                                  .otherwise("NQ"))\
                    .withColumn('provinsi', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('provinsi'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_provinsi'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_provinsi'))
                                  .otherwise("NQ"))\
                    .withColumn('kabupaten', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('kabupaten'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kabupaten'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kabupaten'))
                                  .otherwise("NQ"))\
                    .withColumn('kecamatan', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('kecamatan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kecamatan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kecamatan'))
                                  .otherwise("NQ"))\
                    .withColumn('kelurahan', 
                              F.when(F.col('trx_lacci') == F.col('lacci_id'), F.col('kelurahan'))
                               .when(F.col('mtd_flag') == "1" , F.col('mtd_kelurahan'))
                               .when(F.col('mm_flag') == "1" , F.col('mm_kelurahan'))
                                  .otherwise("NQ"))
dfJOINeci = dfJOINeci.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','filename','rec_id','event_date','productline_bonus_l4','retry_count','retry_ts','status_data','status','trx_lacci')
dfJOINlacci = dfJOINlacci.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','filename','rec_id','event_date','productline_bonus_l4','retry_count','retry_ts','status_data','status','trx_lacci')

# MERGE 
df = [ dfJOINeci,dfJOINlacci ]
dfMerge = reduce(DataFrame.unionAll, df)

df_CHG_Part1 = dfMerge.withColumn('trx_lacci', 
                              F.when(F.col('trx_lacci').like("%a%"), "")
                                  .otherwise(F.col('trx_lacci')))
# CEK
dfJOINeci.where("trx_lacci == eci").show()
dfJOINlacci.where("trx_lacci == lacci_id").show()
#dfJOINeci.select('cell_id','trx_lacci').distinct().show()
print("JOIN SUB DIM=",dfRCG_SUB.count(), " JOIN MTD=", dfJOIN_MTD.count(),"JOIN MM=", dfJOIN_MM.count())
print("JOIN ECI=", dfJOINeci.count(), "JOIN LACCI=", dfJOINlacci.count())
#print("SPLIT FILTER Y =", df_GOODfuture.count(), " dan filter X", df_REJECTfuture.count())

df_GOODfuture
# WRITE SA_USAGE_CHG Part1 to csv
df_CHG_Part1.write.mode('overwrite').saveAsTable("output.sa_usage_chg1_nonminimalisir")


end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)

# CREATE FILELOG
app = "chg_hive1_nominimalisir"
nama_file = "/home/hdoop/BelajarPyspark/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.close()

