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

# ===============================>READ DATA
#hv.sql('CREATE TABLE IF NOT EXISTS base.source_chg(timestamp string,a_party string,locationnumbertypea string,location_number_a string,b_party string,locationnumbertypeb string,location_number_b string,nrofreroutings string,imsi string,nrofnetworkreroutings string,redirectingpartyid string,precallduration string,callduration string,chargingid string,roamingflag string,vlr_num string,cell_id string,negotiated_qos string,requested_qos string,subscribed_qos string,dialled_apn string,event_type string,provider_id string,currency string,subscriber_status string,forwarding_flag string,first_call_flag string,camel_roaming string,time_zone string,account_balance string,account_delta string,account_number string,account_owner string,event_source string,guiding_resource_type string,rounded_duration string,total_volume string,rounded_total_volume string,event_start_period string,charge_including_free_allowance string,discount_amount string,free_total_volume string,free_duration string,call_direction string,charge_code string,special_number_ind string,bonus_information string,internalcause string,basiccause string,time_band string,call_type string,bonus_consumed string,vas_code string,service_filter string,national_calling_zone string,national_called_zone string,international_calling_zone string,international_called_zone string,credit_indicator string,event_id string,access_code string,country_name string,cp_name string,content_id string,rating_group string,bft_indicator string,unapplied_amount string,partition_id string,rateddatetime string,area string,cost_band string,rating_offer_id string,settlement_indicator string,tap_code string,mcc_mnc string,rating_pricing_item_id string,file_identifier string,record_number string,future_string1 string,future_string2 string,future_string3 string,allowance_consumed_revenue string,ifrs_ind_for_allowance_revenue string,ifrs_ind_for_vas_event string,itemized_bill_fee string,consumed_monetary_allowance string,partial_charge_ind string)row format delimited fields terminated by "|"')
#hv.sql("load data local inpath 'file:///home/hdoop/datayunita/USAGECHG/CHG' overwrite into table base.source_chg;")

#hv.sql('DROP TABLE base.chg_prep_mtd;')
#hv.sql('CREATE TABLE IF NOT EXISTS base.chg_prep_mtd(trx_date string,subs_id_mtd string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string,lacci_area_name string,lacci_region_network string,lacci_cluster_sales string,lacci_lac string,lacci_ci string,lacci_node string,lacci_region_sales string,lacci_branch string,lacci_subbranch string,lacci_provinsi string,lacci_kabupaten string,lacci_kecamatan string,lacci_kelurahan string) STORED AS PARQUET;')
#spark.read.parquet('file:///home/hdoop/datayunita/USAGECHG/prepMTD/*').write.mode('overwrite').saveAsTable("base.chg_prep_mtd")

#hv.sql('DROP TABLE base.chg_prep_mm;')
#hv.sql('CREATE TABLE IF NOT EXISTS base.chg_prep_mm(trx_date string,subs_id_mm string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string,lacci_area_name string,lacci_region_network string,lacci_cluster_sales string,lacci_lac string,lacci_ci string,lacci_node string,lacci_region_sales string,lacci_branch string,lacci_subbranch string,lacci_provinsi string,lacci_kabupaten string,lacci_kecamatan string,lacci_kelurahan string) STORED AS PARQUET;')
#spark.read.parquet('file:///home/hdoop/datayunita/USAGECHG/prepMM/*').write.mode('overwrite').saveAsTable("base.chg_prep_mm")

#hv.sql('CREATE TABLE IF NOT EXISTS base.chg_laccima(lac string,ci string,cell_name string,vendor string,site_id string,ne_id string,site_name string,msc_name string,node string,longitude string,latitude string,region_network string,provinsi string,kabupaten string,kecamatan string,kelurahan string,region_sales string,branch string,subbranch string,address string,cluster_sales string,mitra_ad string,cgi_pre string,cgi_post string,ocs_cluster string,ocs_zone string,area_sales string,eci string,lacci_id string,file_date string,file_id string,load_ts string,load_user string,event_date string)row format delimited fields terminated by ";"')
#hv.sql("load data local inpath 'file:///home/hdoop/datayunita/USAGECHG/LACCIMA_DIM_TERBARU.csv' overwrite into table base.chg_laccima;")

# Read data SUB_DIM
df_SUB_DIM = hv.table('base.subs_dim')\
            .select('msisdn','subs_id','cust_type_desc','cust_subtype_desc').toDF('msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc')

# READ MTD
df_MTD = hv.table('base.chg_prep_mtd')\
        .toDF('trx_date','subs_id_mtd','lacci_id','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','trx','dur','load_ts','load_user','event_date','job_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')\
        .select('subs_id_mtd','msisdn','lacci_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')

# READ MM
df_MM = hv.table('base.chg_prep_mm')\
        .toDF('trx_date','subs_id_mm','lacci_id_mm','msisdn','cust_type_desc','cust_subtype_desc','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','node_type','trx','dur','load_ts','load_user','event_date','job_id','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')\
        .select('subs_id_mm','msisdn','lacci_id_mm','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')

# READ DATA LACCIMA
df_LACCIMA = hv.table('base.chg_laccima')\
            .withColumn('substr_lacci_id_4', substring('lacci_id', -4,4))\
            .withColumn('substr_eci_4', substring('eci', -4,4))

df_ECI = df_LACCIMA.select('lac','ci','cell_name','vendor','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','substr_eci_4')\
                   .withColumn('concat_eci',F.concat(F.col('eci'), F.lit('_'), F.col('substr_eci_4')))

df_LACCI = df_LACCIMA.select('lac','ci','cell_name','vendor','node','longitude','latitude','region_network','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','mitra_ad','cgi_pre','cgi_post','ocs_cluster','ocs_zone','area_sales','eci','lacci_id','substr_lacci_id_4')\
                     .withColumn('concat_lacci_id',F.concat(F.col('lacci_id'), F.lit('_'), F.col('substr_lacci_id_4')))\
                     .fillna("UNKNOWN",['area_sales'])

# Read data SA_PAYR
df_CHG = hv.table('base.source_chg')\
         .withColumn('filename', lit("CHG_JKT_Pre_20230120235634_01172361_51.1_REG"))\
         .withColumn('rec_id',row_number().over(Window.orderBy(monotonically_increasing_id())))\
         .withColumn('timestamp',trim(col('timestamp')))\
         .withColumn('a_party',trim(col('a_party')))\
         .withColumn('account_delta',trim(col('account_delta')))\
         .withColumn('Flag',when(col('timestamp').isNull() & col('a_party').isNull() & col('account_delta').isNull(),'BAD').otherwise('GOOD'))

# SPLIT FILTER
df_CHGbad = df_CHG.filter(col('Flag') == 'BAD')
df_CHGgood  = df_CHG.filter(col('Flag')  == 'GOOD')\
                .withColumn('callduration',trim(col('callduration')))\
                .withColumn('chargingid',trim(col('chargingid')))\
                .withColumn('cell_id',trim(col('cell_id')))\
                .withColumn('account_balance',trim(col('account_balance')))\
                .withColumn('account_delta',trim(col('account_delta')))\
                .withColumn('rounded_duration',trim(col('rounded_duration')))\
                .withColumn('total_volume',trim(col('total_volume')))\
                .withColumn('rounded_total_volume',trim(col('rounded_total_volume')))\
                .withColumn('event_start_period',trim(col('event_start_period')))\
                .withColumn('unapplied_amount',trim(col('unapplied_amount')))\
                .withColumn('b_party', 
                              F.when(F.col('event_type') == "GPRS", "0")
                                .when((F.col('b_party').isNull()) | (F.col('b_party') == ""), "0")
                                  .otherwise(F.col('b_party')))\
                .withColumn('imsi', 
                              F.when(F.col('imsi').isNull(), "0")
                                  .otherwise(F.col('imsi')))\
                .withColumn('call_duration', 
                              F.when((F.col('callduration').isNull()) | (F.col('callduration') == ""), "0")
                                  .otherwise(F.col('callduration')))\
                .withColumn('charging_id', 
                              F.when((F.col('chargingid').isNull()) | (F.col('chargingid') == ""), "0")
                                  .otherwise(F.col('chargingid')))\
                .withColumn('roaming_flag', 
                              F.when((F.col('roamingflag') == "Y"), "1")
                                  .otherwise("0"))\
                .withColumn('account_balance', 
                              F.when((F.col('account_balance').isNull()) | (F.col('account_balance') == ""), "0")
                                  .otherwise(F.col('account_balance')))\
                .withColumn('rounded_duration', 
                              F.when((F.col('rounded_duration').isNull()) | (F.col('rounded_duration') == ""), "0")
                                  .otherwise(F.col('rounded_duration')))\
                .withColumn('rev', 
                              F.when((F.col('account_delta').isNull()), "0")
                                .when((F.col('credit_indicator') == "C"), F.col('account_delta')*-1)
                                  .otherwise(F.col('account_delta')))\
                .withColumn('cifa', 
                              F.when((F.col('charge_including_free_allowance').isNull()) | (trim(col('charge_including_free_allowance')) == ""), "0")
                                  .otherwise(F.col('charge_including_free_allowance')))\
                .withColumn('key_b_party', 
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
                                  .otherwise(0))\
                .fillna("0",['unapplied_amount'])\
                .withColumn('brand', lit("byU"))\
                .withColumn('load_user', lit("solusi247"))\
                .withColumn('pre_post_flag', lit("1"))\
                .withColumn('retry_count', lit("0"))\
                .withColumn('mcc', substring('mcc_mnc', 1,3))\
                .withColumn('mnc',F.expr("substring(mcc_mnc,4,length(mcc_mnc)-5)"))\
                .withColumn('offer_id',F.expr("substring(bonus_consumed,0,instr(bonus_consumed,'#')-1)"))\
                .withColumn('network_activity_id',F.concat(F.col('timestamp'), F.col('event_id'), F.col('account_delta')))\
                .withColumn('trim_cell_id',trim(col('cell_id')))\
                .withColumn('x1', substring('trim_cell_id', 11,7))\
                .withColumn('x2', substring('trim_cell_id',18,3))\
                .withColumn('x3', substring('trim_cell_id', 6,5))\
                .withColumn('x4', substring('trim_cell_id',11,5))\
                .withColumn('t1',F.concat(F.col('x1'),F.lit('|'), F.col('x2')))\
                .withColumn('t2',F.concat(F.col('x3'),F.lit('|'), F.col('x4')))\
                .withColumn('t1', F.regexp_replace('t1', r'^0+', ''))\
                .withColumn('t2', F.regexp_replace('t2', r'^0+', ''))\
                .withColumn('trx_lacci', 
                      F.when((F.col('future_string3') == "129") | (F.col('future_string3') == "130"), F.col('t1'))
                          .otherwise(F.col('t2')))\
                .withColumn('x1', substring('cell_id', 11,7))\
                .withColumn('x2',F.expr("substring(cell_id,18,length(cell_id)-19)"))\
                .withColumn('x3', substring('cell_id', 6,5))\
                .withColumn('x4',F.expr("substring(cell_id,11,length(cell_id)-12)"))\
                .withColumn('c1',F.concat(F.col('x1'),F.lit('_'), F.col('x2')))\
                .withColumn('c2',F.concat(F.col('x3'),F.lit('_'), F.col('x4')))\
                .withColumn('cell_id', 
                      F.when((F.col('future_string3') == "129") | (F.col('future_string3') == "130"), F.col('c1'))
                          .otherwise(F.col('c2')))\
                .withColumn('timestamp_r', to_timestamp(col('timestamp'), "yyyyMMddHHmmss"))\
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
                .withColumn('number_of_reroutings', lit("0"))\
                .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','a_party','location_number_type_a','location_number_a','b_party','locationnumbertypeb','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internalcause','basiccause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rateddatetime','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')\
                .toDF('timestamp_r','trx_date','trx_hour','msisdn','account_owner','a_party','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

# JOIN
SA_USAGE_CHG_1 = df_CHGgood.join(df_SUB_DIM,col('msisdn') == col('msisdn_sub'), how = 'left')\
                      .withColumn('subs_id', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('subs_id'))
                                  .otherwise("-99"))\
                      .withColumn('cust_type_desc', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('cust_type_desc'))
                                  .otherwise("-99"))\
                      .withColumn('cust_subtype_desc', 
                              F.when(F.col('msisdn') == F.col('msisdn_sub'), F.col('cust_subtype_desc'))
                                  .otherwise("-99"))\
                      .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')\
                      .toDF('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')\
                      .join(df_MTD, (col('subs_id') == col('subs_id_mtd')) & ( col('msisdn_sub') == col('msisdn')),"left")\
                      .withColumn('mtd_lacci_id', 
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
                                  .otherwise("0"))\
                      .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn_sub','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag')\
                      .join(df_MM, (col('subs_id') == col('subs_id_mm')) & ( col('msisdn_sub') == col('msisdn')),"left")\
                      .withColumn('mm_lacci_id', 
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
                                  .otherwise(F.col('trx_lacci')))\
                      .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag','mm_lacci_id','mm_area_name','mm_region_network','mm_cluster_sales','mm_lac','mm_ci','mm_node','mm_region_sales','mm_branch','mm_subbranch','mm_provinsi','mm_kabupaten','mm_kecamatan','mm_kelurahan','mm_flag')\
                      .withColumn('substr_trx_lacci_4', substring('trx_lacci', -4,4))\
                      .withColumn('concat_trx_lacci',F.concat(F.col('trx_lacci'), F.lit('_'), F.col('substr_trx_lacci_4')))\
                      .withColumn('Flag',when((col('future_string3') == "129") | (col('future_string3') == "130"),'Y').otherwise('X'))

# SPLIT FILTER GOOD join ECI
dfJOINeci  = SA_USAGE_CHG_1.filter(col('Flag')  == 'Y')\
                .join(df_ECI,col('concat_trx_lacci') ==  col('concat_eci'), how = 'left')\
                .withColumn('lacci_id', 
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
                                  .otherwise("NQ"))\
                    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','filename','rec_id','event_date','productline_bonus_l4','retry_count','retry_ts','status_data','status','trx_lacci')

# SPLIT FILTER REJECT join LACCI
dfJOINlacci = SA_USAGE_CHG_1.filter(col('Flag') == 'X')\
                    .join(df_LACCI,col('concat_trx_lacci') ==  col('concat_lacci_id'), how = 'left')\
                    .withColumn('lacci_id', 
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
                                  .otherwise("NQ"))\
                    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','event_type','provider_id','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','site_name','pre_post_flag','mcc','mnc','network_activity_id','filename','rec_id','event_date','productline_bonus_l4','retry_count','retry_ts','status_data','status','trx_lacci')

# MERGE 
df = [ dfJOINeci,dfJOINlacci ]
CHG_Part1 = reduce(DataFrame.unionAll, df)\
            .withColumn('trx_lacci', 
                    F.when(F.col('trx_lacci').like("%a%"), "")
                        .otherwise(F.col('trx_lacci')))\
            .write.mode('overwrite').saveAsTable("output.sa_usage_chg1")

# CEK
print("JOIN ECI=", dfJOINeci.count(), "JOIN LACCI=", dfJOINlacci.count())

end_date = datetime.today() + timedelta(hours=7)
duration = (end_date - start_date)
print("start date:",start_date," end date:",end_date," duration:",duration)

# CREATE FILELOG
app = "chg_hive1"
nama_file = "/home/hdoop/BelajarPyspark/"+app+"_"+run_date+".log"
f = open(nama_file, "w")
f.writelines('\nSTART_DATE={}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nEND_DATE={}'.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
f.writelines('\nDURATION={}'.format(duration))
f.close()
