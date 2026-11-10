import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1794664680634 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_customers_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794664680634")

# Script generated for node Amazon S3
AmazonS3_node1794667247979 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_orders_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794667247979")

# Script generated for node Amazon S3
AmazonS3_node1794667402194 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_order_items_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794667402194")

# Script generated for node Amazon S3
AmazonS3_node1794667493591 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_order_payments_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794667493591")

# Script generated for node Amazon S3
AmazonS3_node1794667639701 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_order_reviews_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794667639701")

# Script generated for node Amazon S3
AmazonS3_node1794667728037 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_products_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794667728037")

# Script generated for node Amazon S3
AmazonS3_node1794668256060 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_sellers_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794668256060")

# Script generated for node Amazon S3
AmazonS3_node1794668325501 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/olist_geolocation_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794668325501")

# Script generated for node Amazon S3
AmazonS3_node1794668514673 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bronze-aws-glue-infnet/product_category_name_translation.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794668514673")

# Script generated for node SQL Query
SqlQuery0 = '''
select
  row_number() over (order by upper(trim(customer_id))) as sk_customer,
  upper(trim(customer_id)) as customer_id,
  coalesce(nullif(upper(trim(customer_unique_id)), ''), 'NAO_INFORMADO') as customer_unique_id,
  coalesce(nullif(lpad(trim(customer_zip_code_prefix), 5, '0'), ''), '00000') as customer_zip_code_prefix,
  coalesce(
    nullif(upper(translate(trim(customer_city),
      'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
      'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')), ''),
    'NAO_INFORMADO') as customer_city,
  coalesce(nullif(upper(trim(customer_state)), ''), 'NI') as customer_state,
  current_timestamp() as _processed_at_utc
from myDataSource
where nullif(trim(customer_id), '') is not null;
'''
SQLQuery_node1794665211118 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1794664680634}, transformation_ctx = "SQLQuery_node1794665211118")

# Script generated for node SQL Query
SqlQuery1 = '''
with base as (
  select
    upper(trim(order_id))    as order_id,
    upper(trim(customer_id)) as customer_id,
    coalesce(nullif(upper(trim(order_status)), ''), 'NAO_INFORMADO') as order_status,
    to_utc_timestamp(try_to_timestamp(nullif(trim(order_purchase_timestamp), ''),      'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo') as purchase_ts,
    to_utc_timestamp(try_to_timestamp(nullif(trim(order_approved_at), ''),             'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo') as approved_ts,
    to_utc_timestamp(try_to_timestamp(nullif(trim(order_delivered_carrier_date), ''),  'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo') as carrier_ts,
    to_utc_timestamp(try_to_timestamp(nullif(trim(order_delivered_customer_date), ''), 'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo') as delivered_ts,
    to_utc_timestamp(try_to_timestamp(nullif(trim(order_estimated_delivery_date), ''), 'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo') as estimated_ts
  from myDataSource
  where nullif(trim(order_id), '') is not null
)
select
  row_number() over (order by order_id) as sk_order,
  order_id,
  customer_id,
  order_status,
  coalesce(purchase_ts,  timestamp '1900-01-01 00:00:00') as order_purchase_ts_utc,
  coalesce(approved_ts,  timestamp '9999-12-31 00:00:00') as order_approved_ts_utc,
  coalesce(carrier_ts,   timestamp '9999-12-31 00:00:00') as order_delivered_carrier_ts_utc,
  coalesce(delivered_ts, timestamp '9999-12-31 00:00:00') as order_delivered_customer_ts_utc,
  coalesce(estimated_ts, timestamp '1900-01-01 00:00:00') as order_estimated_delivery_ts_utc,
  approved_ts  is not null as is_approved,
  delivered_ts is not null as is_delivered,
  coalesce(year(purchase_ts),  1900) as year,
  coalesce(month(purchase_ts), 1)    as month,
  current_timestamp() as _processed_at_utc
from base;
'''
SQLQuery_node1794667304912 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":AmazonS3_node1794667247979}, transformation_ctx = "SQLQuery_node1794667304912")

# Script generated for node SQL Query
SqlQuery2 = '''
with base as (
  select
    upper(trim(order_id)) as order_id,
    try_cast(order_item_id as int) as order_item_id,
    coalesce(nullif(upper(trim(product_id)), ''), 'NAO_INFORMADO') as product_id,
    coalesce(nullif(upper(trim(seller_id)),  ''), 'NAO_INFORMADO') as seller_id,
    coalesce(
      to_utc_timestamp(try_to_timestamp(nullif(trim(shipping_limit_date), ''), 'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo'),
      timestamp '1900-01-01 00:00:00') as shipping_limit_ts_utc,
    coalesce(try_cast(price         as decimal(12,2)), cast(-1 as decimal(12,2))) as price,
    coalesce(try_cast(freight_value as decimal(12,2)), cast(-1 as decimal(12,2))) as freight_value
  from myDataSource
  where nullif(trim(order_id), '') is not null
)
select
  row_number() over (order by order_id, order_item_id) as sk_order_item,
  order_id, order_item_id, product_id, seller_id,
  shipping_limit_ts_utc, price, freight_value,
  current_timestamp() as _processed_at_utc
from base
where order_item_id is not null;
'''
SQLQuery_node1794667433596 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource":AmazonS3_node1794667402194}, transformation_ctx = "SQLQuery_node1794667433596")

# Script generated for node SQL Query
SqlQuery3 = '''
with base as (
  select
    upper(trim(order_id)) as order_id,
    coalesce(try_cast(payment_sequential as int), -1) as payment_sequential,
    case
      when nullif(trim(payment_type), '') is null    then 'NAO_INFORMADO'
      when upper(trim(payment_type)) = 'NOT_DEFINED' then 'NAO_INFORMADO'
      else upper(trim(payment_type))
    end as payment_type,
    coalesce(try_cast(payment_installments as int), -1) as payment_installments,
    coalesce(try_cast(payment_value as decimal(12,2)), cast(-1 as decimal(12,2))) as payment_value
  from myDataSource
  where nullif(trim(order_id), '') is not null
)
select
  row_number() over (order by order_id, payment_sequential) as sk_order_payment,
  order_id, payment_sequential, payment_type, payment_installments, payment_value,
  current_timestamp() as _processed_at_utc
from base;
'''
SQLQuery_node1794667530218 = sparkSqlQuery(glueContext, query = SqlQuery3, mapping = {"myDataSource":AmazonS3_node1794667493591}, transformation_ctx = "SQLQuery_node1794667530218")

# Script generated for node SQL Query
SqlQuery4 = '''
with base as (
  select
    upper(trim(review_id)) as review_id,
    upper(trim(order_id))  as order_id,
    coalesce(try_cast(review_score as int), -1) as review_score,
    coalesce(nullif(upper(trim(review_comment_title)), ''), 'NAO_INFORMADO') as review_comment_title,
    coalesce(nullif(upper(regexp_replace(trim(review_comment_message), '[\\r\\n]+', ' ')), ''), 'NAO_INFORMADO') as review_comment_message,
    nullif(trim(review_comment_message), '') is not null as has_comment,
    coalesce(
      to_utc_timestamp(try_to_timestamp(nullif(trim(review_creation_date), ''), 'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo'),
      timestamp '1900-01-01 00:00:00') as review_creation_ts_utc,
    coalesce(
      to_utc_timestamp(try_to_timestamp(nullif(trim(review_answer_timestamp), ''), 'yyyy-MM-dd HH:mm:ss'), 'America/Sao_Paulo'),
      timestamp '9999-12-31 00:00:00') as review_answer_ts_utc
  from myDataSource
  where nullif(trim(review_id), '') is not null
    and nullif(trim(order_id),  '') is not null
)
select
  row_number() over (order by review_id, order_id) as sk_order_review,
  review_id, order_id, review_score,
  review_comment_title, review_comment_message, has_comment,
  review_creation_ts_utc, review_answer_ts_utc,
  current_timestamp() as _processed_at_utc
from base;
'''
SQLQuery_node1794667657832 = sparkSqlQuery(glueContext, query = SqlQuery4, mapping = {"myDataSource":AmazonS3_node1794667639701}, transformation_ctx = "SQLQuery_node1794667657832")

# Script generated for node SQL Query
SqlQuery5 = '''
with base as (
  select
    upper(trim(product_id)) as product_id,
    coalesce(nullif(upper(trim(product_category_name)), ''), 'NAO_INFORMADO') as product_category_name,
    coalesce(try_cast(product_name_lenght        as int), -1) as product_name_length,
    coalesce(try_cast(product_description_lenght as int), -1) as product_description_length,
    coalesce(try_cast(product_photos_qty         as int), -1) as product_photos_qty,
    coalesce(try_cast(product_weight_g           as int), -1) as product_weight_g,
    coalesce(try_cast(product_length_cm          as int), -1) as product_length_cm,
    coalesce(try_cast(product_height_cm          as int), -1) as product_height_cm,
    coalesce(try_cast(product_width_cm           as int), -1) as product_width_cm
  from myDataSource
  where nullif(trim(product_id), '') is not null
)
select
  row_number() over (order by product_id) as sk_product,
  product_id, product_category_name,
  product_name_length, product_description_length, product_photos_qty,
  product_weight_g, product_length_cm, product_height_cm, product_width_cm,
  current_timestamp() as _processed_at_utc
from base;
'''
SQLQuery_node1794667770028 = sparkSqlQuery(glueContext, query = SqlQuery5, mapping = {"myDataSource":AmazonS3_node1794667728037}, transformation_ctx = "SQLQuery_node1794667770028")

# Script generated for node SQL Query
SqlQuery6 = '''
select
  row_number() over (order by upper(trim(seller_id))) as sk_seller,
  upper(trim(seller_id)) as seller_id,
  coalesce(nullif(lpad(trim(seller_zip_code_prefix), 5, '0'), ''), '00000') as seller_zip_code_prefix,
  coalesce(
    nullif(upper(translate(trim(seller_city),
      'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
      'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')), ''),
    'NAO_INFORMADO') as seller_city,
  coalesce(nullif(upper(trim(seller_state)), ''), 'NI') as seller_state,
  current_timestamp() as _processed_at_utc
from myDataSource
where nullif(trim(seller_id), '') is not null;
'''
SQLQuery_node1794668292707 = sparkSqlQuery(glueContext, query = SqlQuery6, mapping = {"myDataSource":AmazonS3_node1794668256060}, transformation_ctx = "SQLQuery_node1794668292707")

# Script generated for node SQL Query
SqlQuery7 = '''
with base as (
  select
    lpad(trim(geolocation_zip_code_prefix), 5, '0') as zip_code_prefix,
    try_cast(geolocation_lat as double) as lat,
    try_cast(geolocation_lng as double) as lng,
    nullif(upper(translate(trim(geolocation_city),
      'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
      'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')), '') as city,
    nullif(upper(trim(geolocation_state)), '') as state
  from myDataSource
  where nullif(trim(geolocation_zip_code_prefix), '') is not null
)
select
  row_number() over (order by zip_code_prefix) as sk_geolocation,
  zip_code_prefix,
  coalesce(avg(case when lat between -34 and 6 and lng between -74 and -34 then lat end), -999) as geolocation_lat,
  coalesce(avg(case when lat between -34 and 6 and lng between -74 and -34 then lng end), -999) as geolocation_lng,
  coalesce(max(city),  'NAO_INFORMADO') as geolocation_city,
  coalesce(max(state), 'NI')            as geolocation_state,
  current_timestamp() as _processed_at_utc
from base
group by zip_code_prefix;
'''
SQLQuery_node1794668363159 = sparkSqlQuery(glueContext, query = SqlQuery7, mapping = {"myDataSource":AmazonS3_node1794668325501}, transformation_ctx = "SQLQuery_node1794668363159")

# Script generated for node SQL Query
SqlQuery8 = '''
select
  row_number() over (order by upper(trim(product_category_name))) as sk_category,
  upper(trim(product_category_name)) as product_category_name,
  coalesce(nullif(upper(trim(product_category_name_english)), ''), 'NOT_INFORMED') as product_category_name_english,
  current_timestamp() as _processed_at_utc
from myDataSource
where nullif(trim(product_category_name), '') is not null;
'''
SQLQuery_node1794668573726 = sparkSqlQuery(glueContext, query = SqlQuery8, mapping = {"myDataSource":AmazonS3_node1794668514673}, transformation_ctx = "SQLQuery_node1794668573726")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794665211118, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794665532018 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794665211118, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/customer/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794665532018")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794667304912, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794667369127 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794667304912, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/order/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794667369127")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794667433596, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794667461632 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794667433596, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/order_item/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794667461632")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794667530218, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794667557646 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794667530218, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/order_payment/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794667557646")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794667657832, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794667687821 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794667657832, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/order_review/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794667687821")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794667770028, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794667784095 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794667770028, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/product/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794667784095")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794668292707, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794668304328 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794668292707, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/seller/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794668304328")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794668363159, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794668490533 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794668363159, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/geolocation/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794668490533")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794668573726, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794664667013", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794668629259 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794668573726, connection_type="s3", format="glueparquet", connection_options={"path": "s3://silver-aws-glue-infnet/category/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794668629259")

job.commit()