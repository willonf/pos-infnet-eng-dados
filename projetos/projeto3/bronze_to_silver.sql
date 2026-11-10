-- =====================================================================
-- Olist - Camada Silver - Transforms "SQL Query" do Glue Studio
-- Alias de entrada em cada job/nó: myDataSource
-- Carga completa: cada execução lê toda a Bronze e o destino sobrescreve.
--
-- SURROGATE KEYS
--   sk_* = row_number() ordenado pela chave natural (1..N).
--   Recalculada a cada carga completa. FKs seguem pelas chaves naturais
--   (order_id, product_id...) e viram sk_* na Gold via join com as dims.
--
-- SENTINELAS
--   texto ausente -> 'NAO_INFORMADO' | UF -> 'NI' | CEP -> '00000'
--   numérico ausente -> -1
--   data "ainda não ocorreu" -> 9999-12-31 00:00:00 (+ flag booleana)
--   data obrigatória ausente/inválida -> 1900-01-01 00:00:00
-- Todos os campos texto em MAIÚSCULO (inclusive IDs hex e sentinelas).
-- Datas de origem: America/Sao_Paulo -> UTC
-- try_cast / try_to_timestamp: valores inválidos viram NULL (modo ANSI).
-- =====================================================================


-- ---------------------------------------------------------------------
-- 0) olist_customers_dataset -> silver.customers
--    customer_unique_id se repete por design (mesma pessoa, vários pedidos).
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 1) olist_orders_dataset -> silver.orders (partição: year/month)
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 2) olist_order_items_dataset -> silver.order_items
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 3) olist_order_payments_dataset -> silver.order_payments
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 4) olist_order_reviews_dataset -> silver.order_reviews
--    Fonte CSV: habilitar multiline e quote '"' (comentários com \n).
--    review_id se repete entre pedidos: chave = (review_id, order_id).
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 5) olist_products_dataset -> silver.products
--    Corrige o typo de origem ("lenght" -> "length").
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 6) olist_sellers_dataset -> silver.sellers
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 7) olist_geolocation_dataset -> silver.geolocation
--    ~1M linhas com vários pontos por CEP: 1 linha por CEP,
--    descartando coordenadas fora do Brasil.
-- ---------------------------------------------------------------------
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


-- ---------------------------------------------------------------------
-- 8) product_category_name_translation -> silver.category_translation
--    O header do CSV pode vir com BOM (﻿): se a coluna não for
--    encontrada, renomeie no Change Schema antes deste nó.
-- ---------------------------------------------------------------------
select
  row_number() over (order by upper(trim(product_category_name))) as sk_category,
  upper(trim(product_category_name)) as product_category_name,
  coalesce(nullif(upper(trim(product_category_name_english)), ''), 'NOT_INFORMED') as product_category_name_english,
  current_timestamp() as _processed_at_utc
from myDataSource
where nullif(trim(product_category_name), '') is not null;