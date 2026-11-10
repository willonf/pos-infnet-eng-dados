-- =====================================================================
-- Olist - Camada Gold - Modelo Estrela
-- Transforms "SQL Query" do Glue Studio (um nó por tabela de destino)
--
-- ORDEM DE EXECUÇÃO (Workflow)
--   1) Job dimensões : dim_tempo, dim_cliente, dim_produto,
--                      dim_vendedor, dim_tipo_pagamento
--   2) Job fatos     : fato_itens_pedido, fato_pagamentos, fato_pedidos
--      (os fatos leem as dimensões já gravadas na Gold para resolver SKs)
--
-- ALIASES
--   Cada bloco lista os aliases que devem ser configurados no nó
--   SQL Query (Input sources -> SQL aliases). Silver = olist_silver.*,
--   Gold = olist_gold.*.
--
-- CONVENÇÕES
--   - Membro desconhecido: sk = -1 em todas as dimensões; o fato usa
--     coalesce(sk, -1) quando o join não encontra correspondência.
--   - dim_tempo: sk_tempo = yyyyMMdd (int), no horário de Brasília.
--     Sentinelas da Silver: 1900-01-01 -> 19000101 (não informado)
--                           9999-12-31 -> 99991231 (ainda não ocorreu)
--   - Medidas inexistentes ficam NULL (não -1) para não distorcer médias.
--   - Destino: Parquet + Snappy, overwrite. Fatos particionados por
--     year/month (data da compra).
-- =====================================================================


-- =====================================================================
-- DIMENSÕES
-- =====================================================================

-- ---------------------------------------------------------------------
-- D1) dim_tempo
--     Aliases: orders (silver)
--     Gera um dia por linha entre a menor e a maior data relevante.
-- ---------------------------------------------------------------------
with datas_pedido as (
  select to_date(from_utc_timestamp(order_purchase_ts_utc, 'America/Sao_Paulo')) as d
  from orders where order_purchase_ts_utc > timestamp '1900-01-01 00:00:00'
  union all
  select to_date(from_utc_timestamp(order_estimated_delivery_ts_utc, 'America/Sao_Paulo'))
  from orders where order_estimated_delivery_ts_utc > timestamp '1900-01-01 00:00:00'
  union all
  select to_date(from_utc_timestamp(order_delivered_customer_ts_utc, 'America/Sao_Paulo'))
  from orders where is_delivered
),
limites as (
  select min(d) as dt_ini, max(d) as dt_fim from datas_pedido
),
calendario as (
  select explode(sequence(dt_ini, dt_fim, interval 1 day)) as data from limites
)
select
  cast(date_format(data, 'yyyyMMdd') as int) as sk_tempo,
  data,
  year(data)       as ano,
  quarter(data)    as trimestre,
  month(data)      as mes,
  case month(data)
    when 1 then 'JANEIRO'  when 2 then 'FEVEREIRO' when 3 then 'MARCO'
    when 4 then 'ABRIL'    when 5 then 'MAIO'      when 6 then 'JUNHO'
    when 7 then 'JULHO'    when 8 then 'AGOSTO'    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO' when 11 then 'NOVEMBRO' else 'DEZEMBRO'
  end              as nome_mes,
  date_format(data, 'yyyy-MM') as ano_mes,
  dayofmonth(data) as dia,
  dayofweek(data)  as dia_semana,          -- 1 = domingo
  case dayofweek(data)
    when 1 then 'DOMINGO' when 2 then 'SEGUNDA' when 3 then 'TERCA'
    when 4 then 'QUARTA'  when 5 then 'QUINTA'  when 6 then 'SEXTA'
    else 'SABADO'
  end              as nome_dia_semana,
  dayofweek(data) in (1, 7) as fim_de_semana
from calendario
union all
select 19000101, date '1900-01-01', 1900, 1, 1, 'NAO_INFORMADO', 'NAO_INFORMADO', 1, -1, 'NAO_INFORMADO', false
union all
select 99991231, date '9999-12-31', 9999, 4, 12, 'NAO_OCORRIDO',  'NAO_OCORRIDO',  31, -1, 'NAO_OCORRIDO',  false;


-- ---------------------------------------------------------------------
-- D2) dim_cliente   (grão: customer_unique_id = pessoa)
--     Aliases: customers, orders, geolocation (silver)
--     Endereço = o do pedido mais recente do cliente.
-- ---------------------------------------------------------------------
with enderecos as (
  select
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    row_number() over (
      partition by c.customer_unique_id
      order by o.order_purchase_ts_utc desc, c.customer_id
    ) as rn
  from customers c
  left join orders o on o.customer_id = c.customer_id
  where c.customer_unique_id <> 'NAO_INFORMADO'
)
select
  row_number() over (order by e.customer_unique_id) as sk_cliente,
  e.customer_unique_id,
  e.customer_zip_code_prefix          as cep_prefixo,
  e.customer_city                     as cidade,
  e.customer_state                    as uf,
  coalesce(g.geolocation_lat, -999)   as latitude,
  coalesce(g.geolocation_lng, -999)   as longitude
from enderecos e
left join geolocation g on g.zip_code_prefix = e.customer_zip_code_prefix
where e.rn = 1
union all
select -1, 'NAO_INFORMADO', '00000', 'NAO_INFORMADO', 'NI', -999, -999;


-- ---------------------------------------------------------------------
-- D3) dim_produto
--     Aliases: products, category_translation (silver)
-- ---------------------------------------------------------------------
select
  p.sk_product                                         as sk_produto,
  p.product_id,
  p.product_category_name                              as categoria,
  coalesce(t.product_category_name_english, 'NOT_INFORMED') as categoria_en,
  p.product_photos_qty                                 as qtd_fotos,
  p.product_weight_g                                   as peso_g,
  p.product_length_cm                                  as comprimento_cm,
  p.product_height_cm                                  as altura_cm,
  p.product_width_cm                                   as largura_cm
from products p
left join category_translation t
  on t.product_category_name = p.product_category_name
union all
select -1, 'NAO_INFORMADO', 'NAO_INFORMADO', 'NOT_INFORMED', -1, -1, -1, -1, -1;


-- ---------------------------------------------------------------------
-- D4) dim_vendedor
--     Aliases: sellers, geolocation (silver)
-- ---------------------------------------------------------------------
select
  s.sk_seller                        as sk_vendedor,
  s.seller_id,
  s.seller_zip_code_prefix           as cep_prefixo,
  s.seller_city                      as cidade,
  s.seller_state                     as uf,
  coalesce(g.geolocation_lat, -999)  as latitude,
  coalesce(g.geolocation_lng, -999)  as longitude
from sellers s
left join geolocation g on g.zip_code_prefix = s.seller_zip_code_prefix
union all
select -1, 'NAO_INFORMADO', '00000', 'NAO_INFORMADO', 'NI', -999, -999;


-- ---------------------------------------------------------------------
-- D5) dim_tipo_pagamento
--     Aliases: order_payments (silver)
-- ---------------------------------------------------------------------
select
  row_number() over (order by payment_type) as sk_tipo_pagamento,
  payment_type                               as tipo_pagamento
from (select distinct payment_type from order_payments) t
where payment_type <> 'NAO_INFORMADO'
union all
select -1, 'NAO_INFORMADO';


-- =====================================================================
-- FATOS
-- Chave de data (horário de Brasília, preservando sentinelas):
--   case when ts = timestamp '9999-12-31 00:00:00' then 99991231
--        when ts = timestamp '1900-01-01 00:00:00' then 19000101
--        else cast(date_format(from_utc_timestamp(ts, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
--   end
-- =====================================================================

-- ---------------------------------------------------------------------
-- F1) fato_itens_pedido   (grão: 1 item de pedido)
--     Aliases: order_items, orders, customers (silver)
--              dim_cliente, dim_produto, dim_vendedor (gold)
--     Partição: year/month
-- ---------------------------------------------------------------------
select
  i.sk_order_item                         as sk_item_pedido,
  i.order_id,                                         -- dimensão degenerada
  i.order_item_id,
  coalesce(dc.sk_cliente,  -1)            as sk_cliente,
  coalesce(dp.sk_produto,  -1)            as sk_produto,
  coalesce(dv.sk_vendedor, -1)            as sk_vendedor,
  case when o.order_purchase_ts_utc = timestamp '1900-01-01 00:00:00' then 19000101
       else cast(date_format(from_utc_timestamp(o.order_purchase_ts_utc, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
  end                                     as sk_data_compra,
  case when o.order_delivered_customer_ts_utc = timestamp '9999-12-31 00:00:00' then 99991231
       when o.order_delivered_customer_ts_utc = timestamp '1900-01-01 00:00:00' then 19000101
       else cast(date_format(from_utc_timestamp(o.order_delivered_customer_ts_utc, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
  end                                     as sk_data_entrega,
  o.order_status                          as status_pedido,
  case when i.price         >= 0 then i.price         end as valor_item,
  case when i.freight_value >= 0 then i.freight_value end as valor_frete,
  case when i.price >= 0 and i.freight_value >= 0
       then i.price + i.freight_value end               as valor_total_item,
  o.year,
  o.month
from order_items i
join orders o              on o.order_id = i.order_id
left join customers c      on c.customer_id = o.customer_id
left join dim_cliente dc   on dc.customer_unique_id = c.customer_unique_id
left join dim_produto dp   on dp.product_id = i.product_id
left join dim_vendedor dv  on dv.seller_id  = i.seller_id;


-- ---------------------------------------------------------------------
-- F2) fato_pagamentos   (grão: 1 pagamento do pedido)
--     Aliases: order_payments, orders, customers (silver)
--              dim_cliente, dim_tipo_pagamento (gold)
--     Partição: year/month
-- ---------------------------------------------------------------------
select
  p.sk_order_payment                      as sk_pagamento,
  p.order_id,
  p.payment_sequential                    as sequencia_pagamento,
  coalesce(dc.sk_cliente, -1)             as sk_cliente,
  coalesce(dt.sk_tipo_pagamento, -1)      as sk_tipo_pagamento,
  case when o.order_purchase_ts_utc = timestamp '1900-01-01 00:00:00' then 19000101
       else cast(date_format(from_utc_timestamp(o.order_purchase_ts_utc, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
  end                                     as sk_data_compra,
  case when p.payment_installments >= 0 then p.payment_installments end as qtd_parcelas,
  case when p.payment_value        >= 0 then p.payment_value        end as valor_pagamento,
  o.year,
  o.month
from order_payments p
join orders o                   on o.order_id = p.order_id
left join customers c           on c.customer_id = o.customer_id
left join dim_cliente dc        on dc.customer_unique_id = c.customer_unique_id
left join dim_tipo_pagamento dt on dt.tipo_pagamento = p.payment_type;


-- ---------------------------------------------------------------------
-- F3) fato_pedidos   (grão: 1 pedido — entrega e satisfação)
--     Aliases: orders, order_items, order_reviews, customers (silver)
--              dim_cliente (gold)
--     Partição: year/month
-- ---------------------------------------------------------------------
with itens as (
  select
    order_id,
    count(*)                                               as qtd_itens,
    sum(case when price         >= 0 then price         end) as valor_produtos,
    sum(case when freight_value >= 0 then freight_value end) as valor_frete
  from order_items
  group by order_id
),
avaliacoes as (
  select
    order_id,
    avg(case when review_score between 1 and 5 then review_score end) as nota_media,
    max(cast(has_comment as int)) = 1                                 as tem_comentario
  from order_reviews
  group by order_id
)
select
  o.sk_order                              as sk_pedido,
  o.order_id,
  coalesce(dc.sk_cliente, -1)             as sk_cliente,
  case when o.order_purchase_ts_utc = timestamp '1900-01-01 00:00:00' then 19000101
       else cast(date_format(from_utc_timestamp(o.order_purchase_ts_utc, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
  end                                     as sk_data_compra,
  case when o.order_delivered_customer_ts_utc = timestamp '9999-12-31 00:00:00' then 99991231
       when o.order_delivered_customer_ts_utc = timestamp '1900-01-01 00:00:00' then 19000101
       else cast(date_format(from_utc_timestamp(o.order_delivered_customer_ts_utc, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
  end                                     as sk_data_entrega,
  case when o.order_estimated_delivery_ts_utc = timestamp '1900-01-01 00:00:00' then 19000101
       else cast(date_format(from_utc_timestamp(o.order_estimated_delivery_ts_utc, 'America/Sao_Paulo'), 'yyyyMMdd') as int)
  end                                     as sk_data_estimada,
  o.order_status                          as status_pedido,
  o.is_delivered                          as entregue,
  -- métricas de entrega (somente pedidos entregues; demais = NULL)
  case when o.is_delivered
       then datediff(o.order_delivered_customer_ts_utc, o.order_purchase_ts_utc) end as dias_ate_entrega,
  case when o.is_delivered
        and o.order_estimated_delivery_ts_utc > timestamp '1900-01-01 00:00:00'
       then datediff(o.order_delivered_customer_ts_utc, o.order_estimated_delivery_ts_utc) end as dias_atraso,
  case when o.is_delivered
        and o.order_estimated_delivery_ts_utc > timestamp '1900-01-01 00:00:00'
       then o.order_delivered_customer_ts_utc > o.order_estimated_delivery_ts_utc end as entregue_com_atraso,
  coalesce(i.qtd_itens, 0)                as qtd_itens,
  i.valor_produtos,
  i.valor_frete,
  i.valor_produtos + i.valor_frete        as valor_total_pedido,
  a.nota_media                            as nota_avaliacao,
  coalesce(a.tem_comentario, false)       as tem_comentario,
  o.year,
  o.month
from orders o
left join itens i          on i.order_id = o.order_id
left join avaliacoes a     on a.order_id = o.order_id
left join customers c      on c.customer_id = o.customer_id
left join dim_cliente dc   on dc.customer_unique_id = c.customer_unique_id;