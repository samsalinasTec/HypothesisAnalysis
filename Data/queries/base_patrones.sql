CREATE OR REPLACE TABLE `sorteostec-ml.h1.intentos_producto_canonico_web_20241001_20251231`
PARTITION BY attempt_date
CLUSTER BY ITEM, STATUS AS
WITH
events_days AS (
  SELECT DISTINCT _TABLE_SUFFIX AS ds
  FROM `sorteostec-analytics360.analytics_277858205.events_*`
  WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\d{8}$')
    AND _TABLE_SUFFIX BETWEEN '20241001' AND '20251231' -- CAMBIAR FECHA
),

e AS (
  -- diario (preferido)
  SELECT
    user_pseudo_id,
    event_params,
    event_name,
    event_timestamp,
    ecommerce,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    items,
    platform,
    -- Dimensiones añadidas. Cambios JQL 6Ene26
    device.category AS device_category,
    geo.country AS geo_country,
    geo.region AS geo_region,
    geo.city AS geo_city,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium
    
  FROM `sorteostec-analytics360.analytics_277858205.events_*`
  WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\d{8}$')
    AND _TABLE_SUFFIX BETWEEN '20241001' AND '20251231' -- CAMBIAR FECHA
    AND platform = 'WEB'
    AND EXISTS (SELECT 1 FROM UNNEST(event_params) WHERE key='ga_session_id')
    AND event_name IN ('add_to_cart','begin_checkout','purchase')

  UNION ALL

  -- intraday solo si falta el día en diario
  SELECT
    user_pseudo_id,
    event_params,
    event_name,
    event_timestamp,
    ecommerce,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    items,
    platform,
    -- Dimensiones añadidas. Cambios JQL 6Ene26
    i.device.category,
    i.geo.country,
    i.geo.region,
    i.geo.city,
    i.traffic_source.source,
    i.traffic_source.medium

  FROM `sorteostec-analytics360.analytics_277858205.events_intraday_*` i
  WHERE i._TABLE_SUFFIX BETWEEN '20241001' AND '20251231' -- CAMBIAR FECHA
    AND NOT EXISTS (SELECT 1 FROM events_days d WHERE d.ds = i._TABLE_SUFFIX)
    AND i.platform = 'WEB'
    AND EXISTS (SELECT 1 FROM UNNEST(i.event_params) WHERE key='ga_session_id')
    AND i.event_name IN ('add_to_cart','begin_checkout','purchase')
),
 
base_raw AS (
    SELECT
      e.user_pseudo_id,
      (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id' LIMIT 1) AS session_id,
      e.event_name,
      e.event_timestamp,
      TIMESTAMP_MICROS(e.event_timestamp) AS event_ts_utc,
      DATETIME(TIMESTAMP_MICROS(e.event_timestamp), "America/Mexico_City") AS event_dt_mx,
      COALESCE(
        (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='transaction_id' LIMIT 1),
        e.ecommerce.transaction_id
      ) AS transaction_id,
      e.event_bundle_sequence_id,
      e.event_server_timestamp_offset,
      i.item_name,                                        -- producto/edición

      -- Preservar dimensiones con ANY_VALUE. Cambios JQL 6Ene26
      ANY_VALUE(e.device_category) AS device_category,
      ANY_VALUE(e.geo_country) AS geo_country,
      ANY_VALUE(e.geo_region) AS geo_region,
      ANY_VALUE(e.geo_city) AS geo_city,
      ANY_VALUE(e.traffic_source) AS traffic_source,
      ANY_VALUE(e.traffic_medium) AS traffic_medium,

      -- COALESCE(SAFE_CAST(i.quantity AS INT64), 1) AS item_qty
      SUM(COALESCE(SAFE_CAST(i.quantity AS INT64), 1)) AS item_qty -- Sumar los boletos del mismo sorteo antes de deduplicar
    FROM e
    LEFT JOIN UNNEST(e.items) AS i
    GROUP BY 1,2,3,4,5,6,7,8,9,10 -- Agrupar por todo menos item_qty
  ),

    base_dedup AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT b.*,
             ROW_NUMBER() OVER (
               -- PARTITION BY user_pseudo_id, session_id, event_name, event_ts_utc, item_name
               PARTITION BY user_pseudo_id, session_id, event_name, event_timestamp, item_name -- event_timestamp es un INT64 ajustado al microseg que no redondea
               ORDER BY event_server_timestamp_offset DESC, event_bundle_sequence_id DESC
             ) AS rn
      FROM base_raw b
    )
    WHERE rn = 1
  ), 
  -- 🔧 conservamos offset/bundle para poder ordenar "el último" evento con precisión
  
  -- Mantener la nuevas columnas. Cambios JQL 6Ene26
  items_flat AS (
    SELECT
      *,
      COALESCE(item_name, '__NO_ITEM__') AS product_key
    FROM base_dedup
  ),
 
  seq AS (
    SELECT
      *,
      IF(event_name='purchase',1,0) AS is_purchase,
      SUM(IF(event_name='purchase',1,0)) OVER (
        PARTITION BY user_pseudo_id, session_id, product_key
        ORDER BY event_ts_utc
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS purchase_cume
    FROM items_flat
  ),
 
  bucketed AS (
    SELECT
      *,
      purchase_cume + IF(is_purchase=1,0,1) AS attempt_id
    FROM seq
  ),
 
  -- ✅ qty de BC por evento–producto (suma de boletos del producto dentro del evento)
  bc_per_event AS (
    SELECT
      user_pseudo_id, session_id, product_key, attempt_id,
      event_ts_utc,
      event_server_timestamp_offset,
      event_bundle_sequence_id,
      SUM(item_qty) AS qty_bc_event
    FROM bucketed
    WHERE event_name = 'begin_checkout'
    GROUP BY 1,2,3,4,5,6,7
  ),
 
  -- ✅ último begin_checkout del intento–producto (orden determinístico)
  bc_last AS (
    SELECT
      user_pseudo_id, session_id, product_key, attempt_id,
      (ARRAY_AGG(STRUCT(event_ts_utc, event_server_timestamp_offset, event_bundle_sequence_id, qty_bc_event)
                 ORDER BY event_ts_utc DESC, event_server_timestamp_offset DESC, event_bundle_sequence_id DESC
                 LIMIT 1))[OFFSET(0)].qty_bc_event AS qty_begin_checkout
    FROM bc_per_event
    GROUP BY 1,2,3,4
  ),
 
  -- Agregado por intento x producto
  agg AS (
    SELECT
      user_pseudo_id,
      session_id,
      product_key,
      attempt_id,
      -- Cambios JQL 6Ene26
      ANY_VALUE(device_category) AS device_category,
      ANY_VALUE(geo_country) AS geo_country,
      ANY_VALUE(geo_region) AS geo_region,
      ANY_VALUE(geo_city) AS geo_city,
      ANY_VALUE(traffic_source) AS traffic_source,
      ANY_VALUE(traffic_medium) AS traffic_medium,

      MIN(IF(event_name='purchase', event_dt_mx, NULL)) AS purchase_dt_mx,
      MAX(event_dt_mx)                                  AS last_dt_mx,
      MAX(IF(event_name='purchase',1,0))                AS has_purchase_int,
      SUM(IF(event_name='add_to_cart', item_qty, 0))    AS qty_add_to_cart,
      SUM(IF(event_name='purchase',   item_qty, 0))     AS qty_purchase,
      MAX(IF(event_name='purchase', transaction_id, NULL)) AS transaction_id
    FROM bucketed
    GROUP BY 1,2,3,4
  )
 
SELECT
  user_pseudo_id             AS USER,
  session_id                 AS SESION,
  product_key                AS ITEM,
  attempt_id                 AS INTENTO,
 
  -- Tiempo representativo
  COALESCE(purchase_dt_mx, last_dt_mx) AS attempt_dt_mx,
  FORMAT_DATETIME('%d/%m/%Y %H:%M:%S', COALESCE(purchase_dt_mx, last_dt_mx)) AS DATETIME,
  DATE(COALESCE(purchase_dt_mx, last_dt_mx)) AS attempt_date,
 
  -- Estatus y cantidades (boletos, NO conteo de eventos)
  has_purchase_int           AS HAS_PURCHASE_INT,
  CASE WHEN has_purchase_int=1 THEN 'PURCHASED' ELSE 'NO_PURCHASE' END AS STATUS,

    -- Dimensiones añadidas al SELECT final. Cambios JQL 6Ene25
  device_category,
  geo_country,
  geo_region,
  geo_city,
  traffic_source,
  traffic_medium,

  qty_add_to_cart,
  COALESCE(bc.qty_begin_checkout, 0)   AS qty_begin_checkout,   -- último BC del intento–producto (suma por evento)
  qty_purchase,
 
  transaction_id
FROM agg
LEFT JOIN bc_last bc

  USING (user_pseudo_id, session_id, product_key, attempt_id);

