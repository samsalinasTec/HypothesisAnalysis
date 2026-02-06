CREATE OR REPLACE TABLE `sorteostec-ml.h1.sesiones_funnel_lineal_web_{table_suffix}`
PARTITION BY session_date
CLUSTER BY user_pseudo_id, session_id
AS
WITH
-- 0) Días que YA tienen tabla diaria events_YYYYMMDD (para NO duplicar con intraday)
events_days AS (
  SELECT
    REGEXP_EXTRACT(table_name, r'^events_(\d{8})$') AS ds
  FROM `sorteostec-analytics360.analytics_277858205.INFORMATION_SCHEMA.TABLES`
  WHERE REGEXP_CONTAINS(table_name, r'^events_\d{8}$')
    AND REGEXP_EXTRACT(table_name, r'^events_(\d{8})$') BETWEEN '{start_date_nodash}' AND '{end_date_nodash}'
),
 
-- 1) Fuente unificada (events + intraday sin duplicar por día)
e AS (
  -- diario (preferido)
  SELECT
    user_pseudo_id,
    event_params,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    items,
    platform
  FROM `sorteostec-analytics360.analytics_277858205.events_*`
  WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\d{8}$')
    AND _TABLE_SUFFIX BETWEEN '{start_date_nodash}' AND '{end_date_nodash}'
    AND platform = 'WEB'
    AND EXISTS (SELECT 1 FROM UNNEST(event_params) WHERE key = 'ga_session_id')
    AND event_name IN (
      'login','logout','view_item_list','select_item',
      'add_to_cart','remove_from_cart','begin_checkout','purchase','sign_up'
    )
 
  UNION ALL
 
  -- intraday solo si falta el día en diario
  SELECT
    i.user_pseudo_id,
    i.event_params,
    i.event_name,
    i.event_timestamp,
    i.event_bundle_sequence_id,
    i.event_server_timestamp_offset,
    i.items,
    i.platform
  FROM `sorteostec-analytics360.analytics_277858205.events_intraday_*` i
  WHERE i._TABLE_SUFFIX BETWEEN '{start_date_nodash}' AND '{end_date_nodash}'
    AND NOT EXISTS (
      SELECT 1 FROM events_days d
      WHERE d.ds = i._TABLE_SUFFIX
    )
    AND i.platform = 'WEB'
    AND EXISTS (SELECT 1 FROM UNNEST(i.event_params) WHERE key = 'ga_session_id')
    AND i.event_name IN (
      'login','logout','view_item_list','select_item',
      'add_to_cart','remove_from_cart','begin_checkout','purchase','sign_up'
    )
),
base_raw AS (
  SELECT
    e.user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id' LIMIT 1) AS session_id,
    e.event_name,
    e.event_timestamp,
    TIMESTAMP_MICROS(e.event_timestamp) AS event_ts_utc,
    DATETIME(TIMESTAMP_MICROS(e.event_timestamp), "America/Mexico_City") AS event_dt_mx,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='transaction_id' LIMIT 1) AS transaction_id,
    (SELECT value.int_value  FROM UNNEST(e.event_params) WHERE key='discount'       LIMIT 1) AS discount,
    e.event_bundle_sequence_id,
    e.event_server_timestamp_offset,
    e.items
  FROM e
),
base_dedup AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (
        PARTITION BY user_pseudo_id, session_id, event_name, event_timestamp
        ORDER BY event_server_timestamp_offset DESC, event_bundle_sequence_id DESC
      ) AS rn
    FROM base_raw b
  )
  WHERE rn = 1
),
marcadores AS (
  SELECT
    user_pseudo_id,
    session_id,
 
    MIN(event_ts_utc) AS session_start_utc,
    MAX(event_ts_utc) AS session_end_utc,
    DATETIME(MIN(event_ts_utc), "America/Mexico_City") AS session_start_mx,
    DATETIME(MAX(event_ts_utc), "America/Mexico_City") AS session_end_mx,
 
    COUNT(*) AS event_count,
    COUNTIF(event_name='purchase') AS purchase_events,
 
    MIN(IF(event_name='login',            event_ts_utc, NULL)) AS login_time_utc,
    MIN(IF(event_name='logout',           event_ts_utc, NULL)) AS logout_time_utc,
    MIN(IF(event_name='view_item_list',   event_ts_utc, NULL)) AS view_item_list_time_utc,
    MIN(IF(event_name='select_item',      event_ts_utc, NULL)) AS select_item_time_utc,
    MIN(IF(event_name='add_to_cart',      event_ts_utc, NULL)) AS add_to_cart_time_utc,
    MIN(IF(event_name='begin_checkout',   event_ts_utc, NULL)) AS begin_checkout_time_utc,
    MIN(IF(event_name='purchase',         event_ts_utc, NULL)) AS purchase_time_utc,
    MIN(IF(event_name='sign_up',          event_ts_utc, NULL)) AS sign_up_time_utc
  FROM base_dedup
  GROUP BY user_pseudo_id, session_id
),
discount_after_login AS (
  SELECT
    m.user_pseudo_id, m.session_id,
    CASE
      WHEN m.login_time_utc IS NULL THEN FALSE
      ELSE EXISTS (
        SELECT 1
        FROM base_dedup d
        WHERE d.user_pseudo_id = m.user_pseudo_id
          AND d.session_id     = m.session_id
          AND d.discount IS NOT NULL
          AND d.event_ts_utc  >= m.login_time_utc
      )
    END AS discount_seen_after_login
  FROM marcadores m
),
 
-- Ordenamos dinámicamente los hitos de funnel presentes en la sesión
ordered AS (
  SELECT
    m.*,
    da.discount_seen_after_login,
    ARRAY(
      SELECT AS STRUCT step, t
      FROM UNNEST([
        STRUCT('VIEW_ITEM_LIST' AS step, m.view_item_list_time_utc AS t),
        STRUCT('SELECT_ITEM',    m.select_item_time_utc),
        STRUCT('ADD_TO_CART',    m.add_to_cart_time_utc),
        STRUCT('BEGIN_CHECKOUT', m.begin_checkout_time_utc)
      ])
      WHERE t IS NOT NULL
      ORDER BY t, step
    ) AS steps
  FROM marcadores m
  LEFT JOIN discount_after_login da
    ON da.user_pseudo_id = m.user_pseudo_id AND da.session_id = m.session_id
)
 
SELECT
  o.user_pseudo_id,
  o.session_id,
 
  o.session_start_mx,
  o.session_end_mx,
 
  DATETIME(o.login_time_utc,          "America/Mexico_City") AS login_time_mx,
  DATETIME(o.logout_time_utc,         "America/Mexico_City") AS logout_time_mx,
  DATETIME(o.view_item_list_time_utc, "America/Mexico_City") AS view_item_list_time_mx,
  DATETIME(o.select_item_time_utc,    "America/Mexico_City") AS select_item_time_mx,
  DATETIME(o.add_to_cart_time_utc,    "America/Mexico_City") AS add_to_cart_time_mx,
  DATETIME(o.begin_checkout_time_utc, "America/Mexico_City") AS begin_checkout_time_mx,
  DATETIME(o.purchase_time_utc,       "America/Mexico_City") AS purchase_time_mx,
  DATETIME(o.sign_up_time_utc,        "America/Mexico_City") AS sign_up_time_mx,
 
  DATE(o.session_start_mx) AS session_date,  -- partición
  o.event_count,
  (o.purchase_events > 0) AS has_purchase,
  (o.sign_up_time_utc IS NOT NULL) AS has_sign_up,
  o.discount_seen_after_login,
 
  -- Comparativos por etapa (consistentes)
  CASE
    WHEN o.login_time_utc IS NULL THEN 'SIN LOGIN'
    WHEN o.view_item_list_time_utc IS NULL THEN 'SIN EVENTO'
    WHEN o.login_time_utc <  o.view_item_list_time_utc THEN 'ANTES'
    WHEN o.login_time_utc >  o.view_item_list_time_utc THEN 'DESPUÉS'
    ELSE 'EMPATE'
  END AS login_vs_view_item_list,
 
  CASE
    WHEN o.login_time_utc IS NULL THEN 'SIN LOGIN'
    WHEN o.select_item_time_utc IS NULL THEN 'SIN EVENTO'
    WHEN o.login_time_utc <  o.select_item_time_utc THEN 'ANTES'
    WHEN o.login_time_utc >  o.select_item_time_utc THEN 'DESPUÉS'
    ELSE 'EMPATE'
  END AS login_vs_select_item,
 
  CASE
    WHEN o.login_time_utc IS NULL THEN 'SIN LOGIN'
    WHEN o.add_to_cart_time_utc IS NULL THEN 'SIN EVENTO'
    WHEN o.login_time_utc <  o.add_to_cart_time_utc THEN 'ANTES'
    WHEN o.login_time_utc >  o.add_to_cart_time_utc THEN 'DESPUÉS'
    ELSE 'EMPATE'
  END AS login_vs_add_to_cart,
 
  CASE
    WHEN o.login_time_utc IS NULL THEN 'SIN LOGIN'
    WHEN o.begin_checkout_time_utc IS NULL THEN 'SIN EVENTO'
    WHEN o.login_time_utc <  o.begin_checkout_time_utc THEN 'ANTES'
    WHEN o.login_time_utc >  o.begin_checkout_time_utc THEN 'DESPUÉS'
    ELSE 'EMPATE'
  END AS login_vs_begin_checkout,
 
  -- Bucket único, sin “sin clasificar”
  CASE
    WHEN o.login_time_utc IS NULL THEN 'SIN LOGIN EN SESIÓN'
    WHEN ARRAY_LENGTH(o.steps) = 0 THEN 'CON LOGIN SIN EVENTOS DE FUNNEL'
    WHEN o.login_time_utc < o.steps[SAFE_OFFSET(0)].t
      THEN CONCAT('LOGIN ANTES DE ', o.steps[SAFE_OFFSET(0)].step)
    WHEN ARRAY_LENGTH(o.steps) = 1
      THEN CONCAT('LOGIN DESPUÉS DE ', o.steps[SAFE_OFFSET(0)].step)
    WHEN o.login_time_utc < o.steps[SAFE_OFFSET(1)].t
      THEN CONCAT('LOGIN ENTRE ', o.steps[SAFE_OFFSET(0)].step, ' Y ', o.steps[SAFE_OFFSET(1)].step)
    WHEN ARRAY_LENGTH(o.steps) = 2
      THEN CONCAT('LOGIN DESPUÉS DE ', o.steps[SAFE_OFFSET(1)].step)
    WHEN o.login_time_utc < o.steps[SAFE_OFFSET(2)].t
      THEN CONCAT('LOGIN ENTRE ', o.steps[SAFE_OFFSET(1)].step, ' Y ', o.steps[SAFE_OFFSET(2)].step)
    WHEN ARRAY_LENGTH(o.steps) = 3
      THEN CONCAT('LOGIN DESPUÉS DE ', o.steps[SAFE_OFFSET(2)].step)
    WHEN o.login_time_utc < o.steps[SAFE_OFFSET(3)].t
      THEN CONCAT('LOGIN ENTRE ', o.steps[SAFE_OFFSET(2)].step, ' Y ', o.steps[SAFE_OFFSET(3)].step)
    ELSE CONCAT('LOGIN DESPUÉS DE ', o.steps[SAFE_OFFSET(3)].step)
  END AS categoria_login
FROM ordered o;

