CREATE OR REPLACE TABLE `sorteostec-ml.h1.patrones_y_funnel_web_20241201_20251130` 
PARTITION BY attempt_date
CLUSTER BY ITEM, STATUS, login_bucket_bc AS

WITH t AS (
  SELECT
    USER            AS user_pseudo_id,
    SESION          AS session_id,
    DATETIME        AS datetime_str,
    PARSE_DATETIME('%d/%m/%Y %H:%M:%S', DATETIME) AS attempt_dt_mx,
    PARSE_DATE('%d/%m/%Y', SUBSTR(DATETIME,1,10)) AS attempt_date,
    ITEM,
    SAFE_CAST(INTENTO AS INT64) AS intento,
    STATUS,
    SAFE_CAST(CANTIDAD_ADD_TO_CART    AS INT64) AS qty_add_to_cart,
    SAFE_CAST(CANTIDAD_BEGIN_CHECKOUT AS INT64) AS qty_begin_checkout,
    SAFE_CAST(CANTIDAD_PURCHASE       AS INT64) AS qty_purchase,
    TRANSACTION_ID,

    -- MONTOS desde el script
    MONTO_ADD_TO_CART,
    MONTO_BEGIN_CHECKOUT,
    MONTO_PURCHASE,

    PROMOS_ADD_CART_COMPLETAS, PROMOS_ADD_CART_INCOMPLETAS, PROMOS_ADD_CART_TODAS,
    PROMOS_CHECKOUT_COMPLETAS, PROMOS_CHECKOUT_INCOMPLETAS, PROMOS_CHECKOUT_TODAS,
    PROMOS_PURCHASE_COMPLETAS, PROMOS_PURCHASE_INCOMPLETAS, PROMOS_PURCHASE_TODAS,
    PATRON_ADD_CART, PATRON_BEGIN_CHECKOUT, PATRON_PURCHASE,
    TIENE_PATRON_COMPLETO, TIENE_PATRON_INCOMPLETO
  FROM `sorteostec-ml.h1.ga4_patrones_promociones_20241201_20251130`
  WHERE PARSE_DATE('%d/%m/%Y', SUBSTR(DATETIME,1,10))
        BETWEEN DATE '2024-12-01' AND DATE '2025-11-30'
),
s AS (
  SELECT
    user_pseudo_id, session_id,
    session_date, session_start_mx, session_end_mx,
    login_time_mx, logout_time_mx,
    view_item_list_time_mx, select_item_time_mx,
    add_to_cart_time_mx, begin_checkout_time_mx, purchase_time_mx,
    sign_up_time_mx,
    event_count, has_purchase, has_sign_up,
    discount_seen_after_login, categoria_login
  FROM `sorteostec-ml.h1.sesiones_funnel_lineal_web_20241201_20251130`
  WHERE session_date BETWEEN DATE '2024-12-01' AND DATE '2025-11-30'
)
SELECT
  -- claves intento–producto
  t.user_pseudo_id, t.session_id, t.intento, t.ITEM, t.STATUS,
  t.attempt_dt_mx, t.attempt_date, t.datetime_str, t.TRANSACTION_ID,

  -- cantidades (boletos)
  t.qty_add_to_cart, t.qty_begin_checkout, t.qty_purchase,

  -- MONTOS por etapa (NUMERIC)
  t.MONTO_ADD_TO_CART, t.MONTO_BEGIN_CHECKOUT, t.MONTO_PURCHASE,
  SAFE_DIVIDE(t.MONTO_PURCHASE, NULLIF(t.qty_purchase,0)) AS precio_unitario_inferido,

  -- promos/patrones
  t.PATRON_ADD_CART, t.PATRON_BEGIN_CHECKOUT, t.PATRON_PURCHASE,
  t.PROMOS_ADD_CART_COMPLETAS, t.PROMOS_ADD_CART_INCOMPLETAS, t.PROMOS_ADD_CART_TODAS,
  t.PROMOS_CHECKOUT_COMPLETAS, t.PROMOS_CHECKOUT_INCOMPLETAS, t.PROMOS_CHECKOUT_TODAS,
  t.PROMOS_PURCHASE_COMPLETAS, t.PROMOS_PURCHASE_INCOMPLETAS, t.PROMOS_PURCHASE_TODAS,
  t.TIENE_PATRON_COMPLETO, t.TIENE_PATRON_INCOMPLETO,

  -- contexto de sesión y buckets de login
  s.session_date, s.session_start_mx, s.session_end_mx,
  s.login_time_mx, s.logout_time_mx,
  s.view_item_list_time_mx, s.select_item_time_mx,
  s.add_to_cart_time_mx, s.begin_checkout_time_mx, s.purchase_time_mx,
  s.sign_up_time_mx,
  s.event_count, s.has_purchase, s.has_sign_up,
  s.discount_seen_after_login, s.categoria_login,

  (ARRAY_LENGTH(t.PROMOS_CHECKOUT_COMPLETAS) > 0) AS ready_at_checkout,
  (ARRAY_LENGTH(t.PROMOS_PURCHASE_COMPLETAS) > 0) AS ready_at_purchase,

  CASE
    WHEN s.has_purchase = TRUE
        AND s.login_time_mx IS NULL
        AND s.logout_time_mx IS NOT NULL
        AND s.purchase_time_mx IS NOT NULL
        AND s.logout_time_mx > s.purchase_time_mx
      THEN 'LOGIN YA INICIADO'

    WHEN s.has_purchase = TRUE
        AND s.login_time_mx IS NULL
      THEN 'PROBABLE LOGIN YA INICIADO'

    WHEN s.has_purchase = FALSE
       AND s.login_time_mx IS NULL
       AND s.logout_time_mx IS NOT NULL
    THEN 'LOGIN INICIADO SIN COMPRA'


    WHEN s.login_time_mx IS NULL THEN 'SIN LOGIN EN SESIÓN'
    WHEN s.begin_checkout_time_mx IS NOT NULL AND s.login_time_mx > s.begin_checkout_time_mx THEN 'LOGIN DESPUÉS DE BEGIN_CHECKOUT'
    WHEN s.login_time_mx IS NOT NULL AND s.begin_checkout_time_mx IS NOT NULL AND s.login_time_mx < s.begin_checkout_time_mx THEN 'LOGIN ANTES DE BEGIN_CHECKOUT'
    WHEN s.login_time_mx IS NOT NULL AND s.add_to_cart_time_mx IS NOT NULL AND s.login_time_mx < s.add_to_cart_time_mx THEN 'LOGIN ANTES DE ADD_TO_CART'
    WHEN s.login_time_mx IS NOT NULL AND s.select_item_time_mx IS NOT NULL AND s.login_time_mx < s.select_item_time_mx THEN 'LOGIN TEMPRANO - ANTES DE SELECT_ITEM'
    WHEN s.login_time_mx IS NOT NULL AND s.view_item_list_time_mx IS NOT NULL AND s.login_time_mx < s.view_item_list_time_mx THEN 'LOGIN TEMPRANO - ANTES DE VIEW_ITEM_LIST'
    WHEN s.login_time_mx IS NOT NULL
        AND s.view_item_list_time_mx IS NULL AND s.select_item_time_mx IS NULL
        AND s.add_to_cart_time_mx IS NULL AND s.begin_checkout_time_mx IS NULL
        AND s.purchase_time_mx IS NULL
      THEN 'CON LOGIN SIN EVENTOS DE FUNNEL'
    ELSE 'OTROS'
  END AS login_bucket_bc

FROM t
LEFT JOIN s USING (user_pseudo_id, session_id);