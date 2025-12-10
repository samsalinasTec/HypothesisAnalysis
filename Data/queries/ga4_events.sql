SELECT
  USER,
  SESION,
  DATETIME,                      -- '%d/%m/%Y %H:%M:%S' (horario MX)
  ITEM,
  INTENTO,
  CASE WHEN HAS_PURCHASE_INT=1 THEN 'PURCHASED' ELSE 'NO_PURCHASE' END AS STATUS,
  qty_add_to_cart      AS CANTIDAD_ADD_TO_CART,
  qty_begin_checkout   AS CANTIDAD_BEGIN_CHECKOUT,
  qty_purchase         AS CANTIDAD_PURCHASE,
  transaction_id       AS TRANSACTION_ID,
  CAST(NULL AS STRING) AS item_id
FROM `{TABLE_B}`
WHERE attempt_date BETWEEN DATE('{DATE_START}') AND DATE('{DATE_END}')