SELECT
  c.clave_promocion,
  MIN(DATE(d.fecha_pagado)) d_inicio_promocion,
  MAX(DATE(d.fecha_pagado)) d_cierre_promocion
FROM
  `sorteostec-ml.ml_siteconversion.compras` c
LEFT JOIN
  `sorteostec-ml.ml_siteconversion.detalle_compra` d
ON
  CAST(c.agrupador_interno_compra AS STRING) = d.agrupador_interno_compra
GROUP BY
  1