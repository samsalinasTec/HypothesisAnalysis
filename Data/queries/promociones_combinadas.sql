WITH BASE AS
(SELECT P.CLAVE_PROMOCION,
         P.DESC_PROMOCION,
         P.DESCL_PROMOCION,
         S.CLAVE_EDICION_PRODUCTO,
         S.DESC_SORTEO,
         S.TIPO_PRODUCTO,
         S.NUMERO_SORTEO,
         S.PRECIO_UNITARIO,
         CP.CANTIDAD_INICIAL,
         TCP.DESCRIPCION AS DESC_TIPO_CANTIDAD_PROMOCION,
         CP2.DESCRIPCION AS DESC_TIPO_CRITERIO_CONDICION,
         TB.DESCRIPCION AS DESC_BENEFICIO_PROMOCION,
         BP.CANTIDAD,
         CP3.DESC_CATEGORIA AS DESC_CATEGORIA_PROMOCION,
         COUNT(DISTINCT S.CLAVE_EDICION_PRODUCTO) OVER(PARTITION BY P.CLAVE_PROMOCION) AS CANT_SORTEOS
    FROM sorteostec-ml.ml_siteconversion.promocion P
    LEFT JOIN sorteostec-ml.ml_siteconversion.grupo_condiciones_promocion GCP
      ON GCP.CLAVE_PROMOCION = P.CLAVE_PROMOCION
    LEFT JOIN sorteostec-ml.ml_siteconversion.condicion_promocion CP
      ON CP.CLAVE_GRUPO_CONDICIONES = GCP.CLAVE_GRUPO_CONDICIONES
    LEFT JOIN sorteostec-ml.ml_siteconversion.sorteo S
      ON S.CLAVE_EDICION_PRODUCTO = CP.CLAVE_EDICION_PRODUCTO
    LEFT JOIN sorteostec-ml.ml_siteconversion.beneficio_promocion BP
      ON BP.CLAVE_GRUPO_CONDICIONES = GCP.CLAVE_GRUPO_CONDICIONES
    LEFT JOIN sorteostec-ml.ml_siteconversion.tipo_beneficio TB
      ON TB.CLAVE_TIPO_BENEFICIO = BP.CLAVE_TIPO_BENEFICIO
    LEFT JOIN sorteostec-ml.ml_siteconversion.tipo_cantidad_promocion TCP
      ON TCP.CLAVE_TIPO_CANTIDAD_CONDICION =
         CP.CLAVE_TIPO_CANTIDAD_CONDICION
    LEFT JOIN sorteostec-ml.ml_siteconversion.tipo_criterio_condicion CP2
      ON CP2.CLAVE_TIPO_CRITERIO_CONDICION =
         CP.CLAVE_TIPO_CRITERIO_CONDICION
    LEFT JOIN sorteostec-ml.ml_siteconversion.categoria_promocion CP3
      ON CP3.CLAVE_CATEGORIA = P.CLAVE_CATEGORIA)
SELECT * FROM BASE WHERE CANT_SORTEOS > 1 