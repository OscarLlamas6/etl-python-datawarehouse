USE [master]
USE [PROYECTO1]

/* CONSULTA 1: TOP 10 CON MEJOR PIB EN 2021*/

SELECT T.year_field AS 'year', P.pais, T.valor FROM (
SELECT F.year_field, R.id_pais, R.valor FROM ( SELECT IP.id_pais, IP.valor, IP.id_fecha
    FROM [PROYECTO1].indicadorpais AS IP
    INNER JOIN [PROYECTO1].indicador AS I
     ON IP.id_indicador = I.id_indicador
    WHERE I.indicador LIKE 'Crecimiento del PIB%') AS R
INNER JOIN [PROYECTO1].fecha AS F ON R.id_fecha = F.id_fecha
WHERE F.year_field = 2020
ORDER BY R.valor DESC
OFFSET 0 ROWS
FETCH NEXT 10 ROWS ONLY) AS T
INNER JOIN [PROYECTO1].pais AS P
ON T.id_pais = P.id_pais

/* CONSULTA 2: TOP 10 CON MENOR PIB EN 2018*/

SELECT T.year_field AS 'year', P.pais, T.valor FROM (
SELECT F.year_field, R.id_pais, R.valor FROM ( SELECT IP.id_pais, IP.valor, IP.id_fecha
    FROM [PROYECTO1].indicadorpais AS IP
    INNER JOIN [PROYECTO1].indicador AS I
     ON IP.id_indicador = I.id_indicador
    WHERE I.indicador LIKE 'Crecimiento del PIB%') AS R
INNER JOIN [PROYECTO1].fecha AS F ON R.id_fecha = F.id_fecha
WHERE F.year_field = 2018
ORDER BY R.valor ASC
OFFSET 0 ROWS
FETCH NEXT 10 ROWS ONLY) AS T
INNER JOIN [PROYECTO1].pais AS P
ON T.id_pais = P.id_pais

/* CONSULTA 3: PAISES QUE TUVIERON UN PIB MAYOR A 10 EN 2005 */

SELECT PI.pais, RY.medida_pib, RY.anio FROM (
    SELECT RP.cod_pais, RP.medida_pib, YP.anio
    FROM [PROYECTO1].registro_pibpais_im AS RP
    INNER JOIN [PROYECTO1].anio_registrado_im AS YP
    ON RP.cod_anio = YP.id_anio
    WHERE YP.anio = 2005 AND RP.medida_pib > 10) AS RY
INNER JOIN [PROYECTO1].pais_im AS PI
ON RY.cod_pais = PI.id_pais
ORDER BY RY.medida_pib DESC

/* CONSULTA 4: PAISES QUE TUVIERON UN PIB MENOR A 7 Y MAYOR 0 EN 2010 */

SELECT PI.pais, RY.medida_pib, RY.anio FROM (
    SELECT RP.cod_pais, RP.medida_pib, YP.anio
    FROM [PROYECTO1].registro_pibpais_im AS RP
    INNER JOIN [PROYECTO1].anio_registrado_im AS YP
    ON RP.cod_anio = YP.id_anio
    WHERE YP.anio = 2010 AND RP.medida_pib < 10 AND RP.medida_pib > 0) AS RY
INNER JOIN [PROYECTO1].pais_im AS PI
ON RY.cod_pais = PI.id_pais
ORDER BY RY.medida_pib DESC

/* CONSULTA 5: PAISES QUE TUVIERON UNA INFLACION MAYOR A 6 EN 1976 */

SELECT PI.pais, RY.medida_inflacion, RY.anio FROM (
    SELECT RP.cod_pais, RP.medida_inflacion, YP.anio
    FROM [PROYECTO1].registro_inflacionpais_inf AS RP
    INNER JOIN [PROYECTO1].anio_registrado_im AS YP
    ON RP.cod_anio = YP.id_anio
    WHERE YP.anio = 1976 AND RP.medida_inflacion > 6) AS RY
INNER JOIN [PROYECTO1].pais_im AS PI
ON RY.cod_pais = PI.id_pais
ORDER BY RY.medida_inflacion DESC

/* CONSULTA 6: PAISES QUE TUVIERON UNA INFLACION MENOR A 6 Y MAYOR A CERO EN 1985 */

SELECT PI.pais, RY.medida_inflacion, RY.anio FROM (
    SELECT RP.cod_pais, RP.medida_inflacion, YP.anio
    FROM [PROYECTO1].registro_inflacionpais_inf AS RP
    INNER JOIN [PROYECTO1].anio_registrado_im AS YP
    ON RP.cod_anio = YP.id_anio
    WHERE YP.anio = 1985 AND RP.medida_inflacion < 6 AND RP.medida_inflacion > 0) AS RY
INNER JOIN [PROYECTO1].pais_im AS PI
ON RY.cod_pais = PI.id_pais
ORDER BY RY.medida_inflacion DESC