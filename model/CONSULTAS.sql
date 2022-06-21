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