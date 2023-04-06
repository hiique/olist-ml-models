-- Databricks notebook source
with tb_pedido as (

  SELECT DISTINCT t1.idPedido, t2.idVendedor

  FROM silver.olist.pedido t1

  LEFT JOIN silver.olist.item_pedido t2 ON t1.idPedido = t2.idPedido

  WHERE dtPedido < '2018-01-01' AND dtPedido >= ADD_MONTHS('2018-01-01', -6)
  AND idVendedor IS NOT NULL

),

tb_join AS (

  SELECT t1.*, t2.vlNota

  FROM tb_pedido t1

  LEFT JOIN silver.olist.avaliacao_pedido t2 ON t1.idPedido = t2.idPedido
  
),

tb_summary AS (

  SELECT idVendedor, avg(vlNota) AS avgNota, PERCENTILE(vlNota, 0.5) as medianNota,
         MIN(vlNota) AS minNota, MAX(vlNota) AS maxNota,
         COUNT(vlNota) / count(idPedido) as pctAvaliacao

  FROM tb_join

  GROUP BY idVendedor

)

SELECT '2018-01-01' as dtReference, *

FROM tb_summary

-- COMMAND ----------


