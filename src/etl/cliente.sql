-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT t1.idPedido, t1.idCliente, t2.idVendedor,
                  t3.descUF

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente AS T3
  ON t1.idCliente = t3.idCliente

  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= ADD_MONTHS('2018-01-01', -6)
  
)

SELECT * FROM tb_join

-- COMMAND ----------


