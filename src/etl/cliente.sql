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
  AND idVendedor IS NOT NULL
  
),

tb_group AS (

  SELECT 
  idVendedor,
  COUNT(DISTINCT descUF) as qtdUFsPedidos,

  COUNT(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteSC,
  COUNT(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteRO,
  COUNT(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClientePI,
  COUNT(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteAM,
  COUNT(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteRR,
  COUNT(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteGO,
  COUNT(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteTO,
  COUNT(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteMT,
  COUNT(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteSP,
  COUNT(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteES,
  COUNT(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClientePB,
  COUNT(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteRS,
  COUNT(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteMS,
  COUNT(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteAL,
  COUNT(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteMG,
  COUNT(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClientePA,
  COUNT(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteBA,
  COUNT(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteSE,
  COUNT(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClientePE,
  COUNT(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteCE,
  COUNT(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteRN,
  COUNT(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteRJ,
  COUNT(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteMA,
  COUNT(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteAC,
  COUNT(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteDF,
  COUNT(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClientePR,
  COUNT(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) / COUNT (DISTINCT idPedido) AS pctClienteAP

  FROM tb_join
  GROUP BY idVendedor
  
)

SELECT '2008-01-01' AS dtReference, *

FROM tb_group



-- COMMAND ----------


