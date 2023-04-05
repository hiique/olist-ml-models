-- Databricks notebook source
SELECT DISTINCT t1.idPedido, t2.idVendedor, t1.dtPedido
FROM silver.olist.pedido t1

LEFT JOIN silver.olist.item_pedido t2 ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido <'2018-01-01' AND t1.dtPedido >= ADD_MONTHS('2018-01-01', -6)
AND idVendedor IS NOT NULL


-- COMMAND ----------

WITH tb_pedidos AS (

  SELECT 
      DISTINCT 
      t1.idPedido,
      t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL

),

tb_join AS (

  SELECT 
        t1.idVendedor,
        t2.*         

  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

),

tb_group AS(
  SELECT idVendedor, 
         descTipoPagamento,
         COUNT(DISTINCT idPedido) as qtdePedidoMeioPagamento,
         sum(vlPagamento) as vlPEdidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
),

tb_summary AS ( 

  SELECT idVendedor,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido



FROM tb_group
GROUP BY 1
),

tb_cartao AS (

  SELECT idVendedor,
         AVG(nrParcelas) AS avgQtdeParcelas,
         PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
         MAX(nrParcelas) AS maxQtdeParcelas,
         MIN(nrParcelas) AS minQtdeParcelas

  FROM tb_join
  WHERE descTipoPagamento = 'credit_card'
  GROUP BY idVendedor

)

SELECT 
       '2018-01-01' AS dtReference,
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.maxQtdeParcelas,
       t2.minQtdeParcelas

FROM tb_summary as t1

LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------


