-- Databricks notebook source
SELECT *
FROM silver.olist.pedido
WHERE dtPedido < '2018-01-01' 
AND dtPedido >= add_months('2018-01-01', -6)


-- COMMAND ----------

WITH tb_join AS (

  SELECT t2.*, t3.idVendedor
  FROM silver.olist.pedido t1

  LEFT JOIN silver.olist.pagamento_pedido t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.item_pedido t3
  ON t1.idPedido = t3.idPedido

  WHERE t1.dtPedido < '2018-01-01' 
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t3.idVendedor IS NOT NULL
),

tb_group AS(
  SELECT idVendedor, 
         descTipoPagamento,
         COUNT(DISTINCT idPedido) as qtdePedidoMeioPagamento,
         sum(vlPagamento) as vlPEdidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
)

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

-- COMMAND ----------


