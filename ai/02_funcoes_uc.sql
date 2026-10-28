-- =====================================================================
-- 02 - Funções do Unity Catalog usadas como TOOLS de agentes
-- Os COMMENTs da função e dos parâmetros viram a descrição da tool
-- (managed MCP server /api/2.0/mcp/functions/workspace/telecom_ai e UCFunctionToolkit).
-- =====================================================================
CREATE OR REPLACE FUNCTION workspace.telecom_ai.get_cliente_360(
  p_cliente_id STRING COMMENT 'Identificador do cliente no formato C00000 (ex.: C00042)'
)
RETURNS TABLE (
  cliente_id STRING, plano STRING, uf STRING, cidade STRING, valor_mensal DECIMAL(10,2),
  tenure_meses INT, faturas_atrasadas_12m BIGINT, n_tickets_90d BIGINT, n_tickets_criticos_90d BIGINT,
  ultimo_ticket_categoria STRING, ultimo_ticket_texto STRING, score_risco DOUBLE, faixa_risco STRING
)
COMMENT 'Retorna a visao 360 de UM cliente: plano, UF/cidade, mensalidade, meses de casa, faturas atrasadas em 12 meses, tickets nos ultimos 90 dias (total e criticos), ultimo ticket (categoria e texto) e o score de risco de churn (0 a 1) com a faixa (baixo/medio/alto). Use quando o usuario perguntar sobre um cliente especifico pelo ID. Nao retorna PII (nome/CPF).'
RETURN
  SELECT cliente_id, plano, uf, cidade, valor_mensal, tenure_meses, faturas_atrasadas_12m,
         n_tickets_90d, n_tickets_criticos_90d, ultimo_ticket_categoria, ultimo_ticket_texto,
         score_risco, faixa_risco
  FROM workspace.telecom_ai.gold_cliente_360
  WHERE cliente_id = upper(trim(p_cliente_id));

CREATE OR REPLACE FUNCTION workspace.telecom_ai.clientes_alto_risco(
  p_uf STRING COMMENT 'Sigla da UF com 2 letras (ex.: SP, RJ, MG). Use NULL ou string vazia para todas as UFs',
  p_limite INT COMMENT 'Quantidade maxima de clientes a retornar (ex.: 10). Valores entre 1 e 100'
)
RETURNS TABLE (
  cliente_id STRING, uf STRING, cidade STRING, plano STRING, valor_mensal DECIMAL(10,2), tenure_meses INT,
  faturas_atrasadas_12m BIGINT, n_tickets_90d BIGINT, ultimo_ticket_categoria STRING,
  score_risco DOUBLE, faixa_risco STRING
)
COMMENT 'Lista os clientes ATIVOS (ainda nao cancelados) com maior risco de churn, ordenados por score_risco decrescente, opcionalmente filtrando por UF. Use para priorizar campanhas/acoes de retencao (ex.: "top 10 clientes em risco em SP").'
RETURN
  -- LIMIT nao aceita parametro em SQL UDF => ranking com row_number + filtro
  SELECT cliente_id, uf, cidade, plano, valor_mensal, tenure_meses, faturas_atrasadas_12m,
         n_tickets_90d, ultimo_ticket_categoria, score_risco, faixa_risco
  FROM (
    SELECT *, row_number() OVER (ORDER BY score_risco DESC, cliente_id) AS rn
    FROM workspace.telecom_ai.gold_cliente_360
    WHERE churn = 0
      AND (p_uf IS NULL OR trim(p_uf) = '' OR uf = upper(trim(p_uf)))
  )
  WHERE rn <= greatest(1, least(coalesce(p_limite, 10), 100))
  ORDER BY score_risco DESC;

CREATE OR REPLACE FUNCTION workspace.telecom_ai.resumo_risco_uf()
RETURNS TABLE (
  uf STRING, clientes BIGINT, clientes_churn BIGINT, taxa_churn DOUBLE, clientes_alto_risco BIGINT,
  score_risco_medio DOUBLE, receita_mensal DECIMAL(14,2), receita_em_risco DECIMAL(14,2)
)
COMMENT 'Resumo de risco de churn por UF: total de clientes, clientes que cancelaram, taxa de churn, quantidade de clientes em faixa de risco alto, score medio e receita mensal total e em risco (R$). Ordenado pela receita em risco decrescente. Use para visao executiva/regional.'
RETURN
  SELECT uf, sum(clientes) AS clientes, sum(clientes_churn) AS clientes_churn,
         round(sum(clientes_churn) / sum(clientes), 4) AS taxa_churn,
         sum(clientes_alto_risco) AS clientes_alto_risco,
         round(sum(score_risco_medio * clientes) / sum(clientes), 4) AS score_risco_medio,
         CAST(sum(receita_mensal) AS DECIMAL(14,2)) AS receita_mensal,
         CAST(sum(receita_em_risco) AS DECIMAL(14,2)) AS receita_em_risco
  FROM workspace.telecom_ai.gold_kpis_uf_plano
  GROUP BY uf
  ORDER BY receita_em_risco DESC;

-- Testes
SELECT * FROM workspace.telecom_ai.clientes_alto_risco('SP', 5);
SELECT * FROM workspace.telecom_ai.clientes_alto_risco(NULL, 3);
SELECT * FROM workspace.telecom_ai.resumo_risco_uf() LIMIT 8;
SELECT * FROM workspace.telecom_ai.get_cliente_360('C00042');
SELECT * FROM workspace.telecom_ai.get_cliente_360((SELECT cliente_id FROM workspace.telecom_ai.clientes_alto_risco('SP', 1)));
