-- =====================================================================================
-- Validação do medalhão — executar:  py -3 sql/executar_sql.py sql/03_validacao.sql
-- =====================================================================================

-- 1) Contagem de linhas por tabela (bronze -> silver mostra o efeito de dedup + expectations)
SELECT 'bronze_clientes' AS tabela, count(*) AS linhas FROM workspace.telecom_ai.bronze_clientes
UNION ALL SELECT 'silver_clientes', count(*) FROM workspace.telecom_ai.silver_clientes
UNION ALL SELECT 'bronze_faturas', count(*) FROM workspace.telecom_ai.bronze_faturas
UNION ALL SELECT 'silver_faturas', count(*) FROM workspace.telecom_ai.silver_faturas
UNION ALL SELECT 'bronze_tickets', count(*) FROM workspace.telecom_ai.bronze_tickets
UNION ALL SELECT 'silver_tickets', count(*) FROM workspace.telecom_ai.silver_tickets
UNION ALL SELECT 'bronze_politicas', count(*) FROM workspace.telecom_ai.bronze_politicas
UNION ALL SELECT 'silver_politicas', count(*) FROM workspace.telecom_ai.silver_politicas
UNION ALL SELECT 'gold_cliente_360', count(*) FROM workspace.telecom_ai.gold_cliente_360
UNION ALL SELECT 'gold_kpis_uf_plano', count(*) FROM workspace.telecom_ai.gold_kpis_uf_plano;

-- 2) Expectations: registros aprovados/reprovados por regra na ÚLTIMA atualização (event log do pipeline)
SELECT
  e.dataset,
  e.name AS expectation,
  sum(e.passed_records) AS aprovados,
  sum(e.failed_records) AS reprovados
FROM (
  SELECT explode(from_json(details:flow_progress:data_quality:expectations,
         'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM event_log(TABLE(workspace.telecom_ai.silver_clientes))
  WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality IS NOT NULL
    AND origin.update_id = (SELECT max_by(origin.update_id, timestamp)
                            FROM event_log(TABLE(workspace.telecom_ai.silver_clientes))
                            WHERE event_type = 'create_update')
)
GROUP BY ALL ORDER BY 1, 2;

-- 3) Amostra da gold_cliente_360 (maiores riscos)
SELECT cliente_id, plano, uf, valor_mensal, tenure_meses, faturas_atrasadas_12m,
       n_tickets_90d, n_tickets_criticos_90d, churn, score_risco, faixa_risco,
       left(ultimo_ticket_texto, 60) AS ultimo_ticket
FROM workspace.telecom_ai.gold_cliente_360
ORDER BY score_risco DESC LIMIT 8;

-- 4) O sinal funciona? Taxa de churn por faixa de score
SELECT faixa_risco, count(*) AS clientes, sum(churn) AS churns,
       round(avg(churn), 3) AS taxa_churn, round(avg(score_risco), 3) AS score_medio
FROM workspace.telecom_ai.gold_cliente_360
GROUP BY faixa_risco ORDER BY score_medio;

-- 5) Top KPIs por UF x plano
SELECT * FROM workspace.telecom_ai.gold_kpis_uf_plano ORDER BY receita_em_risco DESC LIMIT 8;
