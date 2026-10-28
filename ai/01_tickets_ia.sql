-- =====================================================================
-- 01 - AI Functions: enriquecimento de tickets com LLM (ai_query)
-- Classifica os ~200 tickets mais recentes em sentimento, motivo e urgência
-- usando o Foundation Model databricks-meta-llama-3-3-70b-instruct com
-- saída estruturada (responseFormat => STRUCT), direto em SQL no warehouse.
-- =====================================================================
CREATE OR REPLACE TABLE workspace.telecom_ai.gold_tickets_ia
COMMENT 'Gold: tickets enriquecidos por LLM (ai_query + Llama 3.3 70B, saida estruturada): sentimento, motivo e urgencia. Amostra dos 200 tickets mais recentes.'
TBLPROPERTIES ('camada' = 'gold', 'origem' = 'ai_query')
AS
WITH recentes AS (
  SELECT ticket_id, cliente_id, data_abertura, canal, categoria, texto
  FROM workspace.telecom_ai.silver_tickets
  ORDER BY data_abertura DESC
  LIMIT 200
),
classificados AS (
  SELECT *,
    ai_query(
      'databricks-meta-llama-3-3-70b-instruct',
      CONCAT(
        'Voce e um analista de retencao de uma operadora de telecom brasileira. ',
        'Classifique a reclamacao do cliente abaixo. Regras: ',
        'sentimento = positivo | neutro | negativo | muito_negativo; ',
        'motivo = cobranca | sinal_qualidade | portabilidade | cancelamento | upgrade | atendimento | outro; ',
        'urgencia = baixa | media | alta (alta = ameaca de cancelar, portabilidade, Anatel/Reclame Aqui ou cobranca indevida recorrente). ',
        'Responda apenas com os campos pedidos. Reclamacao: ', texto),
      responseFormat => 'STRUCT<classificacao:STRUCT<sentimento:STRING, motivo:STRING, urgencia:STRING>>',
      failOnError => false
    ) AS resp
  FROM recentes
)
SELECT
  ticket_id, cliente_id, data_abertura, canal, categoria AS categoria_original, texto,
  lower(get_json_object(resp.result, '$.sentimento')) AS sentimento,
  lower(get_json_object(resp.result, '$.motivo'))     AS motivo,
  lower(get_json_object(resp.result, '$.urgencia'))   AS urgencia,
  resp.errorMessage AS erro_llm,
  current_timestamp() AS classificado_em
FROM classificados;

-- Distribuições
SELECT sentimento, count(*) AS n FROM workspace.telecom_ai.gold_tickets_ia GROUP BY 1 ORDER BY 2 DESC;
SELECT motivo, count(*) AS n FROM workspace.telecom_ai.gold_tickets_ia GROUP BY 1 ORDER BY 2 DESC;
SELECT urgencia, count(*) AS n FROM workspace.telecom_ai.gold_tickets_ia GROUP BY 1 ORDER BY 2 DESC;
-- Concordância motivo (LLM) x categoria original
SELECT round(avg(CASE WHEN motivo = categoria_original THEN 1 ELSE 0 END), 3) AS concordancia_motivo, count(erro_llm) AS erros FROM workspace.telecom_ai.gold_tickets_ia;
SELECT ticket_id, sentimento, motivo, urgencia, left(texto, 110) AS texto FROM workspace.telecom_ai.gold_tickets_ia LIMIT 6;
