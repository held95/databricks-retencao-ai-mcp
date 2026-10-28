-- =====================================================================================
-- Governança Unity Catalog — Copiloto de Retenção Telecom
-- Executar após o pipeline:  py -3 sql/executar_sql.py sql/02_governanca.sql
-- =====================================================================================

-- 1) A função workspace.telecom_ai.mask_cpf é criada em sql/01_setup.sql (precisa existir ANTES do pipeline),
--    e é aplicada como COLUMN MASK em silver_clientes.cpf na própria definição do pipeline
--    (schema DDL: `cpf STRING MASK workspace.telecom_ai.mask_cpf`). Alternativa equivalente fora do pipeline:
--    ALTER MATERIALIZED VIEW workspace.telecom_ai.silver_clientes ALTER COLUMN cpf SET MASK workspace.telecom_ai.mask_cpf;

-- 2) Visão governada: consumo de clientes com CPF mascarado e nome abreviado.
--    (Defesa em profundidade: a visão também aplica mask_cpf e abrevia o nome; é a interface de consumo
--     recomendada para analistas, Genie e agentes.)
CREATE OR REPLACE VIEW workspace.telecom_ai.vw_clientes_seguro (
  cliente_id COMMENT 'Identificador do cliente',
  nome_abreviado COMMENT 'Primeiro nome + inicial do sobrenome',
  cpf_mascarado COMMENT 'CPF mascarado via workspace.telecom_ai.mask_cpf',
  uf, cidade, plano, valor_mensal, data_adesao, canal_aquisicao, status
)
COMMENT 'Visão governada de clientes: PII mascarada (CPF via mask_cpf, nome abreviado). Use esta visão em vez de silver_clientes.'
AS SELECT
  cliente_id,
  concat(split(nome, ' ')[0], ' ', left(split(nome, ' ')[1], 1), '.') AS nome_abreviado,
  workspace.telecom_ai.mask_cpf(cpf) AS cpf_mascarado,
  uf, cidade, plano, valor_mensal, data_adesao, canal_aquisicao, status
FROM workspace.telecom_ai.silver_clientes;

-- 3) Tags de governança (classificação de dados)
ALTER SCHEMA workspace.telecom_ai SET TAGS ('projeto' = 'retencao_ai_mcp', 'dominio' = 'telecom', 'dados' = 'sinteticos');
ALTER VIEW workspace.telecom_ai.vw_clientes_seguro SET TAGS ('classificacao' = 'pii_mascarada');
ALTER TABLE workspace.telecom_ai.vw_clientes_seguro ALTER COLUMN cpf_mascarado SET TAGS ('pii' = 'cpf_mascarado');
COMMENT ON VOLUME workspace.telecom_ai.raw IS 'Landing zone de arquivos brutos (CSV/JSONL) do projeto de retenção — landing/<entidade>/';

-- 4) Auditoria: histórico Delta e linhagem
DESCRIBE HISTORY workspace.telecom_ai.silver_politicas;
-- Linhagem de colunas/tabelas (system tables; pode estar indisponível no Free Edition):
SELECT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS ultima_vez
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'workspace.telecom_ai.%'
GROUP BY ALL ORDER BY target_table_full_name;
