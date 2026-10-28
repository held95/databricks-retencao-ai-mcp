-- Setup do Unity Catalog (idempotente).  py -3 sql/executar_sql.py sql/01_setup.sql
CREATE SCHEMA IF NOT EXISTS workspace.telecom_ai;
COMMENT ON SCHEMA workspace.telecom_ai IS 'Copiloto de Retenção Telecom: medalhão (bronze/silver/gold) de clientes, faturas, tickets e políticas + ativos de IA/MCP. Dados sintéticos.';
CREATE VOLUME IF NOT EXISTS workspace.telecom_ai.raw
  COMMENT 'Landing zone de arquivos brutos (CSV/JSONL) do projeto de retenção';

-- Função de máscara de CPF (PII) — usada como COLUMN MASK no pipeline (silver_clientes.cpf).
CREATE OR REPLACE FUNCTION workspace.telecom_ai.mask_cpf(cpf STRING)
RETURNS STRING
COMMENT 'Mascara CPF (PII): mostra só o 3º bloco (***.***.789-**). Admins veem o valor completo.'
RETURN CASE
  WHEN cpf IS NULL THEN NULL
  WHEN is_account_group_member('admins') THEN cpf
  ELSE concat('***.***.', substr(cpf, 9, 3), '-**')
END;
