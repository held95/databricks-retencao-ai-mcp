# 01 — Camada de dados (medalhão com Lakeflow Declarative Pipelines)

## Arquitetura

```
data_gen/gerar_dados.py (stdlib, seed 42)
        │  CSV / JSONL
        ▼
/Volumes/workspace/telecom_ai/raw/landing/<entidade>/       (UC Volume gerenciado)
        │  Auto Loader (cloudFiles) — streaming tables
        ▼
BRONZE  bronze_clientes · bronze_faturas · bronze_tickets · bronze_politicas
        │  tipagem, trim, dedup por chave, expectations (expect / expect_or_drop)
        ▼
SILVER  silver_clientes · silver_faturas · silver_tickets   (materialized views)
        silver_politicas                                     (streaming table + CDF → Vector Search)
        │  agregações + score heurístico
        ▼
GOLD    gold_cliente_360 · gold_kpis_uf_plano               (materialized views)
        │
        ▼
GOVERNANÇA  column mask mask_cpf em silver_clientes.cpf · vw_clientes_seguro · tags · DESCRIBE HISTORY
```

- **Pipeline**: `pipeline_retencao_telecom` — serverless, catálogo `workspace`, schema `telecom_ai`,
  API Python `from pyspark import pipelines as dp`, canal CURRENT, modo desenvolvimento.
- **ID**: `b5b70270-98dd-4ff5-b70e-a088d04c21fb`
  — https://dbc-1d83b389-044f.cloud.databricks.com/pipelines/b5b70270-98dd-4ff5-b70e-a088d04c21fb
- **Job** `job_retencao_diario` (ID `1088903333559427`, cron 06:00 America/Sao_Paulo, **PAUSED**) — dispara o pipeline
  — https://dbc-1d83b389-044f.cloud.databricks.com/jobs/1088903333559427
- **Deploy**: Declarative Automation Bundle (`databricks.yml` na raiz). Código sincronizado em
  `/Workspace/Users/<seu_usuario>/retencao_ai_mcp/.bundle/dev/files/pipeline/`.

## Dados sintéticos

| Entidade  | Formato | Linhas (bronze) | Sujeira injetada |
|-----------|---------|-----------------|------------------|
| clientes  | CSV     | 806             | 2 `cliente_id` nulos, 1 UF inválida (`XX`), 3 duplicatas exatas |
| faturas   | CSV     | 8.937 (out/2025–set/2026) | 5 valores negativos, 3 `cliente_id` nulos, 10 duplicatas |
| tickets   | JSONL   | 1.594           | 3 textos vazios, 2 `cliente_id` nulos, 5 duplicatas |
| politicas | JSONL   | 15              | — (3–4 frases cada, com regras de elegibilidade) |

Tickets têm texto livre pt-BR em 5 categorias (`cobranca`, `sinal_qualidade`, `portabilidade`,
`cancelamento`, `upgrade`) e 3 sentimentos (negativo/neutro/positivo).

**Sinal de churn embutido** (label = cancelou em out/2026, logo após a foto de 30/09/2026):
logística latente sobre nº de faturas atrasadas, tickets críticos (cancelamento/sinal) nos últimos 90 dias,
tenure < 12 meses, valor do plano e um fator de insatisfação latente + ruído gaussiano.

## Qualidade (expectations)

| Tabela | Regra | Tipo |
|---|---|---|
| silver_clientes | `cliente_id_nao_nulo`, `uf_valida` | expect_or_drop |
| silver_clientes | `cpf_formato_valido`, `valor_mensal_positivo` | expect (monitora) |
| silver_faturas | `fatura_id_nao_nulo`, `cliente_id_nao_nulo`, `valor_positivo` | expect_or_drop |
| silver_faturas | `vencimento_preenchido` | expect |
| silver_tickets | `cliente_id_nao_nulo`, `texto_nao_vazio` | expect_or_drop |
| silver_tickets | `categoria_conhecida` | expect |
| silver_politicas | `politica_id_nao_nulo` / `texto_minimo` | expect_or_drop / expect |

Dedup: `row_number()` por chave de negócio, mantendo o registro ingerido mais recente.
Não usamos `expect_or_fail` (dados sujos são esperados e não devem parar o pipeline).

## score_risco (heurística transparente)

```
z = -3.2 + 0.45·faturas_atrasadas_12m + 0.25·n_tickets_90d + 0.60·n_tickets_criticos_90d
         + 1.0·[tenure_meses < 12] + 0.008·(valor_mensal − 100)
score_risco = 1 / (1 + e^(−z))         faixa: baixo < 0,3 ≤ medio < 0,6 ≤ alto
```
Pesos escolhidos à mão (explicáveis para o negócio); não usa o label `churn`, logo não há vazamento.

## Comandos

```bash
# warehouse: env DATABRICKS_WAREHOUSE_ID (padrão 568abc53566f5fa8)
py -3 data_gen/gerar_dados.py                       # gera data_gen/output/<entidade>/
py -3 data_gen/checar_sinal_local.py                # prevê contagens e valida o sinal localmente
py -3 sql/executar_sql.py sql/01_setup.sql         # schema + volume + função mask_cpf (antes do pipeline!)
for e in clientes faturas tickets politicas; do
  databricks fs cp -r --overwrite data_gen/output/$e dbfs:/Volumes/workspace/telecom_ai/raw/landing/$e
done
databricks bundle validate && databricks bundle deploy
databricks bundle run pipeline_retencao --full-refresh-all
databricks bundle run job_retencao_diario          # (opcional) mesmo pipeline via Lakeflow Job
py -3 sql/executar_sql.py sql/02_governanca.sql     # vw_clientes_seguro, tags, histórico, lineage
py -3 sql/executar_sql.py sql/03_validacao.sql      # contagens, expectations, churn por faixa
```

## Governança

- `workspace.telecom_ai.mask_cpf(cpf)` — UDF SQL: admins veem o CPF; demais veem `***.***.133-**`.
- **Column mask aplicada no próprio pipeline** (schema DDL de `silver_clientes`:
  `cpf STRING MASK workspace.telecom_ai.mask_cpf`) — confirmada em `information_schema.column_masks`;
  `SELECT cpf FROM silver_clientes` retorna `***.***.133-**`. A função é criada em `01_setup.sql` porque precisa
  existir antes da 1ª execução do pipeline. (`ALTER MATERIALIZED VIEW ... ALTER COLUMN cpf SET MASK` também funcionou.)
- `vw_clientes_seguro` — visão governada (CPF mascarado + nome abreviado, ex.: `Gabriela D.`), tag
  `classificacao=pii_mascarada`, tag de coluna `pii=cpf_mascarado`; schema com tags `projeto/dominio/dados`.
- Comentários de tabela e de coluna em todas as tabelas silver/gold (via `schema=` / `comment=` do pipeline).
- `DESCRIBE HISTORY silver_politicas` mostra CREATE TABLE → DLT SETUP → STREAMING UPDATE (15 linhas).
  A lineage aparece no Catalog Explorer; `system.access.table_lineage` ainda retornou 0 linhas logo após a
  execução (atraso de ingestão das system tables).

## Resultados (execução real em 27/10/2026, workspace `dbc-1d83b389-044f`)

- `databricks bundle deploy` criou o pipeline e o job; `bundle run --full-refresh-all` → update
  `63ddcbde-f09c-4aeb-a492-7af517e31a7c` **COMPLETED** na 1ª tentativa (~1 min, serverless, DLT 18.3).
  Re-execução incremental `54c2acc8…` (com a column mask) e execução via job (run `393429902956739`): **SUCCESS**.

### Contagens

| Entidade | Bronze | Silver | Removidas |
|---|---|---|---|
| clientes | 806 | 800 | 3 duplicatas (dedup) + 2 ids nulos + 1 UF inválida (expectations) |
| faturas | 8.937 | 8.919 | 10 duplicatas + 5 valores negativos + 3 ids nulos |
| tickets | 1.594 | 1.584 | 5 duplicatas + 3 textos vazios + 2 ids nulos |
| politicas | 15 | 15 | — |

Gold: `gold_cliente_360` = 800 linhas (1 por cliente); `gold_kpis_uf_plano` = 55 linhas (UF × plano).

### Expectations (event log, `event_log(TABLE(...))`, última atualização)

| Tabela | Regra | Aprovados | Reprovados |
|---|---|---|---|
| silver_clientes | cliente_id_nao_nulo (drop) | 801 | **2** |
| silver_clientes | uf_valida (drop) | 802 | **1** |
| silver_clientes | cpf_formato_valido / valor_mensal_positivo | 803 | 0 |
| silver_faturas | cliente_id_nao_nulo (drop) | 8.924 | **3** |
| silver_faturas | valor_positivo (drop) | 8.922 | **5** |
| silver_faturas | fatura_id_nao_nulo / vencimento_preenchido | 8.927 | 0 |
| silver_tickets | cliente_id_nao_nulo (drop) | 1.587 | **2** |
| silver_tickets | texto_nao_vazio (drop) | 1.586 | **3** |
| silver_tickets | categoria_conhecida | 1.589 | 0 |
| silver_politicas | politica_id_nao_nulo / texto_minimo | 15 | 0 (full refresh) |

### O sinal funciona — churn por faixa de `score_risco` (churn geral 132/800 = 16,5%)

| faixa | clientes | churns | taxa churn | score médio |
|---|---|---|---|---|
| baixo | 700 | 87 | 12,4% | 0,097 |
| medio | 71 | 22 | 31,0% | 0,422 |
| alto | 29 | 23 | **79,3%** | 0,764 |

Amostra (maior risco): `C00778` — Pós 50GB/MG, tenure 2 meses, 11 tickets em 90d (5 críticos), score 0,972,
último ticket "Não aguento mais, quero cancelar tudo: internet e móvel." (churn = 1).
Maior receita em risco: SP × Pós Ilimitado (R$ 479,70/mês, 3 clientes em risco alto).

### Histórico
Um primeiro workspace (trial `dbc-4ce01da8-b703`) estava sem créditos (pipeline/warehouse bloqueados e Jobs API 403);
o projeto foi reimplantado sem alterações de código no novo workspace Free Edition, onde tudo funcionou.
