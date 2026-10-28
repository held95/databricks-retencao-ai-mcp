"""
04 - Genie space "Retenção Telecom" (text-to-SQL em pt-BR sobre as tabelas gold).

Cria (ou reaproveita, se já existir com o mesmo título) o space via API
`w.genie.create_space(serialized_space=...)` e faz uma pergunta de teste via Conversation API.

Uso: py -3 ai/04_genie_space.py
"""
import json
import uuid

from databricks.sdk import WorkspaceClient

TITULO = "Retenção Telecom"
WAREHOUSE_ID = "568abc53566f5fa8"
PASTA_RELATIVA = "retencao_ai_mcp"  # dentro da pasta do usuário logado
TABELAS = ["workspace.telecom_ai.gold_cliente_360",
           "workspace.telecom_ai.gold_kpis_uf_plano",
           "workspace.telecom_ai.gold_tickets_ia"]
PERGUNTAS = [
    "Qual a taxa de churn por UF?",
    "Quais os 10 clientes ativos com maior score de risco em SP?",
    "Qual a receita mensal em risco por plano?",
    "Qual a distribuição de sentimento dos tickets classificados pela IA por motivo?",
    "Quantos tickets de urgência alta existem por canal?",
]
INSTRUCOES = """\
* Responda sempre em português do Brasil. Valores monetários em R$ (reais), mensais.
* A foto dos dados é de 2026-09-30; churn = 1 significa que o cliente cancelou em out/2026.
* score_risco vai de 0 a 1 (heurística logística). faixa_risco: baixo (<0,3), medio (0,3–0,6), alto (>=0,6).
* "Clientes em risco" ou "a reter" = clientes ATIVOS (churn = 0) ordenados por score_risco desc.
* "Receita em risco" = soma de valor_mensal dos clientes com faixa_risco = 'alto' (coluna receita_em_risco em gold_kpis_uf_plano).
* Taxa de churn agregada deve ser recalculada como sum(clientes_churn)/sum(clientes), nunca média de taxa_churn.
* gold_tickets_ia contém só os ~200 tickets mais recentes classificados por LLM (sentimento, motivo, urgencia); junte com gold_cliente_360 por cliente_id.
* gold_kpis_uf_plano tem 1 linha por (uf, plano): para totais por UF ou por plano SEMPRE agregue com SUM(...) GROUP BY.
* Não existe PII nessas tabelas (sem nome/CPF).
"""
EXEMPLOS_SQL = [
    ("Taxa de churn por UF",
     "SELECT uf, sum(clientes_churn) AS clientes_churn, sum(clientes) AS clientes,\n"
     "       round(sum(clientes_churn) / sum(clientes), 4) AS taxa_churn\n"
     "FROM workspace.telecom_ai.gold_kpis_uf_plano\nGROUP BY uf\nORDER BY taxa_churn DESC"),
    ("Clientes ativos de alto risco com ticket de urgência alta",
     "SELECT c.cliente_id, c.uf, c.plano, c.score_risco, t.motivo, t.sentimento\n"
     "FROM workspace.telecom_ai.gold_cliente_360 c\n"
     "JOIN workspace.telecom_ai.gold_tickets_ia t ON t.cliente_id = c.cliente_id\n"
     "WHERE c.churn = 0 AND c.faixa_risco = 'alto' AND t.urgencia = 'alta'\nORDER BY c.score_risco DESC"),
]


def _id() -> str:
    return uuid.uuid4().hex


def _linhas(texto: str):
    ls = texto.splitlines(keepends=True)
    return ls or [texto]


def serializar() -> str:
    perguntas = sorted([{"id": _id(), "question": [p]} for p in PERGUNTAS], key=lambda x: x["id"])
    exemplos = sorted([{"id": _id(), "question": [q], "sql": _linhas(s)} for q, s in EXEMPLOS_SQL],
                      key=lambda x: x["id"])
    return json.dumps({
        "version": 2,
        "config": {"sample_questions": perguntas},
        "data_sources": {"tables": [{"identifier": t} for t in sorted(TABELAS)]},
        "instructions": {
            "text_instructions": [{"id": _id(), "content": _linhas(INSTRUCOES)}],
            "example_question_sqls": exemplos,
        },
    }, ensure_ascii=False)


def main():
    w = WorkspaceClient(profile="DEFAULT")
    pasta = f"/Workspace/Users/{w.current_user.me().user_name}/{PASTA_RELATIVA}"
    existente = [s for s in (w.genie.list_spaces().spaces or []) if s.title == TITULO]
    if existente:
        space_id = existente[0].space_id
        w.genie.update_space(space_id, serialized_space=serializar(), warehouse_id=WAREHOUSE_ID)
        print("Space já existe — instruções atualizadas:", space_id)
    else:
        sp = w.genie.create_space(
            warehouse_id=WAREHOUSE_ID, serialized_space=serializar(), title=TITULO, parent_path=pasta,
            description="Perguntas em linguagem natural (pt-BR) sobre churn, risco e tickets classificados por IA.")
        space_id = sp.space_id
        print("Space criado:", space_id)
    print(f"URL: {w.config.host}/genie/rooms/{space_id}")

    pergunta = "Quais as 5 UFs com maior receita em risco?"
    msg = w.genie.start_conversation_and_wait(space_id, pergunta)
    print(f"\nPergunta: {pergunta}\nStatus: {msg.status}")
    for a in msg.attachments or []:
        if a.text:
            print("Texto:", a.text.content)
        if a.query:
            print("Descrição:", a.query.description)
            print("SQL:\n" + a.query.query)
            res = w.genie.get_message_attachment_query_result(
                space_id, msg.conversation_id, msg.message_id, a.attachment_id)
            sr = res.statement_response
            if sr and sr.result and sr.result.data_array:
                print(" | ".join(c.name for c in sr.manifest.schema.columns))
                for l in sr.result.data_array[:10]:
                    print(" | ".join(map(str, l)))


if __name__ == "__main__":
    main()
