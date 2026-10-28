"""
Servidor MCP customizado do Copiloto de Retenção Telecom.

Roda como Databricks App (`mcp-telecom-retencao`) expondo o protocolo MCP via
Streamable HTTP em `/mcp`. É a camada de *ação* do sistema multiagente: os
servidores MCP gerenciados do Databricks (UC Functions, Vector Search, Genie)
cuidam da leitura/análise; este servidor executa operações de escrita
controladas (abrir caso de retenção) e de observabilidade (status do pipeline).

Autenticação:
- O cliente chega pelo proxy do Databricks Apps com OAuth (usuário ou SP com CAN_USE no app).
- Dentro do app, as chamadas ao Databricks usam o *service principal* do app
  (DATABRICKS_CLIENT_ID/SECRET injetados pela plataforma) — princípio do menor privilégio:
  o SP só tem USE CATALOG/USE SCHEMA/SELECT/MODIFY no schema `workspace.telecom_ai`
  e CAN_USE no SQL warehouse.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    Format,
    StatementParameterListItem,
    StatementState,
)
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mcp-telecom-retencao")

# --------------------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------------------
CATALOGO = os.getenv("UC_CATALOGO", "workspace")
SCHEMA = os.getenv("UC_SCHEMA", "telecom_ai")
TABELA_CASOS = f"{CATALOGO}.{SCHEMA}.casos_retencao"
# DATABRICKS_WAREHOUSE_ID vem do recurso `sql-warehouse` declarado no app (valueFrom no app.yaml)
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
FILTRO_PIPELINE = os.getenv("FILTRO_PIPELINE", "retencao")

PRIORIDADES_VALIDAS = {"baixa", "media", "alta", "critica"}

DDL_CASOS = f"""
CREATE TABLE IF NOT EXISTS {TABELA_CASOS} (
  caso_id          STRING    COMMENT 'Identificador único do caso (UUID)',
  cliente_id       STRING    COMMENT 'Cliente alvo da ação de retenção (FK gold_cliente_360)',
  motivo           STRING    COMMENT 'Motivo/diagnóstico de risco de churn',
  oferta_sugerida  STRING    COMMENT 'Oferta de retenção sugerida (baseada nas políticas)',
  prioridade       STRING    COMMENT 'baixa | media | alta | critica',
  status           STRING    COMMENT 'aberto | em_andamento | fechado',
  origem           STRING    COMMENT 'Quem abriu o caso (ex.: agente_mcp)',
  criado_em        TIMESTAMP COMMENT 'Data/hora de abertura (UTC)'
)
COMMENT 'Casos de retenção abertos pelo agente via servidor MCP customizado (Databricks App mcp-telecom-retencao).'
"""

_ws: WorkspaceClient | None = None
_tabela_ok = False


def ws() -> WorkspaceClient:
    """Cliente do workspace autenticado como o service principal do app (ou perfil local em dev)."""
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


def executar_sql(sql: str, parametros: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Executa SQL **parametrizado** no SQL warehouse (Statement Execution API).

    Nunca interpolamos input do usuário/LLM no texto SQL: tudo vai em `parameters`
    (marcadores `:nome`), o que elimina SQL injection vindo de prompt injection.
    """
    if not WAREHOUSE_ID:
        raise RuntimeError("Defina DATABRICKS_WAREHOUSE_ID (no app vem do recurso sql-warehouse).")
    params = [
        StatementParameterListItem(name=k, value=None if v is None else str(v),
                                   type="INT" if isinstance(v, int) else "STRING")
        for k, v in (parametros or {}).items()
    ]
    resp = ws().statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        parameters=params or None,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    estado = resp.status.state if resp.status else None
    # Se o warehouse estava frio, o statement pode ainda estar rodando: faz polling.
    while estado in (StatementState.PENDING, StatementState.RUNNING):
        import time

        time.sleep(2)
        resp = ws().statement_execution.get_statement(resp.statement_id)
        estado = resp.status.state if resp.status else None
    if estado != StatementState.SUCCEEDED:
        erro = resp.status.error.message if resp.status and resp.status.error else str(estado)
        raise RuntimeError(f"Falha no SQL: {erro}")
    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return []
    colunas = [c.name for c in resp.manifest.schema.columns]
    linhas = (resp.result.data_array if resp.result else None) or []
    return [dict(zip(colunas, linha)) for linha in linhas]


def garantir_tabela() -> None:
    """Cria a tabela de casos se ainda não existir (idempotente, executa 1x por processo)."""
    global _tabela_ok
    if not _tabela_ok:
        executar_sql(DDL_CASOS)
        _tabela_ok = True


# --------------------------------------------------------------------------------------
# Servidor MCP
# --------------------------------------------------------------------------------------
PORTA = int(os.getenv("DATABRICKS_APP_PORT", "8000"))

# FastMCP (SDK oficial `mcp` 1.x). stateless_http: cada request é independente,
# o que funciona bem atrás do proxy do Databricks Apps e com restarts/réplicas.
mcp = FastMCP(
    name="mcp-telecom-retencao",
    host="0.0.0.0",
    port=PORTA,
    streamable_http_path="/mcp",
    stateless_http=True,
    instructions=(
        "Servidor de AÇÕES de retenção. Use `abrir_caso_retencao` somente depois de "
        "analisar o risco do cliente e escolher uma política de retenção. "
        "Use `listar_casos_abertos` para evitar casos duplicados e "
        "`status_pipeline_retencao` para checar se os dados estão atualizados."
    ),
)


@mcp.tool()
async def abrir_caso_retencao(cliente_id: str, motivo: str, oferta_sugerida: str, prioridade: str) -> dict:
    """Abre um caso de retenção para um cliente em risco de churn.

    Grava uma linha na tabela `workspace.telecom_ai.casos_retencao` com status 'aberto'.

    Args:
        cliente_id: ID do cliente (ex.: 'C00042'), como aparece em gold_cliente_360.
        motivo: Diagnóstico resumido do porquê o cliente está em risco (máx. 1000 caracteres).
        oferta_sugerida: Oferta de retenção a aplicar, citando a política (máx. 1000 caracteres).
        prioridade: Uma de 'baixa', 'media', 'alta' ou 'critica'.

    Returns:
        Dicionário com caso_id, status e dados gravados.
    """
    prioridade_norm = (prioridade or "").strip().lower().replace("é", "e").replace("í", "i")
    if prioridade_norm not in PRIORIDADES_VALIDAS:
        raise ValueError(f"prioridade inválida '{prioridade}'. Use uma de: {sorted(PRIORIDADES_VALIDAS)}")
    cliente_id = (cliente_id or "").strip()
    if not cliente_id or len(cliente_id) > 64:
        raise ValueError("cliente_id obrigatório (até 64 caracteres)")

    caso_id = str(uuid.uuid4())
    agora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _gravar() -> None:
        garantir_tabela()
        executar_sql(
            f"""INSERT INTO {TABELA_CASOS}
                (caso_id, cliente_id, motivo, oferta_sugerida, prioridade, status, origem, criado_em)
                VALUES (:caso_id, :cliente_id, :motivo, :oferta, :prioridade, 'aberto', 'agente_mcp',
                        CAST(:criado_em AS TIMESTAMP))""",
            {
                "caso_id": caso_id,
                "cliente_id": cliente_id,
                "motivo": (motivo or "")[:1000],
                "oferta": (oferta_sugerida or "")[:1000],
                "prioridade": prioridade_norm,
                "criado_em": agora,
            },
        )

    await asyncio.to_thread(_gravar)
    log.info("Caso %s aberto para cliente %s (prioridade=%s)", caso_id, cliente_id, prioridade_norm)
    return {
        "caso_id": caso_id,
        "status": "aberto",
        "cliente_id": cliente_id,
        "prioridade": prioridade_norm,
        "criado_em_utc": agora,
        "tabela": TABELA_CASOS,
    }


@mcp.tool()
async def listar_casos_abertos(limite: int = 10) -> list[dict]:
    """Lista os casos de retenção com status 'aberto', do mais recente para o mais antigo.

    Args:
        limite: Quantidade máxima de casos a retornar (1 a 100, padrão 10).
    """
    limite = max(1, min(int(limite), 100))

    def _ler() -> list[dict]:
        garantir_tabela()
        return executar_sql(
            f"""SELECT caso_id, cliente_id, motivo, oferta_sugerida, prioridade, status,
                       CAST(criado_em AS STRING) AS criado_em
                FROM {TABELA_CASOS}
                WHERE status = 'aberto'
                ORDER BY criado_em DESC
                LIMIT :limite""",
            {"limite": limite},
        )

    return await asyncio.to_thread(_ler)


@mcp.tool()
async def status_pipeline_retencao() -> dict:
    """Informa o estado da última atualização do pipeline Lakeflow (Declarative Pipelines) de retenção.

    Procura pipelines cujo nome contém 'retencao' e retorna o estado do último update
    (ex.: COMPLETED, FAILED, RUNNING), para o agente saber se os dados gold estão frescos.
    """

    def _status() -> dict:
        w = ws()
        try:
            pipelines = [p for p in w.pipelines.list_pipelines() if FILTRO_PIPELINE in (p.name or "").lower()]
        except Exception as e:  # noqa: BLE001
            return {"encontrado": False, "erro": f"Sem permissão ou falha ao listar pipelines: {e}"}
        if not pipelines:
            return {
                "encontrado": False,
                "mensagem": f"Nenhum pipeline com '{FILTRO_PIPELINE}' no nome visível para o service principal do app.",
            }
        resultado = []
        for p in pipelines:
            item: dict[str, Any] = {"pipeline_id": p.pipeline_id, "nome": p.name, "estado_pipeline": str(p.state.value if p.state else None)}
            try:
                updates = w.pipelines.list_updates(pipeline_id=p.pipeline_id, max_results=1).updates or []
                if updates:
                    u = updates[0]
                    item.update(
                        ultimo_update_id=u.update_id,
                        ultimo_update_estado=u.state.value if u.state else None,
                        ultimo_update_criado_em_ms=u.creation_time,
                    )
                else:
                    item["ultimo_update_estado"] = "SEM_EXECUCOES"
            except Exception as e:  # noqa: BLE001
                item["erro_updates"] = str(e)
            resultado.append(item)
        return {"encontrado": True, "pipelines": resultado}

    return await asyncio.to_thread(_status)


# --------------------------------------------------------------------------------------
# Ferramentas de LEITURA (espelham os MCP gerenciados)
# Motivo: (1) no Free Edition os MCP gerenciados dependem de um Public Preview que pode
# estar desligado no workspace; (2) o MCP gerenciado de Vector Search faz busca ANN pura,
# que ranqueia mal em português — aqui usamos query_type="HYBRID" (vetor + palavra-chave).
# Continua tudo governado pelo Unity Catalog: o SP do app só tem EXECUTE/SELECT no schema.
# --------------------------------------------------------------------------------------
INDICE_POLITICAS = os.getenv("INDICE_POLITICAS", f"{CATALOGO}.{SCHEMA}.politicas_index")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")


@mcp.tool()
async def clientes_alto_risco(p_uf: str, p_limite: int = 5) -> list[dict]:
    """Lista os clientes ATIVOS de maior score de risco de churn numa UF (função UC workspace.telecom_ai.clientes_alto_risco).

    Args:
        p_uf: Sigla da UF com 2 letras maiúsculas (ex.: 'SP').
        p_limite: Quantidade de clientes (1 a 50, padrão 5).
    """
    uf = (p_uf or "").strip().upper()[:2]
    limite = max(1, min(int(p_limite), 50))
    return await asyncio.to_thread(
        executar_sql, f"SELECT * FROM {CATALOGO}.{SCHEMA}.clientes_alto_risco(:uf, :limite)", {"uf": uf, "limite": limite}
    )


@mcp.tool()
async def get_cliente_360(p_cliente_id: str) -> list[dict]:
    """Retorna o perfil 360 de um cliente (plano, UF, valor mensal, tenure, faturas atrasadas, tickets 90d,
    texto do último ticket, score_risco) via função UC workspace.telecom_ai.get_cliente_360.

    Args:
        p_cliente_id: ID do cliente (ex.: 'C00605').
    """
    cid = (p_cliente_id or "").strip()[:64]
    return await asyncio.to_thread(
        executar_sql, f"SELECT * FROM {CATALOGO}.{SCHEMA}.get_cliente_360(:cid)", {"cid": cid}
    )


@mcp.tool()
async def buscar_politicas(consulta: str, k: int = 3) -> list[dict]:
    """Busca as políticas de retenção mais relevantes para um perfil/motivo de risco (Vector Search HYBRID
    no índice workspace.telecom_ai.politicas_index).

    Args:
        consulta: Descrição do problema/perfil do cliente em português (ex.: 'reclama de sinal fraco, quer cancelar').
        k: Número de políticas a retornar (1 a 10, padrão 3).
    """
    k = max(1, min(int(k), 10))

    def _buscar() -> list[dict]:
        r = ws().vector_search_indexes.query_index(
            index_name=INDICE_POLITICAS,
            columns=["politica_id", "titulo", "texto"],
            query_text=consulta,
            query_type="HYBRID",
            num_results=k,
        )
        cols = [c.name for c in r.manifest.columns]
        return [dict(zip(cols, linha)) for linha in (r.result.data_array or [])]

    return await asyncio.to_thread(_buscar)


@mcp.tool()
async def perguntar_genie(pergunta: str) -> dict:
    """Faz uma pergunta em linguagem natural ao Genie space 'Retenção Telecom' (dados gold de churn/KPIs)
    e devolve a resposta, o SQL gerado e até 20 linhas do resultado.

    Args:
        pergunta: Pergunta em português (ex.: 'qual a taxa de churn por UF?').
    """
    if not GENIE_SPACE_ID:
        return {"erro": "GENIE_SPACE_ID não configurado"}

    def _genie() -> dict:
        w = ws()
        msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, pergunta)
        saida: dict[str, Any] = {"status": str(msg.status.value if msg.status else None)}
        for a in msg.attachments or []:
            if a.text:
                saida["resposta"] = a.text.content
            if a.query:
                saida["sql"] = a.query.query
                saida["descricao"] = a.query.description
                res = w.genie.get_message_attachment_query_result(
                    GENIE_SPACE_ID, msg.conversation_id, msg.message_id, a.attachment_id
                )
                sr = res.statement_response
                if sr and sr.manifest and sr.result:
                    cols = [c.name for c in sr.manifest.schema.columns]
                    saida["linhas"] = [dict(zip(cols, l)) for l in (sr.result.data_array or [])[:20]]
        return saida

    return await asyncio.to_thread(_genie)


@mcp.resource(
    "telecom://schema/casos_retencao",
    name="schema_casos_retencao",
    description="Schema (DDL) da tabela workspace.telecom_ai.casos_retencao gravada por este servidor.",
    mime_type="text/plain",
)
def schema_casos_retencao() -> str:
    return DDL_CASOS.strip()


@mcp.prompt(name="plano_retencao", description="Roteiro para montar um plano de retenção para um cliente.")
def plano_retencao(cliente_id: str) -> str:
    return (
        f"Monte um plano de retenção para o cliente {cliente_id}:\n"
        "1. Consulte o perfil 360 e o score de risco (função UC get_cliente_360).\n"
        "2. Explique os principais fatores de risco (faturas atrasadas, tickets recentes, tenure, valor).\n"
        "3. Busque nas políticas de retenção (vector search) a política aplicável e cite o politica_id.\n"
        "4. Verifique com listar_casos_abertos se já existe caso aberto para este cliente.\n"
        "5. Se não existir, chame abrir_caso_retencao com motivo, oferta_sugerida e prioridade "
        "('critica' se score_risco >= 0.8, 'alta' se >= 0.6, senão 'media').\n"
        "Responda em português, de forma objetiva."
    )


if __name__ == "__main__":
    log.info("Iniciando MCP server em 0.0.0.0:%s/mcp (warehouse=%s, tabela=%s)", PORTA, WAREHOUSE_ID, TABELA_CASOS)
    mcp.run(transport="streamable-http")
