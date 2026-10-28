"""
Sistema multiagente do Copiloto de Retenção Telecom (LangGraph + MCP no Databricks).

Arquitetura (padrão *supervisor*):

    usuário ──► supervisor ──► analista_dados          (MCP gerenciado: UC Functions + Genie)
                    ▲  │──────► especialista_politicas  (MCP gerenciado: Vector Search)
                    │  └──────► executor_acoes          (MCP customizado: Databricks App)
                    └──────────── cada especialista devolve o resultado ao supervisor

- O supervisor NÃO tem ferramentas de dados: ele só decide quem trabalha a seguir
  (tool-call `encaminhar`) ou encerra (`finalizar`). Isso isola permissões por papel:
  somente o executor consegue escrever (abrir caso).
- Cada especialista é um agente ReAct (`langchain.agents.create_agent`) com o seu
  próprio conjunto de ferramentas MCP.
- LLM: Foundation Model APIs via `ChatDatabricks`.
- Observabilidade: MLflow Tracing (autolog do LangChain) num experimento do workspace.
- Memória de conversa: checkpointer `InMemorySaver` (thread_id), `recursion_limit` no config.

Uso:
    .venv\\Scripts\\python.exe agent\\retencao_multiagente.py "sua pergunta"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks, DatabricksMCPServer, DatabricksMultiServerMCPClient
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.types import Command
from typing_extensions import TypedDict

log = logging.getLogger("retencao_multiagente")

# --------------------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------------------
PERFIL = os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
CATALOGO, SCHEMA = "workspace", "telecom_ai"
NOME_APP_MCP = "mcp-telecom-retencao"
TITULO_GENIE = "Retenção Telecom"
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
# Experimento MLflow: /Users/<usuario-atual>/retencao_ai_mcp/agente_retencao (ou MLFLOW_EXPERIMENTO)
EXPERIMENTO = os.getenv("MLFLOW_EXPERIMENTO")
RECURSION_LIMIT = 40
MAX_PASSOS_SUPERVISOR = 8

ESPECIALISTAS = ("analista_dados", "especialista_politicas", "executor_acoes")
PASSOS_LOG: list[str] = []  # trilha legível (Markdown) de cada especialista, usada na transcrição


# --------------------------------------------------------------------------------------
# Descoberta das URLs MCP
# --------------------------------------------------------------------------------------
def descobrir_genie_space_id(w: WorkspaceClient) -> str | None:
    """Procura o Genie space pelo título (sem criar nada)."""
    try:
        token = None
        while True:
            resp = w.genie.list_spaces(page_token=token)
            for s in resp.spaces or []:
                if (s.title or "").strip().lower() == TITULO_GENIE.lower():
                    return s.space_id
            token = resp.next_page_token
            if not token:
                return None
    except Exception as e:  # noqa: BLE001
        log.warning("Não foi possível listar Genie spaces: %s", e)
        return None


def descobrir_url_app(w: WorkspaceClient) -> str | None:
    try:
        app = w.apps.get(NOME_APP_MCP)
        return f"{app.url.rstrip('/')}/mcp" if app.url else None
    except Exception as e:  # noqa: BLE001
        log.warning("App %s indisponível: %s", NOME_APP_MCP, e)
        return None


def urls_mcp(w: WorkspaceClient) -> dict[str, str | None]:
    host = w.config.host.rstrip("/")
    genie_id = descobrir_genie_space_id(w)
    return {
        "uc_functions": f"{host}/api/2.0/mcp/functions/{CATALOGO}/{SCHEMA}",
        "vector_search": f"{host}/api/2.0/mcp/vector-search/{CATALOGO}/{SCHEMA}",
        "genie": f"{host}/api/2.0/mcp/genie/{genie_id}" if genie_id else None,
        "app_custom": descobrir_url_app(w),
    }


def _erro_raiz(e: BaseException) -> str:
    """Desembrulha ExceptionGroup (anyio/TaskGroup) para mostrar a causa real."""
    while getattr(e, "exceptions", None):
        e = e.exceptions[0]
    return f"{type(e).__name__}: {str(e)[:200]}"


async def carregar_ferramentas(w: WorkspaceClient) -> dict[str, list]:
    """Carrega as ferramentas de cada servidor MCP separadamente (tolerante a falhas)."""
    urls = urls_mcp(w)
    ferramentas: dict[str, list] = {}
    for nome, url in urls.items():
        if not url:
            ferramentas[nome] = []
            print(f"[mcp] {nome}: indisponível (não encontrado)")
            continue
        cliente = DatabricksMultiServerMCPClient(
            [DatabricksMCPServer(name=nome, url=url, workspace_client=w, timeout=120.0, handle_tool_error=True)]
        )
        try:
            ferramentas[nome] = await cliente.get_tools()
            print(f"[mcp] {nome}: {[t.name for t in ferramentas[nome]]}  <- {url}")
        except Exception as e:  # noqa: BLE001
            ferramentas[nome] = []
            print(f"[mcp] {nome}: ERRO ao listar ferramentas ({_erro_raiz(e)})")
    return ferramentas


# --------------------------------------------------------------------------------------
# Prompts (pt-BR)
# --------------------------------------------------------------------------------------
PROMPT_SUPERVISOR = """Você é o SUPERVISOR de uma equipe de retenção de clientes de uma operadora de telecom brasileira.
Sua equipe:
- analista_dados: consulta dados (perfil 360 do cliente, score de risco, clientes de alto risco por UF, perguntas em linguagem natural via Genie).
- especialista_politicas: busca nas políticas de retenção qual oferta/política se aplica a um perfil de cliente.
- executor_acoes: abre casos de retenção (escrita) e consulta casos abertos / status do pipeline.

Regras:
1. Divida a tarefa em passos e use a ferramenta `encaminhar` para delegar UM passo por vez ao especialista certo,
   com uma instrução clara e autocontida (inclua cliente_id, UF, score_risco, valores e fatores de risco já descobertos).
2. Ordem típica: analista_dados -> especialista_politicas -> executor_acoes.
3. Nunca invente dados: use apenas o que os especialistas retornarem.
4. Só peça ao executor_acoes para abrir caso depois de saber o cliente, o motivo do risco e a política aplicável.
5. Quando tudo estiver feito, chame `finalizar` com a resposta final em português para o usuário
   (cliente, por que está em risco, política aplicada e o caso_id aberto)."""

PROMPT_ANALISTA = """Você é o ANALISTA DE DADOS de retenção. Use as ferramentas disponíveis (funções do Unity Catalog
e/ou Genie) para buscar dados reais. Para "cliente de maior risco em uma UF" use a função de clientes de alto risco
(parâmetros: UF com 2 letras maiúsculas, limite) e depois o perfil 360 do cliente escolhido.
Explique os fatores de risco com números (score_risco, faturas atrasadas, tickets recentes, tenure, valor mensal,
texto do último ticket). Responda em português, de forma objetiva, sem inventar dados."""

PROMPT_POLITICAS = """Você é o ESPECIALISTA EM POLÍTICAS de retenção. Use a busca vetorial nas políticas de retenção
para encontrar a(s) política(s) que se aplicam ao perfil descrito (motivo do risco, plano, valor, tempo de casa).
Faça consultas CURTAS (3 a 8 palavras) focadas na CAUSA RAIZ do risco (ex.: "problema de sinal tickets de qualidade",
"faturas atrasadas inadimplência", "portabilidade concorrente oferta"); se útil, faça uma 2a consulta pelo perfil
(tempo de casa, valor). Escolha a política que resolve a causa raiz e, opcionalmente, uma complementar.
Retorne o politica_id, o título e a oferta concreta a ser feita, citando o trecho da política. Responda em português."""

PROMPT_EXECUTOR = """Você é o EXECUTOR DE AÇÕES de retenção. Você pode abrir casos de retenção e listar casos abertos.
Antes de abrir um caso, verifique com listar_casos_abertos se já existe caso aberto para o mesmo cliente_id.
Ao abrir: prioridade 'critica' se score_risco >= 0.8, 'alta' se >= 0.6, senão 'media'.
O motivo deve resumir os fatores de risco; a oferta_sugerida deve citar o politica_id.
Retorne o caso_id criado. Responda em português."""
# (o executor recebe o score_risco na instrução do supervisor para calcular a prioridade)


# --------------------------------------------------------------------------------------
# Estado e grafo
# --------------------------------------------------------------------------------------
class EstadoEquipe(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    passos: int
    resposta_final: str


@tool
def encaminhar(especialista: Literal["analista_dados", "especialista_politicas", "executor_acoes"], instrucao: str) -> str:
    """Delega um passo da tarefa a um especialista da equipe.

    Args:
        especialista: 'analista_dados', 'especialista_politicas' ou 'executor_acoes'.
        instrucao: Instrução clara e autocontida para o especialista (inclua IDs e fatos já conhecidos).
    """
    return "encaminhado"


@tool
def finalizar(resposta: str) -> str:
    """Encerra o trabalho e entrega a resposta final ao usuário (em português)."""
    return "finalizado"


def _texto(msg: BaseMessage) -> str:
    c = msg.content
    if isinstance(c, str):
        return c
    # conteúdo em blocos (ex.: modelos de raciocínio): pega só os blocos de texto
    return "\n".join(b.get("text", "") for b in c if isinstance(b, dict) and b.get("type") == "text")


# Ferramentas do MCP customizado por papel (o app também expõe leitura: espelho dos MCP gerenciados)
FERRAMENTAS_LEITURA_APP = {"clientes_alto_risco", "get_cliente_360", "perguntar_genie"}
FERRAMENTAS_POLITICAS_APP = {"buscar_politicas"}


def selecionar_ferramentas(ferramentas: dict[str, list]) -> tuple[list, list, list]:
    """Distribui as ferramentas MCP por papel (menor privilégio por especialista).

    Preferência: MCP gerenciado (UC Functions / Genie) para o analista. Para políticas usamos a
    `buscar_politicas` do app (Vector Search HYBRID), que ranqueia melhor em português que a ANN
    do MCP gerenciado; se o app não estiver disponível, cai para o MCP gerenciado de Vector Search.
    Se o Public Preview de MCP gerenciado estiver desligado no workspace, o analista usa as
    ferramentas de leitura equivalentes do app. O executor só recebe as ferramentas de ação.
    """
    app = ferramentas.get("app_custom", [])
    gerenciado_dados = ferramentas.get("uc_functions", []) + ferramentas.get("genie", [])
    analista = gerenciado_dados or [t for t in app if t.name in FERRAMENTAS_LEITURA_APP]
    politicas = [t for t in app if t.name in FERRAMENTAS_POLITICAS_APP] or ferramentas.get("vector_search", [])
    executor = [t for t in app if t.name not in FERRAMENTAS_LEITURA_APP | FERRAMENTAS_POLITICAS_APP]
    for papel, ts in (("analista_dados", analista), ("especialista_politicas", politicas), ("executor_acoes", executor)):
        print(f"[papel] {papel}: {[t.name for t in ts]}")
    return analista, politicas, executor


def construir_grafo(llm: ChatDatabricks, ferramentas: dict[str, list]):
    tools_analista, tools_politicas, tools_executor = selecionar_ferramentas(ferramentas)

    agentes = {
        "analista_dados": create_agent(llm, tools_analista, system_prompt=PROMPT_ANALISTA, name="analista_dados"),
        "especialista_politicas": create_agent(llm, tools_politicas, system_prompt=PROMPT_POLITICAS, name="especialista_politicas"),
        "executor_acoes": create_agent(llm, tools_executor, system_prompt=PROMPT_EXECUTOR, name="executor_acoes"),
    }
    llm_supervisor = llm.bind_tools([encaminhar, finalizar], tool_choice="required")

    async def supervisor(estado: EstadoEquipe) -> Command:
        passos = estado.get("passos", 0)
        if passos >= MAX_PASSOS_SUPERVISOR:
            return Command(goto=END, update={"resposta_final": "Limite de passos do supervisor atingido."})
        resp: AIMessage = await llm_supervisor.ainvoke([SystemMessage(PROMPT_SUPERVISOR), *estado["messages"]])
        if not resp.tool_calls:  # fallback: modelo respondeu em texto
            return Command(goto=END, update={"messages": [resp], "resposta_final": _texto(resp)})
        chamada = resp.tool_calls[0]
        resp = AIMessage(content=resp.content, tool_calls=[chamada], id=resp.id)  # 1 delegação por vez
        if chamada["name"] == "finalizar":
            final = chamada["args"].get("resposta", "")
            return Command(
                goto=END,
                update={"messages": [resp, ToolMessage(final, tool_call_id=chamada["id"])], "resposta_final": final},
            )
        destino = chamada["args"].get("especialista")
        if destino not in ESPECIALISTAS:
            erro = ToolMessage(f"Especialista inválido: {destino}. Use um de {ESPECIALISTAS}.", tool_call_id=chamada["id"])
            return Command(goto="supervisor", update={"messages": [resp, erro], "passos": passos + 1})
        return Command(goto=destino, update={"messages": [resp], "passos": passos + 1})

    def no_especialista(nome: str):
        agente = agentes[nome]

        async def executar(estado: EstadoEquipe) -> Command:
            chamada = estado["messages"][-1].tool_calls[0]
            instrucao = chamada["args"].get("instrucao", "")
            print(f"\n>>> supervisor -> {nome}: {instrucao}")
            PASSOS_LOG.append(f"### supervisor → `{nome}`\n\n> {instrucao}\n")
            resultado = await agente.ainvoke({"messages": [HumanMessage(instrucao)]}, {"recursion_limit": 20})
            for m in resultado["messages"]:
                if isinstance(m, AIMessage) and m.tool_calls:
                    for tc in m.tool_calls:
                        print(f"    [{nome}] tool_call {tc['name']}({json.dumps(tc['args'], ensure_ascii=False)})")
                        PASSOS_LOG.append(f"- tool call MCP `{tc['name']}` `{json.dumps(tc['args'], ensure_ascii=False)}`")
                elif isinstance(m, ToolMessage):
                    print(f"    [{nome}] tool_result: {_texto(m)[:600]}")
                    PASSOS_LOG.append(f"  - resultado: `{' '.join(_texto(m).split())[:500]}`")
            resposta = _texto(resultado["messages"][-1])
            print(f"<<< {nome}: {resposta}")
            PASSOS_LOG.append(f"\n**Resposta de `{nome}`:**\n\n{resposta}\n")
            return Command(
                goto="supervisor",
                update={"messages": [ToolMessage(f"[{nome}] {resposta}", tool_call_id=chamada["id"], name=nome)]},
            )

        return executar

    g = StateGraph(EstadoEquipe)
    g.add_node("supervisor", supervisor, destinations=(*ESPECIALISTAS, END))
    for nome in ESPECIALISTAS:
        g.add_node(nome, no_especialista(nome), destinations=("supervisor",))
    g.add_edge(START, "supervisor")
    return g.compile(checkpointer=InMemorySaver(), name="supervisor_retencao")


# --------------------------------------------------------------------------------------
# Execução
# --------------------------------------------------------------------------------------
PERGUNTA_DEMO = (
    "Pegue o cliente de maior risco em SP, explique por que ele está em risco, "
    "qual política de retenção se aplica e abra um caso de retenção."
)


def configurar_mlflow(w: WorkspaceClient) -> str | None:
    try:
        mlflow.set_tracking_uri(f"databricks://{PERFIL}" if PERFIL != "DEFAULT" else "databricks")
        caminho = EXPERIMENTO or f"/Users/{w.current_user.me().user_name}/retencao_ai_mcp/agente_retencao"
        w.workspace.mkdirs(caminho.rsplit("/", 1)[0])
        exp = mlflow.set_experiment(caminho)
        mlflow.langchain.autolog()
        return exp.experiment_id
    except Exception as e:  # noqa: BLE001
        print(f"[mlflow] tracing desabilitado: {e}")
        return None


async def main(pergunta: str, transcript: Path | None) -> str:
    w = WorkspaceClient(profile=PERFIL)
    exp_id = configurar_mlflow(w)
    ferramentas = await carregar_ferramentas(w)
    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.0, max_tokens=1500, workspace_client=w)
    grafo = construir_grafo(llm, ferramentas)

    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": RECURSION_LIMIT}
    print(f"\n=== Pergunta: {pergunta}\n")
    estado = await grafo.ainvoke({"messages": [HumanMessage(pergunta)], "passos": 0, "resposta_final": ""}, config)
    final = estado.get("resposta_final") or _texto(estado["messages"][-1])
    print(f"\n=== Resposta final ===\n{final}")
    if exp_id:
        print(f"\n[mlflow] traces: {w.config.host.rstrip('/')}/ml/experiments/{exp_id}/traces")

    if transcript:
        linhas = [
            "# Transcrição da demo — Copiloto de Retenção Telecom (multiagente + MCP)",
            "",
            f"- Data: {datetime.now():%Y-%m-%d %H:%M}",
            f"- LLM: `{LLM_ENDPOINT}`",
            f"- thread_id: `{thread_id}`",
            f"- Ferramentas MCP: " + "; ".join(f"{k}={[t.name for t in v]}" for k, v in ferramentas.items()),
            "",
            "## Mensagens do supervisor",
            "",
        ]
        for m in estado["messages"]:
            papel = type(m).__name__.replace("Message", "")
            if isinstance(m, AIMessage) and m.tool_calls:
                tc = m.tool_calls[0]
                linhas.append(f"**Supervisor → `{tc['name']}`**: `{json.dumps(tc['args'], ensure_ascii=False)}`\n")
            else:
                linhas.append(f"**{getattr(m, 'name', None) or papel}**:\n\n{_texto(m)}\n")
        linhas += ["## Passo a passo dos especialistas (tool calls MCP)", "", *PASSOS_LOG, ""]
        if exp_id:
            linhas += [f"- MLflow traces: {w.config.host.rstrip('/')}/ml/experiments/{exp_id}/traces", ""]
        linhas += ["## Resposta final", "", final, ""]
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text("\n".join(linhas), encoding="utf-8")
    return final


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Supervisor multiagente de retenção (LangGraph + MCP Databricks)")
    ap.add_argument("pergunta", nargs="?", default=PERGUNTA_DEMO)
    ap.add_argument("--transcript", type=Path, default=None, help="Salva a transcrição em Markdown")
    args = ap.parse_args()
    asyncio.run(main(args.pergunta, args.transcript))
