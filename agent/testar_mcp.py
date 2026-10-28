"""
Smoke test dos servidores MCP (gerenciados + customizado) com `databricks_mcp.DatabricksMCPClient`.

Para cada URL: lista as ferramentas e, quando possível, faz uma chamada de exemplo.
Uso:  .venv\\Scripts\\python.exe agent\\testar_mcp.py [--abrir-caso]
Perfil: DATABRICKS_CONFIG_PROFILE (padrão DEFAULT).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

from retencao_multiagente import PERFIL, _erro_raiz, urls_mcp


def _texto(res) -> str:
    return " | ".join(getattr(c, "text", str(c)) for c in (res.content or []))[:400]


def main(abrir_caso: bool) -> None:
    w = WorkspaceClient(profile=PERFIL)
    urls = urls_mcp(w)
    host = w.config.host.rstrip("/")
    urls["ai_search (alternativa)"] = f"{host}/api/2.0/mcp/ai-search/workspace/telecom_ai"
    exemplos = {
        "uc_functions": ("workspace__telecom_ai__clientes_alto_risco", {"p_uf": "SP", "p_limite": 3}),
        "app_custom": ("buscar_politicas", {"consulta": "problema de sinal", "k": 1}),
    }
    for nome, url in urls.items():
        print(f"\n### {nome}: {url}")
        if not url:
            print("   (não encontrado)")
            continue
        try:
            cli = DatabricksMCPClient(server_url=url, workspace_client=w)
            tools = cli.list_tools()
            print(f"   OK list_tools -> {[t.name for t in tools]}")
            if nome in exemplos and any(t.name == exemplos[nome][0] for t in tools):
                print(f"   call {exemplos[nome][0]} -> {_texto(cli.call_tool(*exemplos[nome]))}")
            if nome == "app_custom" and abrir_caso:
                res = cli.call_tool("abrir_caso_retencao", {"cliente_id": "TESTE_MCP", "motivo": "smoke test",
                                                            "oferta_sugerida": "n/a", "prioridade": "baixa"})
                print(f"   call abrir_caso_retencao -> {_texto(res)}")
        except Exception as e:  # noqa: BLE001
            print(f"   ERRO: {_erro_raiz(e)}")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    ap = argparse.ArgumentParser()
    ap.add_argument("--abrir-caso", action="store_true", help="Também grava um caso de teste via app")
    main(ap.parse_args().abrir_caso)
