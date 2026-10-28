"""
Deploy do servidor MCP customizado como Databricks App (idempotente e parametrizado).

Passos:
 1. Cria o app `mcp-telecom-retencao` (se não existir) com o recurso `sql-warehouse` (CAN_USE).
 2. Concede ao service principal (SP) do app os privilégios mínimos no Unity Catalog:
      USE CATALOG  <catalogo>
      USE SCHEMA, SELECT, MODIFY, CREATE TABLE, EXECUTE  em <catalogo>.<schema>
    + recurso genie-space (CAN_RUN) para a ferramenta perguntar_genie
    e CAN_VIEW no(s) pipeline(s) Lakeflow cujo nome contém o filtro (para `status_pipeline_retencao`).
 3. (Opcional) Cria a tabela casos_retencao como o usuário que faz o deploy.
 4. Envia o código (`server.py`, `app.yaml`, `requirements.txt`) para o workspace e faz `apps deploy`.
 5. Imprime a URL do app e do endpoint MCP (`<url>/mcp`).

Uso (na raiz do projeto, com o venv):
    .venv\\Scripts\\python.exe mcp_server\\deploy_app.py --warehouse-id <id>
Variáveis/flags: DATABRICKS_CONFIG_PROFILE (padrão DEFAULT), DATABRICKS_WAREHOUSE_ID,
--app-name, --catalogo, --schema, --pasta-workspace, --filtro-pipeline.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppDeploymentMode,
    AppResource,
    AppResourceGenieSpace,
    AppResourceGenieSpaceGenieSpacePermission,
    AppResourceSqlWarehouse,
    AppResourceSqlWarehouseSqlWarehousePermission,
)
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.pipelines import PipelineAccessControlRequest, PipelinePermissionLevel
from databricks.sdk.service.workspace import ImportFormat

AQUI = Path(__file__).parent
ARQUIVOS_APP = ["server.py", "app.yaml", "requirements.txt"]


def passo(msg: str) -> None:
    print(f"\n==> {msg}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
    ap.add_argument("--warehouse-id", default=os.getenv("DATABRICKS_WAREHOUSE_ID"))
    ap.add_argument("--app-name", default="mcp-telecom-retencao")
    ap.add_argument("--catalogo", default="workspace")
    ap.add_argument("--schema", default="telecom_ai")
    ap.add_argument("--pasta-workspace", default=None,
                    help="Padrão: /Workspace/Users/<usuario>/retencao_ai_mcp/mcp_server")
    ap.add_argument("--filtro-pipeline", default="retencao")
    ap.add_argument("--genie-space-id", default=os.getenv("GENIE_SPACE_ID", "01f1d2578a841c018d73b75bc354b90e"),
                    help="Genie space exposto pela ferramenta perguntar_genie (recurso genie-space, CAN_RUN)")
    ap.add_argument("--sem-tabela", action="store_true", help="Não cria a tabela casos_retencao no deploy")
    args = ap.parse_args()

    w = WorkspaceClient(profile=args.profile)
    eu = w.current_user.me().user_name
    if not args.warehouse_id:
        whs = list(w.warehouses.list())
        if not whs:
            sys.exit("Nenhum SQL warehouse encontrado; informe --warehouse-id.")
        args.warehouse_id = whs[0].id
        print(f"(usando o primeiro warehouse disponível: {whs[0].name} / {whs[0].id})")
    pasta = args.pasta_workspace or f"/Workspace/Users/{eu}/retencao_ai_mcp/mcp_server"
    schema_full = f"{args.catalogo}.{args.schema}"
    print(f"Workspace: {w.config.host} | usuário: {eu} | warehouse: {args.warehouse_id} | schema: {schema_full}")

    # 1. App ------------------------------------------------------------------------
    passo(f"Criando/obtendo o app {args.app_name}")
    recurso_wh = AppResource(
        name="sql-warehouse",  # referenciado em app.yaml (valueFrom: sql-warehouse)
        description="SQL warehouse para gravar/ler casos_retencao",
        sql_warehouse=AppResourceSqlWarehouse(
            id=args.warehouse_id, permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE
        ),
    )
    recursos = [recurso_wh]
    if args.genie_space_id:
        recursos.append(AppResource(
            name="genie-space",  # referenciado em app.yaml (valueFrom: genie-space)
            description="Genie space Retenção Telecom (ferramenta perguntar_genie)",
            genie_space=AppResourceGenieSpace(name="genie-space", space_id=args.genie_space_id,
                                              permission=AppResourceGenieSpaceGenieSpacePermission.CAN_RUN),
        ))
    try:
        app = w.apps.get(args.app_name)
        print("app já existe; atualizando recursos")
        app = w.apps.update(
            name=args.app_name,
            app=App(name=args.app_name, description=app.description, resources=recursos),
        )
    except Exception as e:  # noqa: BLE001
        if "does not exist" not in str(e).lower() and "not found" not in str(e).lower():
            raise
        app = w.apps.create_and_wait(
            App(
                name=args.app_name,
                description="Servidor MCP customizado do Copiloto de Retenção Telecom (Streamable HTTP em /mcp)",
                resources=recursos,
            )
        )
    sp = app.service_principal_client_id
    print(f"service principal do app: {app.service_principal_name} ({sp})")

    # 2. Grants UC (menor privilégio) -----------------------------------------------------
    passo("Concedendo privilégios no Unity Catalog ao SP do app")
    w.grants.update("catalog", args.catalogo, changes=[PermissionsChange(principal=sp, add=[Privilege.USE_CATALOG])])
    w.grants.update(
        "schema",
        schema_full,
        changes=[PermissionsChange(principal=sp, add=[Privilege.USE_SCHEMA, Privilege.SELECT,
                                                      Privilege.MODIFY, Privilege.CREATE_TABLE,
                                                      Privilege.EXECUTE])],
    )
    print(f"USE CATALOG {args.catalogo}; USE SCHEMA/SELECT/MODIFY/CREATE TABLE/EXECUTE em {schema_full}")

    for p in w.pipelines.list_pipelines():
        if args.filtro_pipeline in (p.name or "").lower():
            w.pipelines.update_permissions(
                p.pipeline_id,
                access_control_list=[PipelineAccessControlRequest(
                    service_principal_name=sp, permission_level=PipelinePermissionLevel.CAN_VIEW)],
            )
            print(f"CAN_VIEW no pipeline {p.name}")

    # 3. Tabela de casos (como usuário; o app também cria IF NOT EXISTS) -----------------
    if not args.sem_tabela:
        passo("Garantindo a tabela casos_retencao")
        sys.path.insert(0, str(AQUI))
        os.environ["DATABRICKS_WAREHOUSE_ID"] = args.warehouse_id
        os.environ["UC_CATALOGO"], os.environ["UC_SCHEMA"] = args.catalogo, args.schema
        import server  # noqa: PLC0415  (reaproveita DDL + executor SQL parametrizado)

        server._ws = w
        try:
            server.garantir_tabela()
            print(f"ok: {server.TABELA_CASOS}")
        except Exception as e:  # noqa: BLE001
            print(f"AVISO: não foi possível criar a tabela agora ({e}); o app tentará no 1º uso.")

    # 4. Upload do código + deploy ---------------------------------------------------
    passo(f"Enviando código para {pasta}")
    w.workspace.mkdirs(pasta)
    for nome in ARQUIVOS_APP:
        w.workspace.upload(f"{pasta}/{nome}", io.BytesIO((AQUI / nome).read_bytes()),
                           format=ImportFormat.RAW, overwrite=True)
        print(f"  {nome}")

    passo("Garantindo que o compute do app está ativo e fazendo deploy")
    estado = app.compute_status.state.value if app.compute_status and app.compute_status.state else ""
    if estado not in ("ACTIVE", "STARTING"):
        w.apps.start_and_wait(args.app_name)
    dep = w.apps.deploy_and_wait(
        args.app_name, AppDeployment(source_code_path=pasta, mode=AppDeploymentMode.SNAPSHOT)
    )
    print(f"deploy: {dep.status.state.value if dep.status else '?'} — {dep.status.message if dep.status else ''}")

    app = w.apps.get(args.app_name)
    print(f"\nURL do app : {app.url}\nEndpoint MCP: {app.url.rstrip('/')}/mcp")
    print("Lembrete: quem consome o MCP precisa de CAN_USE no app (o dono já tem).")
    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
