"""
Executa um arquivo .sql (instruções separadas por ';' no fim da linha) no SQL warehouse serverless
via Statement Execution API e imprime os resultados.

Uso:  py -3 sql/executar_sql.py sql/03_validacao.sql
Warehouse: env DATABRICKS_WAREHOUSE_ID (padrão 568abc53566f5fa8).
Requer o Databricks CLI no PATH (perfil DEFAULT, OAuth U2M).
"""
import os
import re
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "568abc53566f5fa8")


def instrucoes(texto: str):
    sem_comentarios = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*\n", sem_comentarios + "\n") if s.strip()]


def main(caminho: str) -> None:
    w = WorkspaceClient(profile="DEFAULT")
    for sql in instrucoes(open(caminho, encoding="utf-8").read()):
        print("\n>>>", sql.splitlines()[0][:110])
        r = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="50s")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            r = w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            print("   ERRO:", r.status.error.message if r.status.error else r.status.state)
            continue
        if r.manifest and r.manifest.schema and r.result and r.result.data_array:
            print("   " + " | ".join(c.name for c in r.manifest.schema.columns))
            for linha in r.result.data_array[:30]:
                print("   " + " | ".join("" if v is None else str(v) for v in linha))
        else:
            print("   OK")


if __name__ == "__main__":
    main(sys.argv[1])
