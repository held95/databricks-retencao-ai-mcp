"""
03 - Vector Search: índice Delta Sync sobre as políticas de retenção (base do RAG).

Endpoint: vs-telecom (já existente, ONLINE)
Índice:   workspace.telecom_ai.politicas_index
Fonte:    workspace.telecom_ai.silver_politicas (streaming table com Change Data Feed)
          -> se a tabela do pipeline não for aceita como fonte, cria a cópia politicas_kb (Delta + CDF).
Embeddings gerenciados: databricks-gte-large-en sobre a coluna `texto`; pipeline TRIGGERED.

Uso: py -3 ai/03_vector_search.py
"""
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType)

ENDPOINT = "vs-telecom"
INDICE = "workspace.telecom_ai.politicas_index"
FONTE = "workspace.telecom_ai.silver_politicas"
FONTE_COPIA = "workspace.telecom_ai.politicas_kb"
WAREHOUSE_ID = "568abc53566f5fa8"
PERGUNTA = "cliente insatisfeito com sinal querendo cancelar"

w = WorkspaceClient(profile="DEFAULT")


def sql(stmt: str):
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=stmt, wait_timeout="50s")
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(r.status.error.message if r.status.error else r.status.state)
    return r


def criar_indice(fonte: str):
    return w.vector_search_indexes.create_index(
        name=INDICE,
        endpoint_name=ENDPOINT,
        primary_key="politica_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=fonte,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="texto", embedding_model_endpoint_name="databricks-gte-large-en")],
            columns_to_sync=["politica_id", "titulo", "texto", "vigente_desde"],
        ),
    )


def main():
    try:
        idx = w.vector_search_indexes.get_index(INDICE)
        print("Índice já existe:", idx.name, "fonte:", idx.delta_sync_index_spec.source_table)
    except Exception:
        try:
            criar_indice(FONTE)
            print("Índice criado sobre", FONTE)
        except Exception as e:  # fallback: cópia Delta com CDF
            print("Falhou sobre a tabela do pipeline:", str(e)[:300])
            sql(f"CREATE OR REPLACE TABLE {FONTE_COPIA} "
                "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') "
                f"COMMENT 'Copia das politicas (fonte do indice vetorial)' AS SELECT * FROM {FONTE}")
            criar_indice(FONTE_COPIA)
            print("Índice criado sobre a cópia", FONTE_COPIA)

    t0 = time.time()
    while time.time() - t0 < 1500:
        idx = w.vector_search_indexes.get_index(INDICE)
        st = idx.status
        print(f"[{int(time.time()-t0)}s] ready={st.ready} linhas={st.indexed_row_count} msg={(st.message or '')[:120]}",
              flush=True)
        if st.ready:
            break
        time.sleep(30)
    else:
        sys.exit("Índice não ficou pronto a tempo")

    # ANN = só semântica (gte-large-en é um modelo em inglês); HYBRID = semântica + palavra-chave,
    # que recupera melhor termos de domínio em pt-BR como "sinal".
    for tipo in ("ANN", "HYBRID"):
        res = w.vector_search_indexes.query_index(
            index_name=INDICE, columns=["politica_id", "titulo", "texto"], query_text=PERGUNTA,
            num_results=3, query_type=tipo)
        print(f"\nBusca {tipo}: '{PERGUNTA}'")
        for linha in res.result.data_array:
            print(f"  {linha[0]} | score={float(linha[-1]):.4f} | {linha[1]}\n      {linha[2][:160]}")


if __name__ == "__main__":
    main()
