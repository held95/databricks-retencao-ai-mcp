"""
Camada SILVER — limpeza, tipagem, deduplicação e qualidade de dados (expectations).

Regras de qualidade (registradas no event log do pipeline):
  - @dp.expect_or_drop  -> linha inválida é descartada e contabilizada
  - @dp.expect          -> linha é mantida, mas a violação é contabilizada (monitoramento)
Não usamos expect_or_fail para não interromper o pipeline em dados sintéticos sujos.
Dedup: por chave de negócio, mantendo o registro mais recente ingerido.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

UFS_VALIDAS = "('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')"


def _dedup(df, chave: str):
    """Mantém 1 linha por chave (a última ingerida). Linhas com chave nula passam para a expectation descartar."""
    w = Window.partitionBy(chave).orderBy(F.col("_ingerido_em").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter((F.col("_rn") == 1) | F.col(chave).isNull())
              .drop("_rn"))


@dp.materialized_view(
    name="silver_clientes",
    comment="Silver: clientes limpos, tipados e deduplicados (1 linha por cliente_id). Contém PII (cpf).",
    table_properties={"camada": "silver", "contem_pii": "true"},
    schema="""
        cliente_id STRING COMMENT 'Identificador único do cliente (C00000)',
        nome STRING COMMENT 'Nome completo (sintético)',
        cpf STRING MASK workspace.telecom_ai.mask_cpf COMMENT 'CPF fictício formatado — PII, column mask mask_cpf',
        uf STRING COMMENT 'Unidade federativa',
        cidade STRING COMMENT 'Cidade do cliente',
        plano STRING COMMENT 'Plano contratado',
        valor_mensal DECIMAL(10,2) COMMENT 'Mensalidade do plano em R$',
        data_adesao DATE COMMENT 'Data de adesão do cliente',
        canal_aquisicao STRING COMMENT 'Canal de aquisição (loja, app, site, televendas, parceiro)',
        status STRING COMMENT 'ativo | cancelado (cancelado = churn em out/2026)',
        data_cancelamento DATE COMMENT 'Data do cancelamento, se houver'
    """,
)
@dp.expect_or_drop("cliente_id_nao_nulo", "cliente_id IS NOT NULL")
@dp.expect_or_drop("uf_valida", f"uf IN {UFS_VALIDAS}")
@dp.expect("cpf_formato_valido", "cpf RLIKE '^[0-9]{3}[.][0-9]{3}[.][0-9]{3}-[0-9]{2}$'")
@dp.expect("valor_mensal_positivo", "valor_mensal > 0")
def silver_clientes():
    df = _dedup(spark.read.table("bronze_clientes"), "cliente_id")
    return df.select(
        F.trim("cliente_id").alias("cliente_id"),
        F.initcap(F.trim("nome")).alias("nome"),
        F.trim("cpf").alias("cpf"),
        F.upper(F.trim("uf")).alias("uf"),
        F.trim("cidade").alias("cidade"),
        F.trim("plano").alias("plano"),
        F.col("valor_mensal").cast("decimal(10,2)").alias("valor_mensal"),
        F.to_date("data_adesao").alias("data_adesao"),
        F.lower("canal_aquisicao").alias("canal_aquisicao"),
        F.lower("status").alias("status"),
        F.to_date(F.nullif(F.col("data_cancelamento"), F.lit(""))).alias("data_cancelamento"),
    )


@dp.materialized_view(
    name="silver_faturas",
    comment="Silver: faturas tipadas e deduplicadas por fatura_id, com flag de atraso.",
    table_properties={"camada": "silver"},
    schema="""
        fatura_id STRING COMMENT 'Identificador da fatura',
        cliente_id STRING COMMENT 'Cliente dono da fatura',
        mes_referencia STRING COMMENT 'Competência (AAAA-MM)',
        data_vencimento DATE COMMENT 'Data de vencimento',
        data_pagamento DATE COMMENT 'Data de pagamento (nula = em aberto)',
        valor DECIMAL(10,2) COMMENT 'Valor da fatura em R$',
        status_pagamento STRING COMMENT 'pago | pago_com_atraso | em_aberto',
        dias_atraso INT COMMENT 'Dias entre vencimento e pagamento (ou 30/09/2026 se em aberto); 0 se em dia',
        atrasada BOOLEAN COMMENT 'TRUE se paga com atraso ou em aberto'
    """,
)
@dp.expect_or_drop("fatura_id_nao_nulo", "fatura_id IS NOT NULL")
@dp.expect_or_drop("cliente_id_nao_nulo", "cliente_id IS NOT NULL")
@dp.expect_or_drop("valor_positivo", "valor > 0")
@dp.expect("vencimento_preenchido", "data_vencimento IS NOT NULL")
def silver_faturas():
    df = _dedup(spark.read.table("bronze_faturas"), "fatura_id")
    venc = F.to_date("data_vencimento")
    pag = F.to_date(F.nullif(F.col("data_pagamento"), F.lit("")))
    atraso = F.greatest(F.lit(0), F.datediff(F.coalesce(pag, F.lit("2026-09-30").cast("date")), venc))
    return df.select(
        "fatura_id",
        F.nullif(F.col("cliente_id"), F.lit("")).alias("cliente_id"),
        "mes_referencia",
        venc.alias("data_vencimento"),
        pag.alias("data_pagamento"),
        F.col("valor").cast("decimal(10,2)").alias("valor"),
        F.col("status_pagamento"),
        atraso.cast("int").alias("dias_atraso"),
        (F.col("status_pagamento") != "pago").alias("atrasada"),
    )


@dp.materialized_view(
    name="silver_tickets",
    comment="Silver: tickets de atendimento com texto livre limpo, deduplicados por ticket_id.",
    table_properties={"camada": "silver"},
    schema="""
        ticket_id STRING COMMENT 'Identificador do ticket',
        cliente_id STRING COMMENT 'Cliente que abriu o ticket',
        data_abertura TIMESTAMP COMMENT 'Data/hora de abertura',
        canal STRING COMMENT 'Canal do atendimento (app, call_center, chat, loja, reclame_aqui, anatel)',
        categoria STRING COMMENT 'cobranca | sinal_qualidade | portabilidade | cancelamento | upgrade',
        texto STRING COMMENT 'Reclamação em texto livre (pt-BR)',
        resolvido BOOLEAN COMMENT 'TRUE se o ticket foi resolvido'
    """,
)
@dp.expect_or_drop("cliente_id_nao_nulo", "cliente_id IS NOT NULL")
@dp.expect_or_drop("texto_nao_vazio", "texto IS NOT NULL AND length(texto) > 0")
@dp.expect("categoria_conhecida",
           "categoria IN ('cobranca','sinal_qualidade','portabilidade','cancelamento','upgrade')")
def silver_tickets():
    df = _dedup(spark.read.table("bronze_tickets"), "ticket_id")
    return df.select(
        "ticket_id",
        "cliente_id",
        F.to_timestamp("data_abertura").alias("data_abertura"),
        F.lower("canal").alias("canal"),
        F.lower("categoria").alias("categoria"),
        F.trim("texto").alias("texto"),
        F.col("resolvido").cast("boolean").alias("resolvido"),
    )


@dp.table(  # streaming table (Delta + CDF) => fonte válida para Delta Sync do Vector Search
    name="silver_politicas",
    comment="Silver: políticas de retenção (fonte do índice vetorial para RAG). Streaming table com Change Data Feed.",
    table_properties={"camada": "silver", "delta.enableChangeDataFeed": "true"},
    schema="""
        politica_id STRING COMMENT 'Identificador da política (P000)',
        titulo STRING COMMENT 'Título da política',
        texto STRING COMMENT 'Texto integral com regras de elegibilidade',
        vigente_desde DATE COMMENT 'Início da vigência'
    """,
)
@dp.expect_or_drop("politica_id_nao_nulo", "politica_id IS NOT NULL")
@dp.expect("texto_minimo", "length(texto) >= 50")
def silver_politicas():
    df = spark.readStream.table("bronze_politicas")  # append-only, sem dedup necessário
    return df.select(
        "politica_id", F.trim("titulo").alias("titulo"), F.trim("texto").alias("texto"),
        F.to_date("vigente_desde").alias("vigente_desde"),
    )
