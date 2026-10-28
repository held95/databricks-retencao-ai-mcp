"""
Camada GOLD — visões de negócio (materialized views).

score_risco (0..1) — heurística logística TRANSPARENTE (sem modelo caixa-preta), calibrada à mão:
    z = -3.2
        + 0.45 * faturas_atrasadas_12m
        + 0.25 * n_tickets_90d
        + 0.60 * n_tickets_criticos_90d      (cancelamento/sinal nos últimos 90 dias)
        + 1.00 * [tenure_meses < 12]
        + 0.008 * (valor_mensal - 100)
    score_risco = 1 / (1 + exp(-z))
faixa_risco: baixo (< 0,3) | medio (0,3–0,6) | alto (>= 0,6)
Data de referência da foto: 2026-09-30 (fim da janela observada). churn = cancelou em out/2026.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

DATA_REF = "2026-09-30"
PESOS = {"intercepto": -3.2, "atrasos": 0.45, "tickets_90d": 0.25, "criticos_90d": 0.60,
         "tenure_baixo": 1.00, "valor": 0.008}


@dp.materialized_view(
    name="gold_cliente_360",
    comment=("Gold: visão 360 do cliente (1 linha por cliente_id) com features de risco, label de churn e "
             "score_risco heurístico logístico. Foto em 2026-09-30. Sem PII (cpf/nome fora)."),
    table_properties={"camada": "gold"},
    schema="""
        cliente_id STRING COMMENT 'Identificador do cliente',
        plano STRING COMMENT 'Plano contratado',
        uf STRING COMMENT 'Unidade federativa',
        cidade STRING COMMENT 'Cidade',
        valor_mensal DECIMAL(10,2) COMMENT 'Mensalidade em R$',
        tenure_meses INT COMMENT 'Meses de casa até 2026-09-30',
        faturas_atrasadas_12m BIGINT COMMENT 'Qtd. de faturas pagas com atraso ou em aberto nos últimos 12 meses',
        valor_medio_fatura DECIMAL(10,2) COMMENT 'Valor médio das faturas nos 12 meses',
        n_tickets_90d BIGINT COMMENT 'Qtd. de tickets abertos nos últimos 90 dias',
        n_tickets_criticos_90d BIGINT COMMENT 'Tickets de cancelamento ou sinal/qualidade nos últimos 90 dias',
        ultimo_ticket_texto STRING COMMENT 'Texto do ticket mais recente do cliente',
        ultimo_ticket_categoria STRING COMMENT 'Categoria do ticket mais recente',
        churn INT COMMENT 'Label: 1 = cancelou em out/2026, 0 = ativo',
        score_risco DOUBLE COMMENT 'Probabilidade heurística de churn (0..1), fórmula logística documentada',
        faixa_risco STRING COMMENT 'baixo (<0,3) | medio (0,3-0,6) | alto (>=0,6)'
    """,
)
def gold_cliente_360():
    ref = F.lit(DATA_REF).cast("date")
    cli = spark.read.table("silver_clientes")
    fat = (spark.read.table("silver_faturas")
           .filter(F.col("data_vencimento") > F.add_months(ref, -12))
           .groupBy("cliente_id")
           .agg(F.sum(F.col("atrasada").cast("int")).alias("faturas_atrasadas_12m"),
                F.avg("valor").cast("decimal(10,2)").alias("valor_medio_fatura")))
    tk = spark.read.table("silver_tickets")
    tk_agg = (tk.groupBy("cliente_id").agg(
        F.sum((F.col("data_abertura") >= F.date_sub(ref, 90)).cast("int")).alias("n_tickets_90d"),
        F.sum(((F.col("data_abertura") >= F.date_sub(ref, 90))
               & F.col("categoria").isin("cancelamento", "sinal_qualidade")).cast("int")).alias("n_tickets_criticos_90d"),
        F.max_by("texto", "data_abertura").alias("ultimo_ticket_texto"),
        F.max_by("categoria", "data_abertura").alias("ultimo_ticket_categoria")))

    base = (cli.join(fat, "cliente_id", "left").join(tk_agg, "cliente_id", "left")
            .withColumn("tenure_meses", F.floor(F.months_between(ref, "data_adesao")).cast("int"))
            .fillna(0, ["faturas_atrasadas_12m", "n_tickets_90d", "n_tickets_criticos_90d"]))
    z = (F.lit(PESOS["intercepto"])
         + PESOS["atrasos"] * F.col("faturas_atrasadas_12m")
         + PESOS["tickets_90d"] * F.col("n_tickets_90d")
         + PESOS["criticos_90d"] * F.col("n_tickets_criticos_90d")
         + PESOS["tenure_baixo"] * (F.col("tenure_meses") < 12).cast("int")
         + PESOS["valor"] * (F.col("valor_mensal").cast("double") - 100))
    score = F.round(1 / (1 + F.exp(-z)), 4)
    return (base.withColumn("score_risco", score)
            .withColumn("faixa_risco", F.when(F.col("score_risco") >= 0.6, "alto")
                        .when(F.col("score_risco") >= 0.3, "medio").otherwise("baixo"))
            .select("cliente_id", "plano", "uf", "cidade", "valor_mensal", "tenure_meses",
                    F.col("faturas_atrasadas_12m").cast("bigint").alias("faturas_atrasadas_12m"),
                    "valor_medio_fatura",
                    F.col("n_tickets_90d").cast("bigint").alias("n_tickets_90d"),
                    F.col("n_tickets_criticos_90d").cast("bigint").alias("n_tickets_criticos_90d"),
                    "ultimo_ticket_texto", "ultimo_ticket_categoria",
                    (F.col("status") == "cancelado").cast("int").alias("churn"),
                    F.col("score_risco").cast("double").alias("score_risco"), "faixa_risco"))


@dp.materialized_view(
    name="gold_kpis_uf_plano",
    comment="Gold: KPIs de retenção por UF x plano — clientes, churn, receita e receita em risco.",
    table_properties={"camada": "gold"},
    schema="""
        uf STRING COMMENT 'Unidade federativa',
        plano STRING COMMENT 'Plano contratado',
        clientes BIGINT COMMENT 'Total de clientes na foto de 2026-09-30',
        clientes_churn BIGINT COMMENT 'Clientes que cancelaram em out/2026',
        taxa_churn DOUBLE COMMENT 'clientes_churn / clientes',
        receita_mensal DECIMAL(14,2) COMMENT 'Soma das mensalidades (R$)',
        receita_em_risco DECIMAL(14,2) COMMENT 'Soma das mensalidades de clientes com faixa_risco = alto (R$)',
        score_risco_medio DOUBLE COMMENT 'Média do score_risco',
        clientes_alto_risco BIGINT COMMENT 'Clientes com faixa_risco = alto'
    """,
)
def gold_kpis_uf_plano():
    c = spark.read.table("gold_cliente_360")
    alto = F.col("faixa_risco") == "alto"
    return (c.groupBy("uf", "plano").agg(
        F.count("*").alias("clientes"),
        F.sum("churn").cast("bigint").alias("clientes_churn"),
        F.round(F.avg("churn"), 4).alias("taxa_churn"),
        F.sum("valor_mensal").cast("decimal(14,2)").alias("receita_mensal"),
        F.coalesce(F.sum(F.when(alto, F.col("valor_mensal"))), F.lit(0)).cast("decimal(14,2)").alias("receita_em_risco"),
        F.round(F.avg("score_risco"), 4).alias("score_risco_medio"),
        F.sum(alto.cast("int")).cast("bigint").alias("clientes_alto_risco")))
