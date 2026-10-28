"""
Camada BRONZE — ingestão incremental via Auto Loader (cloudFiles) como streaming tables.

Cada entidade é lida de /Volumes/workspace/telecom_ai/raw/landing/<entidade>/ e gravada "como veio"
(todas as colunas string no CSV, sem limpeza), acrescida de metadados de linhagem do arquivo.
O checkpoint e o schema location do Auto Loader são gerenciados pelo próprio pipeline.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/telecom_ai/raw/landing"


def _auto_loader(entidade: str, formato: str):
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.inferColumnTypes", "false")      # bronze = tudo como string
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if formato == "csv":
        leitor = leitor.option("header", "true")
    return (
        leitor.load(f"{LANDING}/{entidade}/")
        .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("_ingerido_em", F.current_timestamp())
    )


@dp.table(name="bronze_clientes",
          comment="Bronze: cadastro de clientes bruto (CSV via Auto Loader), sem limpeza.",
          table_properties={"camada": "bronze"})
def bronze_clientes():
    return _auto_loader("clientes", "csv")


@dp.table(name="bronze_faturas",
          comment="Bronze: faturas mensais brutas (CSV via Auto Loader), sem limpeza.",
          table_properties={"camada": "bronze"})
def bronze_faturas():
    return _auto_loader("faturas", "csv")


@dp.table(name="bronze_tickets",
          comment="Bronze: tickets de atendimento brutos com texto livre (JSONL via Auto Loader).",
          table_properties={"camada": "bronze"})
def bronze_tickets():
    return _auto_loader("tickets", "json")


@dp.table(name="bronze_politicas",
          comment="Bronze: políticas de retenção em texto (JSONL via Auto Loader).",
          table_properties={"camada": "bronze"})
def bronze_politicas():
    return _auto_loader("politicas", "json")
