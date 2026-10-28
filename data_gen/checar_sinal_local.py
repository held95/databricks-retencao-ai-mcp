"""
Checagem local (stdlib) dos arquivos gerados — replica as regras de silver/gold do pipeline
para prever contagens e confirmar que o sinal de churn é aprendível ANTES de subir ao Databricks.

Uso:  py -3 data_gen/checar_sinal_local.py
"""
import csv
import json
import math
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

OUT = Path(__file__).parent / "output"
REF = date(2026, 9, 30)
UFS = set("AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split())

cli_raw = list(csv.DictReader(open(OUT / "clientes/clientes.csv", encoding="utf-8")))
fat_raw = list(csv.DictReader(open(OUT / "faturas/faturas.csv", encoding="utf-8")))
tk_raw = [json.loads(l) for l in open(OUT / "tickets/tickets.jsonl", encoding="utf-8")]
pol_raw = [json.loads(l) for l in open(OUT / "politicas/politicas.jsonl", encoding="utf-8")]

def dedup(rows, chave):
    vistos, out = set(), []
    for r in rows:
        if r[chave] and r[chave] in vistos:
            continue
        vistos.add(r[chave]); out.append(r)
    return out

cli = [r for r in dedup(cli_raw, "cliente_id") if r["cliente_id"] and r["uf"] in UFS]
fat = [r for r in dedup(fat_raw, "fatura_id") if r["cliente_id"] and float(r["valor"]) > 0]
tk = [r for r in dedup(tk_raw, "ticket_id") if r["cliente_id"] and r["texto"]]
print(f"bronze: clientes={len(cli_raw)} faturas={len(fat_raw)} tickets={len(tk_raw)} politicas={len(pol_raw)}")
print(f"silver (previsto): clientes={len(cli)} faturas={len(fat)} tickets={len(tk)} politicas={len(pol_raw)}")

atrasos = defaultdict(int)
for f in fat:
    if date.fromisoformat(f["data_vencimento"]) > REF - timedelta(days=365) and f["status_pagamento"] != "pago":
        atrasos[f["cliente_id"]] += 1
t90, crit = defaultdict(int), defaultdict(int)
for t in tk:
    if datetime.fromisoformat(t["data_abertura"]).date() >= REF - timedelta(days=90):
        t90[t["cliente_id"]] += 1
        if t["categoria"] in ("cancelamento", "sinal_qualidade"):
            crit[t["cliente_id"]] += 1

faixas = defaultdict(lambda: [0, 0])
for c in cli:
    ad = date.fromisoformat(c["data_adesao"])
    tenure = (REF.year - ad.year) * 12 + REF.month - ad.month - (REF.day < ad.day)
    k = c["cliente_id"]
    z = (-3.2 + 0.45 * atrasos[k] + 0.25 * t90[k] + 0.6 * crit[k] + 1.0 * (tenure < 12)
         + 0.008 * (float(c["valor_mensal"]) - 100))
    s = 1 / (1 + math.exp(-z))
    fx = "alto" if s >= 0.6 else "medio" if s >= 0.3 else "baixo"
    faixas[fx][0] += 1
    faixas[fx][1] += c["status"] == "cancelado"
tot = sum(v[0] for v in faixas.values()); ch = sum(v[1] for v in faixas.values())
print(f"churn geral: {ch}/{tot} = {ch / tot:.1%}")
for fx in ("baixo", "medio", "alto"):
    n, c = faixas[fx]
    print(f"  faixa {fx:5s}: clientes={n:4d} churn={c:4d} taxa={c / max(n, 1):.1%}")
