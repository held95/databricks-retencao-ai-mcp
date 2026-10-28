"""
Gerador de dados sintéticos — Copiloto de Retenção Telecom.

Gera 4 entidades (somente stdlib, seed fixa => reprodutível):
  - clientes   (CSV)   ~800 linhas
  - faturas    (CSV)   ~9.000 linhas (12 meses: out/2025 .. set/2026)
  - tickets    (JSONL) ~1.500 linhas com reclamações em texto livre pt-BR
  - politicas  (JSONL) 15 políticas de retenção

Sinal de churn "aprendível" embutido (label = cancelou em out/2026, logo após a janela):
  atrasos de fatura, tickets recentes de cancelamento/sinal, tenure baixo e plano caro
  aumentam a probabilidade de churn (modelo logístico latente + ruído).

Linhas sujas propositais (para as expectations do pipeline agirem):
  ids nulos, valores negativos, duplicatas exatas, textos vazios, UF inválida.

Uso:  py -3 data_gen/gerar_dados.py   (saída em data_gen/output/<entidade>/)
"""
from __future__ import annotations

import csv
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
N_CLIENTES = 800
DATA_REF = date(2026, 9, 30)  # fim da janela observada
MESES = [(2025 + (9 + i) // 12, (9 + i) % 12 + 1) for i in range(12)]  # out/2025..set/2026
SAIDA = Path(__file__).parent / "output"

rnd = random.Random(SEED)

# --------------------------------------------------------------------------------------
# Dimensões
# --------------------------------------------------------------------------------------
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela",
         "João", "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Pedro", "Rafaela", "Samuel",
         "Tatiane", "Vinícius", "Larissa", "Thiago", "Juliana", "Rodrigo", "Camila", "Gustavo",
         "Beatriz", "André", "Fernanda", "Marcelo", "Patrícia", "Ricardo", "Letícia", "Daniel"]
SOBRENOMES = ["Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira",
              "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes",
              "Soares", "Fernandes", "Vieira", "Barbosa", "Rocha", "Dias", "Nascimento", "Moreira"]
UF_CIDADES = {
    "SP": (["São Paulo", "Campinas", "Santos", "Ribeirão Preto"], 0.28),
    "RJ": (["Rio de Janeiro", "Niterói", "Petrópolis"], 0.14),
    "MG": (["Belo Horizonte", "Uberlândia", "Juiz de Fora"], 0.12),
    "RS": (["Porto Alegre", "Caxias do Sul"], 0.07),
    "PR": (["Curitiba", "Londrina"], 0.07),
    "BA": (["Salvador", "Feira de Santana"], 0.08),
    "PE": (["Recife", "Olinda"], 0.06),
    "CE": (["Fortaleza"], 0.05),
    "DF": (["Brasília"], 0.05),
    "GO": (["Goiânia"], 0.04),
    "SC": (["Florianópolis", "Joinville"], 0.04),
}
# plano -> (valor_mensal, peso)
PLANOS = {
    "Controle 20GB": (49.90, 0.30),
    "Pós 50GB": (99.90, 0.25),
    "Fibra 500MB": (119.90, 0.20),
    "Pós Ilimitado": (159.90, 0.15),
    "Combo Fibra + Móvel": (219.90, 0.10),
}
CANAIS_AQ = ["loja", "app", "televendas", "site", "parceiro"]
CANAIS_TICKET = ["app", "call_center", "chat", "loja", "reclame_aqui", "anatel"]


def escolher_ponderado(d: dict):
    chaves = list(d)
    return rnd.choices(chaves, weights=[d[k][1] for k in chaves], k=1)[0]


def gerar_cpf() -> str:
    """CPF fictício com dígitos verificadores válidos (formato 000.000.000-00)."""
    n = [rnd.randint(0, 9) for _ in range(9)]
    for _ in range(2):
        s = sum(v * p for v, p in zip(n, range(len(n) + 1, 1, -1)))
        d = (s * 10) % 11
        n.append(0 if d == 10 else d)
    t = "".join(map(str, n))
    return f"{t[:3]}.{t[3:6]}.{t[6:9]}-{t[9:]}"


# --------------------------------------------------------------------------------------
# Textos de tickets (pt-BR, variados por categoria e sentimento)
# --------------------------------------------------------------------------------------
TEXTOS = {
    "cobranca": {
        "negativo": [
            "Fui cobrado duas vezes na fatura de {mes}, já liguei três vezes e ninguém resolve.",
            "Apareceu um serviço de {servico} que eu nunca contratei na minha conta. Quero estorno imediato.",
            "O valor da fatura veio R$ {valor} acima do combinado na venda. Isso é propaganda enganosa.",
            "Paguei o boleto em dia e mesmo assim consta como atrasado, recebi cobrança indevida.",
            "Reajuste abusivo no plano sem aviso prévio, a fatura subiu muito este mês.",
        ],
        "neutro": [
            "Gostaria de entender a cobrança proporcional que veio na fatura de {mes}.",
            "Preciso da segunda via do boleto e da alteração da data de vencimento para o dia {dia}.",
            "Solicito detalhamento dos itens cobrados na última fatura.",
        ],
        "positivo": [
            "Obrigado, o estorno da cobrança duplicada caiu certinho na fatura de {mes}.",
            "Atendente resolveu rápido a dúvida sobre a fatura, muito bom.",
        ],
    },
    "sinal_qualidade": {
        "negativo": [
            "A internet cai toda noite desde {mes}, trabalho em home office e estou sendo prejudicado.",
            "Sinal 4G inexistente dentro de casa em {cidade}, impossível fazer ligações.",
            "Velocidade da fibra entrega menos de 30% do contratado, já fiz vários testes.",
            "Fiquei {dias} dias sem internet e o técnico não apareceu na visita agendada.",
            "Ligações caindo o tempo todo, a qualidade piorou muito nos últimos meses.",
        ],
        "neutro": [
            "Gostaria de agendar uma visita técnica para verificar o roteador.",
            "A conexão está oscilando um pouco, podem verificar a linha?",
            "Há manutenção programada na minha região em {cidade}?",
        ],
        "positivo": [
            "Técnico veio no mesmo dia e a internet voltou a funcionar perfeitamente.",
            "Depois da troca do roteador a velocidade ficou ótima, obrigado.",
        ],
    },
    "portabilidade": {
        "negativo": [
            "Recebi oferta de outra operadora pela metade do preço, vou fazer a portabilidade se não melhorarem.",
            "Pedi portabilidade do meu número há {dias} dias e até agora nada, perdi minha linha.",
            "A portabilidade foi feita errada e estou sem conseguir receber ligações.",
        ],
        "neutro": [
            "Quais são os prazos para trazer meu número de outra operadora?",
            "Quero saber como funciona a portabilidade para o plano {plano}.",
        ],
        "positivo": [
            "Portabilidade concluída sem problemas, número já está funcionando.",
        ],
    },
    "cancelamento": {
        "negativo": [
            "Quero cancelar meu plano. Cansei de pagar caro por um serviço que não funciona.",
            "Solicito o cancelamento imediato, já estou contratando com a concorrência.",
            "Tentei cancelar pelo app e não consigo, fico preso no atendimento eletrônico. Quero cancelar!",
            "Vou cancelar e registrar reclamação na Anatel se a multa de fidelidade for cobrada.",
            "Não aguento mais, quero cancelar tudo: internet e móvel.",
        ],
        "neutro": [
            "Qual o valor da multa caso eu cancele antes do fim da fidelidade?",
            "Estou pensando em cancelar por questão de orçamento, existe alguma alternativa?",
        ],
        "positivo": [
            "Desisti do cancelamento após a oferta de desconto, obrigado pela atenção.",
        ],
    },
    "upgrade": {
        "negativo": [
            "Pedi upgrade para o {plano} e continuo com a velocidade antiga.",
            "Fizeram upgrade no meu plano sem minha autorização e a fatura aumentou.",
        ],
        "neutro": [
            "Gostaria de conhecer os planos com mais franquia de dados.",
            "Quero migrar para o {plano}, quais as condições?",
            "Tem algum combo de fibra com móvel disponível para minha região?",
        ],
        "positivo": [
            "Adorei o novo plano {plano}, a velocidade melhorou bastante!",
            "Upgrade feito em minutos pelo app, excelente experiência.",
        ],
    },
}
SERVICOS = ["streaming de música", "seguro celular", "revista digital", "jogos premium"]
MESES_NOME = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto",
              "setembro", "outubro", "novembro", "dezembro"]


def texto_ticket(categoria: str, sentimento: str, cliente: dict) -> str:
    t = rnd.choice(TEXTOS[categoria][sentimento])
    return t.format(
        mes=rnd.choice(MESES_NOME), servico=rnd.choice(SERVICOS),
        valor=f"{rnd.randint(15, 90)},{rnd.randint(0, 99):02d}", dia=rnd.choice([5, 10, 15, 20, 25]),
        cidade=cliente["cidade"], dias=rnd.randint(2, 15), plano=rnd.choice(list(PLANOS)),
    )


# --------------------------------------------------------------------------------------
# Políticas de retenção
# --------------------------------------------------------------------------------------
POLITICAS = [
    ("Desconto progressivo para clientes fiéis de alto risco",
     "Clientes com mais de 24 meses de casa e score de risco de churn acima de 0,6 podem receber desconto de 20% por 6 meses. "
     "O desconto é aplicado automaticamente a partir da próxima fatura. Não é cumulativo com outras promoções vigentes. "
     "Exige ausência de faturas em aberto há mais de 60 dias."),
    ("Bônus de dados para clientes de planos Controle",
     "Clientes do plano Controle 20GB com risco médio ou alto podem receber 10GB de bônus mensal por 3 meses. "
     "O bônus é concedido sem alteração de fidelidade. Indicado quando o motivo da insatisfação é franquia insuficiente. "
     "Limite de uma concessão a cada 12 meses."),
    ("Visita técnica prioritária por problema de sinal",
     "Clientes com 2 ou mais tickets de sinal ou qualidade nos últimos 90 dias têm direito a visita técnica prioritária em até 24 horas. "
     "Se o problema for confirmado, conceder crédito de 1 mensalidade proporcional aos dias sem serviço. "
     "A visita deve ser registrada como caso de retenção. Não há custo para o cliente."),
    ("Isenção de multa de fidelidade por falha recorrente",
     "Quando houver falha de serviço comprovada e recorrente (3 ou mais tickets de sinal em 90 dias), a multa de fidelidade pode ser isentada. "
     "Antes da isenção, o atendente deve ofertar ao menos uma alternativa de retenção. "
     "A isenção exige aprovação do supervisor de retenção. Documentar o motivo no caso."),
    ("Estorno imediato de cobrança indevida",
     "Cobranças indevidas comprovadas devem ser estornadas na fatura seguinte com correção. "
     "Clientes com reclamação de cobrança e risco alto recebem adicionalmente 10% de desconto no mês seguinte. "
     "O estorno deve ser confirmado por escrito ao cliente. Reincidência deve ser escalada para a área de faturamento."),
    ("Downgrade assistido para clientes com restrição de orçamento",
     "Clientes que citam preço ou orçamento como motivo de cancelamento podem migrar para um plano inferior sem multa. "
     "O atendente deve simular o novo valor e mostrar a economia anual. "
     "O downgrade preserva o número e os benefícios de fidelidade acumulados. Preferível ao cancelamento."),
    ("Contra-oferta de portabilidade",
     "Clientes que mencionam oferta de outra operadora ou portabilidade podem receber cobertura de preço de até 15% sobre o valor atual. "
     "A contra-oferta vale por 12 meses com nova fidelidade de 12 meses. "
     "Exige tenure mínimo de 6 meses. Não se aplica a clientes com mais de 2 faturas atrasadas nos últimos 12 meses."),
    ("Upgrade gratuito temporário",
     "Clientes de Fibra 500MB com risco alto podem receber upgrade gratuito de velocidade por 3 meses. "
     "Após o período, o cliente escolhe se mantém o plano superior com 30% de desconto ou retorna ao original. "
     "Indicado para clientes sem pendências financeiras. Limite de uma concessão por cliente."),
    ("Renegociação de débitos com retenção",
     "Clientes com 3 ou mais faturas atrasadas nos últimos 12 meses podem parcelar o débito em até 6 vezes sem juros. "
     "A renegociação deve ser oferecida antes de qualquer desconto comercial. "
     "Após a renegociação, o cliente fica elegível a outras políticas somente com 3 meses de adimplência."),
    ("Programa de boas-vindas para clientes novos",
     "Clientes com menos de 6 meses de casa e qualquer ticket negativo recebem contato proativo do time de sucesso do cliente. "
     "Pode ser concedido 1 mês de serviço adicional (streaming parceiro) sem custo. "
     "O objetivo é reduzir o churn precoce, que é o mais alto da base."),
    ("Combo com desconto para clientes de alto valor",
     "Clientes dos planos Pós Ilimitado ou Combo Fibra + Móvel com risco acima de 0,5 podem receber 25% de desconto por 4 meses. "
     "A oferta exige manutenção do combo completo. "
     "Clientes com receita mensal acima de R$ 150 têm prioridade de atendimento na fila de retenção."),
    ("Atendimento por canal regulatório",
     "Tickets abertos via Anatel ou Reclame Aqui devem ser tratados em até 48 horas pela célula de retenção especializada. "
     "O atendente pode aplicar qualquer política de retenção sem aprovação adicional até o limite de 20% de desconto. "
     "Resposta pública deve ser registrada após solução."),
    ("Limites gerais de concessão",
     "Nenhum cliente pode acumular mais de 30% de desconto total sobre a mensalidade. "
     "Descontos acima de 25% exigem aprovação do supervisor. "
     "Benefícios de retenção não podem ser concedidos a clientes com suspeita de fraude ou CPF irregular."),
    ("Retenção de clientes regionais com baixa cobertura",
     "Em UFs com histórico de reclamação de sinal acima da média, clientes podem receber chip de outra faixa ou repetidor Wi-Fi gratuito. "
     "A oferta vale para clientes móveis e de fibra. "
     "O custo do equipamento é absorvido pela área de rede. Registrar a UF no caso de retenção."),
    ("Fluxo padrão de cancelamento",
     "Toda solicitação de cancelamento deve passar por uma tentativa de retenção registrada. "
     "Se o cliente recusar duas ofertas, o cancelamento deve ser efetivado imediatamente, conforme regulamentação da Anatel. "
     "É proibido dificultar o cancelamento. O motivo principal deve ser classificado no caso."),
]


# --------------------------------------------------------------------------------------
# Geração
# --------------------------------------------------------------------------------------
def sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def main() -> None:
    clientes, faturas, tickets = [], [], []
    fat_seq, tkt_seq = 0, 0

    for i in range(1, N_CLIENTES + 1):
        uf = escolher_ponderado(UF_CIDADES)
        plano = escolher_ponderado(PLANOS)
        tenure = rnd.randint(1, 11) if rnd.random() < 0.15 else rnd.randint(12, 84)  # meses de casa
        adesao = DATA_REF - timedelta(days=int(tenure * 30.4) + rnd.randint(0, 20))
        cli = {
            "cliente_id": f"C{i:05d}",
            "nome": f"{rnd.choice(NOMES)} {rnd.choice(SOBRENOMES)} {rnd.choice(SOBRENOMES)}",
            "cpf": gerar_cpf(),
            "uf": uf,
            "cidade": rnd.choice(UF_CIDADES[uf][0]),
            "plano": plano,
            "valor_mensal": f"{PLANOS[plano][0]:.2f}",
            "data_adesao": adesao.isoformat(),
            "canal_aquisicao": rnd.choice(CANAIS_AQ),
        }
        # Perfil latente do cliente
        perfil_atraso = rnd.choices([0.03, 0.15, 0.40], weights=[0.70, 0.20, 0.10])[0]
        insatisfacao = rnd.random()  # 0..1

        # Faturas: 12 meses (ou desde a adesão)
        n_atrasadas = 0
        for (ano, mes) in MESES:
            venc = date(ano, mes, rnd.choice([5, 10, 15, 20]))
            if venc < adesao:
                continue
            fat_seq += 1
            valor = PLANOS[plano][0] * rnd.uniform(0.95, 1.12)
            atrasou = rnd.random() < perfil_atraso
            if atrasou:
                n_atrasadas += 1
                dias = rnd.randint(5, 75)
                pag = venc + timedelta(days=dias)
                status = "em_aberto" if pag > DATA_REF else "pago_com_atraso"
                pag_str = "" if pag > DATA_REF else pag.isoformat()
            else:
                pag = venc - timedelta(days=rnd.randint(0, 6))
                status, pag_str = "pago", pag.isoformat()
            faturas.append({
                "fatura_id": f"F{fat_seq:06d}", "cliente_id": cli["cliente_id"],
                "mes_referencia": f"{ano}-{mes:02d}", "data_vencimento": venc.isoformat(),
                "data_pagamento": pag_str, "valor": f"{valor:.2f}", "status_pagamento": status,
            })

        # Tickets: volume e categoria dependem da insatisfação
        n_tk = min(12, int(rnd.expovariate(1 / (1.2 + 3.8 * insatisfacao ** 2))))
        n_criticos_90d = 0
        for _ in range(n_tk):
            if insatisfacao > 0.7:
                cat = rnd.choices(list(TEXTOS), weights=[0.2, 0.3, 0.15, 0.3, 0.05])[0]
                sent = rnd.choices(["negativo", "neutro", "positivo"], weights=[0.8, 0.15, 0.05])[0]
                dias_atras = int(rnd.triangular(0, 365, 20))  # recentes
            else:
                cat = rnd.choices(list(TEXTOS), weights=[0.3, 0.2, 0.1, 0.07, 0.33])[0]
                sent = rnd.choices(["negativo", "neutro", "positivo"], weights=[0.35, 0.4, 0.25])[0]
                dias_atras = rnd.randint(0, 365)
            abertura = DATA_REF - timedelta(days=dias_atras, minutes=rnd.randint(0, 1439))
            if abertura.toordinal() < adesao.toordinal():
                abertura = adesao + timedelta(days=rnd.randint(0, 10))
            if dias_atras <= 90 and cat in ("cancelamento", "sinal_qualidade") and sent == "negativo":
                n_criticos_90d += 1
            tkt_seq += 1
            tickets.append({
                "ticket_id": f"T{tkt_seq:06d}", "cliente_id": cli["cliente_id"],
                "data_abertura": f"{abertura.isoformat()}T{rnd.randint(7, 22):02d}:{rnd.randint(0, 59):02d}:00",
                "canal": rnd.choice(CANAIS_TICKET), "categoria": cat,
                "texto": texto_ticket(cat, sent, cli),
                "resolvido": rnd.random() < (0.45 if sent == "negativo" else 0.85),
            })

        # Label de churn (cancelou em out/2026) — logística latente + ruído
        logit = (-3.0 + 0.45 * n_atrasadas + 0.9 * n_criticos_90d + 1.2 * (tenure < 12)
                 + 0.008 * (PLANOS[plano][0] - 100) + 1.5 * (insatisfacao - 0.5) + rnd.gauss(0, 0.5))
        churn = rnd.random() < sigmoid(logit)
        cli["status"] = "cancelado" if churn else "ativo"
        cli["data_cancelamento"] = (DATA_REF + timedelta(days=rnd.randint(1, 27))).isoformat() if churn else ""
        clientes.append(cli)

    # ---------------- linhas sujas propositais ----------------
    sujos_cli = [dict(clientes[5], cliente_id=""), dict(clientes[9], cliente_id=""),
                 dict(clientes[20], uf="XX", cliente_id="C99901")]
    clientes += sujos_cli + [dict(clientes[k]) for k in (1, 2, 3)]  # 3 duplicatas exatas
    sujos_fat = [dict(faturas[k], valor=f"-{faturas[k]['valor']}", fatura_id=f"F9{k:05d}") for k in (10, 50, 90, 130, 170)]
    sujos_fat += [dict(faturas[k], cliente_id="", fatura_id=f"F8{k:05d}") for k in (11, 51, 91)]
    faturas += sujos_fat + [dict(faturas[k]) for k in range(200, 210)]  # 10 duplicatas
    sujos_tk = [dict(tickets[k], texto="", ticket_id=f"T9{k:05d}") for k in (3, 30, 300)]
    sujos_tk += [dict(tickets[k], cliente_id=None, ticket_id=f"T8{k:05d}") for k in (4, 40)]
    tickets += sujos_tk + [dict(tickets[k]) for k in range(500, 505)]  # 5 duplicatas
    rnd.shuffle(clientes); rnd.shuffle(faturas); rnd.shuffle(tickets)

    # ---------------- escrita ----------------
    for ent in ("clientes", "faturas", "tickets", "politicas"):
        (SAIDA / ent).mkdir(parents=True, exist_ok=True)
    with open(SAIDA / "clientes" / "clientes.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(clientes[0]))
        w.writeheader(); w.writerows(clientes)
    with open(SAIDA / "faturas" / "faturas.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(faturas[0]))
        w.writeheader(); w.writerows(faturas)
    with open(SAIDA / "tickets" / "tickets.jsonl", "w", encoding="utf-8") as f:
        for t in tickets:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    with open(SAIDA / "politicas" / "politicas.jsonl", "w", encoding="utf-8") as f:
        for k, (titulo, texto) in enumerate(POLITICAS, 1):
            f.write(json.dumps({"politica_id": f"P{k:03d}", "titulo": titulo, "texto": texto,
                                "vigente_desde": "2026-01-01"}, ensure_ascii=False) + "\n")

    n_churn = sum(1 for c in clientes if c["status"] == "cancelado")
    print(f"clientes={len(clientes)} (churn={n_churn}) faturas={len(faturas)} "
          f"tickets={len(tickets)} politicas={len(POLITICAS)}")


if __name__ == "__main__":
    main()
