# Transcrição da demo — Copiloto de Retenção Telecom (multiagente + MCP)

- Data: 2026-10-28 06:53
- LLM: `databricks-meta-llama-3-3-70b-instruct`
- thread_id: `1e2bbfa7-f8a7-4861-a53c-f9409a69477d`
- Ferramentas MCP: uc_functions=[]; vector_search=[]; genie=[]; app_custom=['abrir_caso_retencao', 'listar_casos_abertos', 'status_pipeline_retencao', 'clientes_alto_risco', 'get_cliente_360', 'buscar_politicas', 'perguntar_genie']

## Mensagens do supervisor

**Human**:

Pegue o cliente de maior risco em SP, explique por que ele está em risco, qual política de retenção se aplica e abra um caso de retenção.

**Supervisor → `encaminhar`**: `{"especialista": "analista_dados", "instrucao": "Pegue o cliente de maior risco em SP"}`

**analista_dados**:

[analista_dados] O cliente de maior risco em SP é o cliente C00605, que possui um score de risco de 0,7648, classificado como alto risco. Ele está no plano Pós Ilimitado, paga R$ 159,90 por mês e tem 58 meses de tenure. Embora não tenha faturas atrasadas nos últimos 12 meses, ele abriu 6 tickets nos últimos 90 dias, sendo 4 deles críticos. O último ticket foi sobre a falta de sinal 4G dentro de casa em Santos, o que impossibilita fazer ligações. Esses fatores contribuem para o alto risco de churn desse cliente.

**Supervisor → `encaminhar`**: `{"especialista": "especialista_politicas", "instrucao": "Qual política de retenção se aplica ao cliente C00605, que está no plano Pós Ilimitado, paga R$ 159,90 por mês, tem 58 meses de tenure, score de risco de 0,7648, abriu 6 tickets nos últimos 90 dias, sendo 4 deles críticos, e está em alto risco de churn devido a problemas de falta de sinal 4G?"}`

**especialista_politicas**:

[especialista_politicas] Com base na saída da função de busca de políticas, as políticas que se aplicam ao cliente C00605 são:

* P003: Visita técnica prioritária por problema de sinal, que concede visita técnica em até 24 horas e crédito de 1 mensalidade proporcional aos dias sem serviço se o problema for confirmado.
* P014: Retenção de clientes regionais com baixa cobertura, que oferece chip de outra faixa ou repetidor Wi-Fi gratuito em UFs com histórico de reclamação de sinal acima da média.
* P004: Isenção de multa de fidelidade por falha recorrente, que isenta a multa de fidelidade se houver falha de serviço comprovada e recorrente (3 ou mais tickets de sinal em 90 dias).

A política P003 resolve a causa raiz do risco, que é o problema de falta de sinal 4G. Além disso, a política P014 pode ser uma opção complementar, caso o cliente esteja em uma região com baixa cobertura. A política P004 também pode ser considerada, caso o cliente tenha mais de 3 tickets de sinal em 90 dias.

Portanto, a oferta concreta a ser feita ao cliente C00605 é:

* Visita técnica prioritária em até 24 horas para resolver o problema de sinal 4G (P003).
* Oferta de chip de outra faixa ou repetidor Wi-Fi gratuito, caso o cliente esteja em uma região com baixa cobertura (P014).
* Isenção de multa de fidelidade, caso o cliente tenha mais de 3 tickets de sinal em 90 dias (P004).

Trecho da política P003: "Clientes com 2 ou mais tickets de sinal ou qualidade nos últimos 90 dias têm direito a visita técnica prioritária em até 24 horas. Se o problema for confirmado, conceder crédito de 1 mensalidade proporcional aos dias sem serviço."

**Supervisor → `encaminhar`**: `{"especialista": "executor_acoes", "instrucao": "Abra um caso de retenção para o cliente C00605, que está em alto risco de churn devido a problemas de falta de sinal 4G, e aplique as políticas P003, P014 e P004. O cliente paga R$ 159,90 por mês, tem 58 meses de tenure, score de risco de 0,7648, e abriu 6 tickets nos últimos 90 dias, sendo 4 deles críticos."}`

**executor_acoes**:

[executor_acoes] O caso de retenção para o cliente C00605 foi aberto com sucesso. O caso_id criado é e535da0d-ecfe-4665-a114-c0d9da18f2b1, com prioridade alta e status aberto. Os dados foram gravados na tabela workspace.telecom_ai.casos_retencao.

**Supervisor → `finalizar`**: `{"resposta": "O cliente C00605 está em alto risco de churn devido a problemas de falta de sinal 4G. A política de retenção aplicada é a P003, que concede visita técnica prioritária em até 24 horas e crédito de 1 mensalidade proporcional aos dias sem serviço se o problema for confirmado. Além disso, também serão consideradas as políticas P014 e P004. O caso de retenção foi aberto com sucesso e o caso_id é e535da0d-ecfe-4665-a114-c0d9da18f2b1."}`

**Tool**:

O cliente C00605 está em alto risco de churn devido a problemas de falta de sinal 4G. A política de retenção aplicada é a P003, que concede visita técnica prioritária em até 24 horas e crédito de 1 mensalidade proporcional aos dias sem serviço se o problema for confirmado. Além disso, também serão consideradas as políticas P014 e P004. O caso de retenção foi aberto com sucesso e o caso_id é e535da0d-ecfe-4665-a114-c0d9da18f2b1.

## Passo a passo dos especialistas (tool calls MCP)

### supervisor → `analista_dados`

> Pegue o cliente de maior risco em SP

- tool call MCP `clientes_alto_risco` `{"p_uf": "SP", "p_limite": 1}`
  - resultado: `{ "cliente_id": "C00605", "uf": "SP", "cidade": "Santos", "plano": "Pós Ilimitado", "valor_mensal": "159.90", "tenure_meses": "58", "faturas_atrasadas_12m": "0", "n_tickets_90d": "6", "ultimo_ticket_categoria": "sinal_qualidade", "score_risco": "0.7648", "faixa_risco": "alto" }`
- tool call MCP `get_cliente_360` `{"p_cliente_id": "C00605"}`
  - resultado: `{ "cliente_id": "C00605", "plano": "Pós Ilimitado", "uf": "SP", "cidade": "Santos", "valor_mensal": "159.90", "tenure_meses": "58", "faturas_atrasadas_12m": "0", "n_tickets_90d": "6", "n_tickets_criticos_90d": "4", "ultimo_ticket_categoria": "sinal_qualidade", "ultimo_ticket_texto": "Sinal 4G inexistente dentro de casa em Santos, impossível fazer ligações.", "score_risco": "0.7648", "faixa_risco": "alto" }`

**Resposta de `analista_dados`:**

O cliente de maior risco em SP é o cliente C00605, que possui um score de risco de 0,7648, classificado como alto risco. Ele está no plano Pós Ilimitado, paga R$ 159,90 por mês e tem 58 meses de tenure. Embora não tenha faturas atrasadas nos últimos 12 meses, ele abriu 6 tickets nos últimos 90 dias, sendo 4 deles críticos. O último ticket foi sobre a falta de sinal 4G dentro de casa em Santos, o que impossibilita fazer ligações. Esses fatores contribuem para o alto risco de churn desse cliente.

### supervisor → `especialista_politicas`

> Qual política de retenção se aplica ao cliente C00605, que está no plano Pós Ilimitado, paga R$ 159,90 por mês, tem 58 meses de tenure, score de risco de 0,7648, abriu 6 tickets nos últimos 90 dias, sendo 4 deles críticos, e está em alto risco de churn devido a problemas de falta de sinal 4G?

- tool call MCP `buscar_politicas` `{"consulta": "problema de sinal 4G", "k": 3}`
  - resultado: `{ "politica_id": "P003", "titulo": "Visita técnica prioritária por problema de sinal", "texto": "Clientes com 2 ou mais tickets de sinal ou qualidade nos últimos 90 dias têm direito a visita técnica prioritária em até 24 horas. Se o problema for confirmado, conceder crédito de 1 mensalidade proporcional aos dias sem serviço. A visita deve ser registrada como caso de retenção. Não há custo para o cliente.", "score": 1.0 } { "politica_id": "P014", "titulo": "Retenção de clientes regionais com baix`

**Resposta de `especialista_politicas`:**

Com base na saída da função de busca de políticas, as políticas que se aplicam ao cliente C00605 são:

* P003: Visita técnica prioritária por problema de sinal, que concede visita técnica em até 24 horas e crédito de 1 mensalidade proporcional aos dias sem serviço se o problema for confirmado.
* P014: Retenção de clientes regionais com baixa cobertura, que oferece chip de outra faixa ou repetidor Wi-Fi gratuito em UFs com histórico de reclamação de sinal acima da média.
* P004: Isenção de multa de fidelidade por falha recorrente, que isenta a multa de fidelidade se houver falha de serviço comprovada e recorrente (3 ou mais tickets de sinal em 90 dias).

A política P003 resolve a causa raiz do risco, que é o problema de falta de sinal 4G. Além disso, a política P014 pode ser uma opção complementar, caso o cliente esteja em uma região com baixa cobertura. A política P004 também pode ser considerada, caso o cliente tenha mais de 3 tickets de sinal em 90 dias.

Portanto, a oferta concreta a ser feita ao cliente C00605 é:

* Visita técnica prioritária em até 24 horas para resolver o problema de sinal 4G (P003).
* Oferta de chip de outra faixa ou repetidor Wi-Fi gratuito, caso o cliente esteja em uma região com baixa cobertura (P014).
* Isenção de multa de fidelidade, caso o cliente tenha mais de 3 tickets de sinal em 90 dias (P004).

Trecho da política P003: "Clientes com 2 ou mais tickets de sinal ou qualidade nos últimos 90 dias têm direito a visita técnica prioritária em até 24 horas. Se o problema for confirmado, conceder crédito de 1 mensalidade proporcional aos dias sem serviço."

### supervisor → `executor_acoes`

> Abra um caso de retenção para o cliente C00605, que está em alto risco de churn devido a problemas de falta de sinal 4G, e aplique as políticas P003, P014 e P004. O cliente paga R$ 159,90 por mês, tem 58 meses de tenure, score de risco de 0,7648, e abriu 6 tickets nos últimos 90 dias, sendo 4 deles críticos.

- tool call MCP `abrir_caso_retencao` `{"cliente_id": "C00605", "motivo": "Problemas de falta de sinal 4G, alto score de risco de churn (0.7648), 6 tickets abertos nos últimos 90 dias, sendo 4 críticos", "oferta_sugerida": "Políticas P003, P014 e P004", "prioridade": "alta"}`
  - resultado: `{ "caso_id": "e535da0d-ecfe-4665-a114-c0d9da18f2b1", "status": "aberto", "cliente_id": "C00605", "prioridade": "alta", "criado_em_utc": "2026-10-28 09:52:57", "tabela": "workspace.telecom_ai.casos_retencao" }`

**Resposta de `executor_acoes`:**

O caso de retenção para o cliente C00605 foi aberto com sucesso. O caso_id criado é e535da0d-ecfe-4665-a114-c0d9da18f2b1, com prioridade alta e status aberto. Os dados foram gravados na tabela workspace.telecom_ai.casos_retencao.


- MLflow traces: https://dbc-1d83b389-044f.cloud.databricks.com/ml/experiments/2440743401552581/traces

## Resposta final

O cliente C00605 está em alto risco de churn devido a problemas de falta de sinal 4G. A política de retenção aplicada é a P003, que concede visita técnica prioritária em até 24 horas e crédito de 1 mensalidade proporcional aos dias sem serviço se o problema for confirmado. Além disso, também serão consideradas as políticas P014 e P004. O caso de retenção foi aberto com sucesso e o caso_id é e535da0d-ecfe-4665-a114-c0d9da18f2b1.

## Verificação (SQL no warehouse, após a execução)

```sql
SELECT caso_id, cliente_id, prioridade, status, origem, criado_em, oferta_sugerida
FROM workspace.telecom_ai.casos_retencao;
-- e535da0d-ecfe-4665-a114-c0d9da18f2b1 | C00605 | alta | aberto | agente_mcp | 2026-10-28 09:52:57 UTC | Políticas P003, P014 e P004
```

- Trace MLflow: status OK, ~54 s ponta a ponta (experimento `2440743401552581`).
- Observação: neste workspace Free Edition os MCP **gerenciados** responderam `NOT_FOUND: Databricks managed MCP servers are not enabled` (Public Preview desligado), então o supervisor usou automaticamente as ferramentas de leitura equivalentes do MCP **customizado** (mesmas funções UC, mesmo índice, mesmo Genie space). Ver `docs/03_mcp_agentes.md`.
- Execução anterior (prompt sem consultas curtas): a busca recebeu a descrição inteira do cliente, o componente keyword do HYBRID se diluiu e o agente escolheu P011 (desconto) em vez de P003 (sinal). Corrigido instruindo o especialista a fazer consultas curtas focadas na causa raiz.
