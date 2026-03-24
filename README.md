# Lab01_PART1_5479786
## Ingestão de Dados End-to-End (Local) — PNCP Contratos Públicos
## Hercules Ramos Veloso de Freitas
**Disciplina:** Fundamentos de Engenharia de Dados  
**Aluno:** Hercules — NUSP 5479786  
**Fonte de dados:** [Portal Nacional de Contratações Públicas (PNCP)](https://pncp.gov.br)  
**Período coletado:** Janeiro/2021 a Março/2026  
**Total de registros:** ~3,65 milhões de contratos públicos

---
Disclaimer / Aviso Legal Nota Importante: Os dados e análises apresentados neste repositório foram capturados exclusivamente para fins de estudo da API do PNCP. O pipeline de ingestão e processamento não passou por auditoria externa e as informações constantes não devem ser utilizadas como base para decisões oficiais ou denúncias, servindo apenas como demonstração técnica de Engenharia de Dados. O autor não se responsabiliza pela exatidão integral dos dados brutos provenientes da fonte original.
---
## Sumário

1. [Fonte de Dados](#1-fonte-de-dados)
2. [Arquitetura](#2-arquitetura)
3. [Estrutura de Diretórios](#3-estrutura-de-diretórios)
4. [Scripts](#4-scripts)
5. [Camada Bronze](#5-camada-bronze-raw)
6. [Camada Silver](#6-camada-silver-tratamento)
7. [Camada Gold](#7-camada-gold-businesswarehouse)
8. [Dicionário de Dados](#8-dicionário-de-dados)
9. [Qualidade dos Dados](#9-qualidade-dos-dados)
10. [Métricas de Negócio](#10-métricas-de-negócio-5-queries)
11. [Instruções de Execução](#11-instruções-de-execução)

---

## 1. Fonte de Dados

O **PNCP** (Portal Nacional de Contratações Públicas) é o repositório oficial do governo federal brasileiro para publicação de contratos, editais e atas de órgãos públicos, conforme exigido pela Lei 14.133/2021.

- **API:** `https://pncp.gov.br/api/consulta/v1/contratos`
- **Formato:** JSON paginado (até 500 registros/página)
- **Acesso:** Público, sem autenticação
- **Cobertura:** Contratos publicados por todos os entes federativos (federal, estadual, municipal)
- **Riqueza de tipagem:** strings, datas, floats, inteiros, objetos aninhados — atende ao requisito do laboratório

A base possui mais de **1 milhão de linhas** (3,65M no período coletado), com tipagem rica em todas as dimensões exigidas

---

## 2. Arquitetura

### Fluxo geral

```
API PNCP (JSON)
      │
      ▼
 bronze.py ──────────────► data/raw/AAAA_MM/pagina_NNNN.json
      │                     (JSON bruto, sem alterações)
      ▼
 silver.py ──────────────► data/silver/contratos_AAAA_MM.parquet
      │                     (dados limpos, tipados, snake_case)
      ▼
 gold_setup.py ──────────► PostgreSQL: Star Schema (tabelas + índices)
      │
 gold_load.py ───────────► PostgreSQL: carga dos Parquets → fato + dims
```

### Diagrama da Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE (Raw)          SILVER (Tratado)      GOLD (Warehouse)   │
│                                                                  │
│  data/raw/             data/silver/          PostgreSQL          │
│  ├─ 2021_01/           ├─ contratos_         ├─ dim_tempo        │
│  │  ├─ pagina_0001     │  2021_01.parquet    ├─ dim_modalidades  │
│  │  └─ _manifesto      ├─ contratos_         ├─ dim_orgaos       │
│  ├─ 2021_02/           │  2021_02.parquet    ├─ dim_fornecedores │
│  └─ ...                └─ ...               ├─ dim_situacoes    │
│                                              └─ fato_contratos   │
└─────────────────────────────────────────────────────────────────┘
```

### Star Schema (Gold)

```
                    dim_tempo
                   (id_data PK)
                        │
   dim_fornecedores      │        dim_orgaos
  (cnpj_contratada PK)  │       (orgao_entidade_id PK)
          │              │              │
          └──────── fato_contratos ─────┘
                         │
              ┌──────────┴──────────┐
         dim_modalidades      dim_situacoes
        (id_modalidade PK)   (id_situacao PK)
```

---

## 3. Estrutura de Diretórios

```
fundamentos/
├── bronze.py               # Coleta raw da API PNCP
├── silver.py               # Tratamento + relatório + gráficos
├── gold_setup.py           # Cria schema no PostgreSQL
├── gold_load.py            # Carrega Silver → PostgreSQL
├── gold_queries.sql        # 5 queries de negócio
├── docker-compose.yml      # PostgreSQL em container
├── requirements.txt        # Dependências Python
│
├── data/
│   ├── raw/                # Bronze: JSONs brutos por mês
│   │   ├── 2021_01/
│   │   │   ├── pagina_0001.json
│   │   │   └── _manifesto.json
│   │   └── ...
│   ├── silver/             # Silver: Parquets tratados
│   │   ├── contratos_2021_01.parquet
│   │   └── ...
│   └── graficos/           # Relatório + 5 gráficos + Markdown
│       ├── relatorio_qualidade.txt
│       ├── graficos_silver.md
│       ├── 01_boxplot_anos.png
│       ├── 02_histograma.png
│       ├── 03_top_fornecedores.png
│       ├── 04_serie_temporal.png
│       └── 05_correlacoes.png
│
└── logs/
    ├── bronze.log
    ├── silver.log
    └── gold_load.log
```

---

## 4. Scripts

| Script | Camada | Descrição |
|--------|--------|-----------|
| `bronze.py` | Bronze | Coleta paginada da API PNCP com paralelismo e checkpoint |
| `silver.py` | Silver | Tratamento, limpeza, relatório de qualidade e gráficos |
| `gold_setup.py` | Gold | Cria tabelas, índices e popula `dim_tempo` no PostgreSQL |
| `gold_load.py` | Gold | Carrega Parquets Silver → dimensões + fato no PostgreSQL |
| `gold_queries.sql` | Gold | 5 queries SQL de métricas de negócio |
| `docker-compose.yml` | Infra | PostgreSQL 16 com tuning para 16 GB RAM |

---

## 5. Camada Bronze (Raw)

### Objetivo
Ingestão **as-is** da API PNCP — dados salvos sem nenhuma alteração.

### Como funciona

- Cada mês gera uma pasta `data/raw/AAAA_MM/` com um arquivo por página
- Um arquivo `_manifesto.json` é gravado **somente após** todas as páginas do mês serem salvas com sucesso — garante retomada segura se o processo for interrompido
- **Janela deslizante de re-verificação:** os últimos 6 meses são sempre re-verificados para capturar publicações retroativas, com custo mínimo (1 request por mês — a requisição de página 1 já é feita de qualquer forma)

### Configuração da API

```python
API_CONFIG = {
    'page_size':           500,    # máximo aceito pela API
    'max_workers':         3,      # workers paralelos
    'delay_between_pages': 0.5,    # intervalo entre requests (s)
    'backoff_factor':      3,      # espera exponencial em 429s
    'max_retries':         7,
}
```

---

## 6. Camada Silver (Tratamento)

### Etapas aplicadas

1. **Desaninhamento de objetos** — a API retorna campos como `orgaoEntidade`, `unidadeOrgao`, `tipoContrato` e `categoriaProcesso` como objetos JSON aninhados; a função `_flatten_registro()` os expande antes do processamento
2. **Renomeação de colunas** — camelCase → snake_case via `COL_MAP`
3. **Relatório de nulos** — registrado no log por mês, antes de qualquer limpeza
4. **Remoção de duplicatas** — por `id` (número de controle PNCP)
5. **Limpeza de strings** — strip + substituição de artefatos (`None`, `nan`)
6. **Conversão de tipos** — floats para valores monetários, int64 para IDs, `datetime64[ms]` para datas
7. **Filtro de sanidade** — remove contratos com `valor_global > R$ 10 bilhões` (erros de digitação na fonte; ~74k registros, 1,92% do total bruto)
8. **Persistência** — Parquet com compressão Snappy em `data/silver/`

### Uso

```bash
python silver.py                # só tratamento
python silver.py --relatorio    # + relatório de qualidade
python silver.py --graficos     # + 5 gráficos + Markdown
python silver.py --tudo         # tudo
```

---

## 7. Camada Gold (Business/Warehouse)

### Modelo de dados — Star Schema

#### Tabela Fato: `fato_contratos`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_contrato` | SERIAL PK | Chave surrogate |
| `id_contrato_pncp` | VARCHAR | Número de controle PNCP |
| `numero_contrato` | VARCHAR | Número do contrato/empenho |
| `processo` | VARCHAR | Número do processo licitatório |
| `categoria_processo_id` | INTEGER | ID da categoria (1=Obras, 2=Compras...) |
| `categoria_processo` | VARCHAR | Nome da categoria |
| `orgao_entidade_id` | VARCHAR → FK | CNPJ do órgão contratante |
| `cnpj_contratada` | CHAR(14) → FK | CNPJ do fornecedor |
| `id_modalidade` | INTEGER → FK | Tipo de contrato |
| `id_situacao` | INTEGER → FK | Situação do contrato (NULL — ver Qualidade) |
| `data_assinatura` | DATE → FK | Data de assinatura |
| `data_vigencia_inicio` | DATE → FK | Início da vigência |
| `data_vigencia_fim` | DATE → FK | Fim da vigência |
| `data_publicacao` | DATE → FK | Data de publicação no PNCP |
| `valor_inicial` | NUMERIC(18,2) | Valor original do contrato |
| `valor_global` | NUMERIC(18,2) | Valor total consolidado |
| `valor_parcelas` | NUMERIC(18,2) | Valor por parcela |
| `ano_mes_coleta` | CHAR(6) | Mês de coleta (ex: `202503`) |
| `data_carga` | TIMESTAMP | Data/hora da carga no DW |
| `usuario_ingestao` | VARCHAR | Identificador do processo de carga |

---

## 8. Dicionário de Dados

### Camada Silver — campos originais da API PNCP

| Campo Silver | Origem API | Tipo | Descrição |
|---|---|---|---|
| `id` | `numeroControlePNCP` | string | Identificador único do contrato no PNCP |
| `orgao_entidade_id` | `orgaoEntidade.cnpj` | string | CNPJ do órgão/entidade contratante |
| `orgao_entidade_nome` | `orgaoEntidade.razaoSocial` | string | Razão social do órgão contratante |
| `objeto_contrato` | `objetoContrato` | string | Descrição do objeto contratado |
| `numero_contrato` | `numeroContratoEmpenho` | string | Número do contrato ou empenho |
| `processo` | `processo` | string | Número do processo administrativo |
| `categoria_processo_id` | `categoriaProcesso.id` | int64 | ID da categoria (1=Obras, 2=Compras, 3=Serviços...) |
| `categoria_processo_nome` | `categoriaProcesso.nome` | string | Nome da categoria do processo |
| `cnpj_contratada` | `niFornecedor` (quando PJ) | string | CNPJ do fornecedor contratado |
| `nome_contratada` | `nomeRazaoSocialFornecedor` | string | Nome/razão social do contratado |
| `valor_inicial` | `valorInicial` | float64 | Valor inicial do contrato (R$) |
| `valor_global` | `valorGlobal` | float64 | Valor global consolidado (R$) |
| `valor_parcelas` | `valorParcela` | float64 | Valor de cada parcela (R$) |
| `data_assinatura` | `dataAssinatura` | timestamp | Data de assinatura do contrato |
| `data_vigencia_inicio` | `dataVigenciaInicio` | timestamp | Início da vigência contratual |
| `data_vigencia_fim` | `dataVigenciaFim` | timestamp | Fim da vigência contratual |
| `situacao_contrato_id` | `situacaoContratoId` | int64 | ID da situação (**ausente na API** — sempre 0) |
| `situacao_contrato_nome` | `situacaoContratoNome` | string | Nome da situação (**ausente na API** — sempre vazio) |
| `data_publicacao` | `dataPublicacaoPncp` | timestamp | Data de publicação no PNCP |
| `ni_fornecedor` | `niFornecedor` | string | CPF ou CNPJ do fornecedor |
| `nome_razao_social_fornecedor` | `nomeRazaoSocialFornecedor` | string | Nome completo do fornecedor |
| `codigo_unidade` | `unidadeOrgao.codigoUnidade` | string | Código da unidade gestora |
| `nome_unidade` | `unidadeOrgao.nomeUnidade` | string | Nome da unidade gestora |
| `modalidade_id` | `tipoContrato.id` | int64 | ID do tipo/modalidade de contrato |
| `modalidade_nome` | `tipoContrato.nome` | string | Nome do tipo de contrato |
| `ano_mes_coleta` | *(gerado)* | string | Mês de referência da coleta (AAAAMM) |
| `data_coleta` | *(gerado)* | timestamp | Momento da execução do silver.py |

---

## 9. Qualidade dos Dados

### Resumo do relatório gerado por `silver.py --relatorio`

| Métrica | Valor |
|---|---|
| Total bruto coletado | 3.865.548 registros |
| Após filtro de sanidade | 3.791.390 registros |
| Descartados (valor > R$10bi) | 74.158 (1,92%) |
| Período | 2021-04 a 2026-03 |
| Arquivos Parquet | 55 |

### Problemas encontrados

| Coluna | % Nulos/Ausentes | Nível | Causa |
|---|---|---|---|
| `situacao_contrato_nome` | **100%** | 🔴 CRÍTICO | Campo não existe no endpoint `/contratos` da API PNCP v1 |
| `cnpj_contratada` | 4,04% | 🟡 BAIXO | Fornecedores pessoa física têm CPF, não CNPJ |
| `processo` | 0,46% | 🟢 BAIXO | Empenhos diretos sem processo licitatório associado |
| `nome_contratada` | 0,02% | 🟢 BAIXO | Registros sem identificação do contratado |
| `data_vigencia_fim` | 0,006% | 🟢 BAIXO | Contratos sem prazo definido |
| `data_assinatura` (fora 2021-2026) | 0,0003% | 🟢 BAIXO | Erros de digitação na fonte (ex: ano 2102) |

### Zeros sentinela (campos preenchidos com 0 pelo pipeline quando ausentes)

| Coluna | % Zeros | Observação |
|---|---|---|
| `situacao_contrato_id` | ~100% | Campo ausente na API — 0 é sentinela |
| `valor_inicial` | ~26% | Contratos sem valor inicial informado |
| `valor_parcelas` | ~23% | Contratos sem parcelamento |

### Estatísticas descritivas — `valor_global`

| Estatística | Valor |
|---|---|
| Total | R$ 2.021,95 bilhões |
| Média | R$ 533.300,37 |
| Mediana | R$ 3.276,91 |
| Desvio Padrão | R$ 27.763.603,68 |
| P25 | R$ 600,00 |
| P75 | R$ 23.000,00 |
| P95 | R$ 414.800,00 |
| P99 | R$ 3.794.599,65 |

> A mediana muito abaixo da média confirma forte assimetria à direita — maioria dos contratos são de pequeno valor (empenhos), com poucos contratos de grande porte puxando a média para cima.

---

## 10. Métricas de Negócio (7 Queries)

As queries estão em `gold_queries.sql` e respondem às seguintes perguntas:

**Query 1 — Evolução anual por modalidade**
> Qual modalidade de contratação concentra mais valor e como evoluiu ano a ano?

**Query 2 — Top 10 órgãos contratantes**
> Quais entidades públicas mais gastaram em contratos? Qual a concentração por unidade gestora?

**Query 3 — Análise de Pareto dos fornecedores**
> Existe concentração de mercado? Os 20% maiores fornecedores representam 80% do valor?

**Query 4 — Sazonalidade das contratações**
> Em quais meses/trimestres o governo contrata mais? Há pressão de final de exercício orçamentário?

**Query 5 — Estoque de contratos ativos**
> Qual o compromisso financeiro futuro dos contratos ainda vigentes?

**Query 6 — Contratos de universidades e institutos federais**
> Quais contratos foram firmados por universidades, institutos federais e centros universitários? Listagem detalhada ordenada por valor, mais resumo agregado por instituição (Query 6b).

**Query 7 — Top 100 maiores contratos da Universidade de São Paulo**
> Quais os 100 maiores contratos firmados pela USP no período, com fornecedor, unidade responsável, modalidade, datas de vigência e valores?

---

## 11. Instruções de Execução

### Pré-requisitos

- Python 3.10+
- Docker + Docker Compose
- PostgreSQL 16 (via Docker)

### Instalação

```bash
# Clone o repositório

# Crie e ative o ambiente virtual
python3 -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# Instale as dependências
pip install -r requirements.txt
```

### `requirements.txt`

```
requests>=2.31.0
tqdm>=4.66.0
pandas>=2.0.0
pyarrow>=14.0.0
matplotlib>=3.7.0
seaborn>=0.13.0
numpy>=1.26.0
psycopg2-binary>=2.9.0
```

### Ordem de execução

```bash
# 1. Coleta Bronze (~15h — pode deixar rodando no tmux)
tmux new -s pncp
python bronze.py
# Ctrl+B D para desconectar, tmux attach -t pncp para reconectar

# 2. Tratamento Silver + relatório + gráficos (~30min)
python silver.py --tudo

# 3. Sobe o banco PostgreSQL
docker compose up -d
docker compose ps   # aguarda status healthy

# 4. Cria schema Gold e carrega os dados (~10min)
python gold_setup.py
python gold_load.py

# 5. Execute as queries no pgAdmin ou psql
psql -h localhost -U postgres -d pncp_db -f gold_queries.sql
```

### Conexão ao banco (pgAdmin ou DBeaver)

```
Host:     localhost (ou IP do servidor)
Port:     5432
Database: pncp_db
Username: postgres
Password: postgres
```

### Retomada após interrupção

O pipeline é **idempotente** em todas as camadas:

| Script | Checkpoint | Comportamento ao rodar novamente |
|---|---|---|
| `bronze.py` | `_manifesto.json` por mês | Pula meses já coletados; re-verifica os 6 mais recentes |
| `silver.py` | `.parquet` existe em `data/silver/` | Pula meses já tratados |
| `gold_load.py` | `ano_mes_coleta` na fato | Pula meses já carregados; faz UPSERT nas dimensões |

### Reprocessar um mês específico

```bash
# Apaga o Parquet Silver do mês
rm data/silver/contratos_2025_03.parquet

# Remove o mês da fato (no psql/pgAdmin)
DELETE FROM fato_contratos WHERE ano_mes_coleta = '202503';

# Reprocessa
python silver.py
python gold_load.py
```

---
## Gráficos da Camada Silver

Gerados por `python silver.py --graficos` em `data/graficos/`.

### Gráfico 1 — Distribuição de Valores por Ano (Boxplot)

![Boxplot por ano](/data/graficos/01_boxplot_anos.png)

Boxplot em escala log₁₀ mostrando a distribuição do `valor_global` por ano. Cada caixa representa o IQR (P25–P75). Nota-se crescimento consistente no volume de contratos a partir de 2023, reflexo da adesão crescente ao PNCP após a Lei 14.133/2021.

---

### Gráfico 2 — Histograma de Distribuição de Valores

![Histograma](/data/graficos/02_histograma.png)

Distribuição do `valor_global` em escala log₁₀, com frequência linear (esquerda) e logarítmica (direita). A concentração entre R$100 e R$1 milhão reflete o perfil típico de empenhos e contratos de serviços. A cauda direita indica presença de contratos de grande porte (obras e concessões).

---

### Gráfico 3 — Top 20 Fornecedores

![Top fornecedores](/data/graficos/03_top_fornecedores.png)

Painel esquerdo: fornecedores com maior volume financeiro total. Painel direito: fornecedores com maior número de contratos. A diferença entre os dois rankings revela que os maiores em valor não são necessariamente os mais frequentes — contratos de grande porte tendem a ser únicos (obras de infraestrutura, concessões).

---

### Gráfico 4 — Série Temporal Mensal

![Série temporal](/data/graficos/04_serie_temporal.png)

Evolução mês a mês do valor total (acima) e da quantidade de contratos (abaixo). Picos em dezembro refletem a pressão de encerramento do exercício orçamentário. O crescimento acentuado a partir de 2024 indica expansão do uso do PNCP como plataforma obrigatória.

---

## Observações Técnicas

**Por que PNCP e não Kaggle/UCI?**
A base do PNCP atende todos os requisitos do laboratório com dados reais e atuais do governo brasileiro: mais de 1 milhão de linhas, riqueza de tipagem (strings, datas, floats, inteiros, objetos aninhados), relevância para análise de negócio (gastos públicos), e API pública documentada.

**Limitação conhecida da API:**
O endpoint `/contratos` (v1) não retorna o campo `situacaoContrato`. A coluna existe no schema para compatibilidade futura com versões da API, mas permanece nula em 100% dos registros nesta versão.

**Sobre os valores absurdos:**
74.158 registros (1,92%) foram removidos na camada Silver por terem `valor_global > R$ 10 bilhões` — erros de digitação na fonte. Esse filtro foi aplicado na Silver para que a Gold já receba dados limpos.
