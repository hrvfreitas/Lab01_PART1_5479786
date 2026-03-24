-- =============================================================================
-- gold_queries.sql — 13 Queries de Negócio + Diagnóstico
-- Lab01_PART1_5479786 — PNCP Contratos Públicos
-- Hercules Ramos Veloso de Freitas
-- =============================================================================
 
-- =============================================================================
-- DIAGNÓSTICO — Rode PRIMEIRO para validar a carga
-- =============================================================================
 
SELECT COUNT(*) AS total_registros FROM fato_contratos;
 
SELECT
    COUNT(*)                                          AS total,
    COUNT(data_assinatura)                            AS com_data,
    COUNT(*) - COUNT(data_assinatura)                 AS sem_data,
    ROUND((COUNT(*) - COUNT(data_assinatura)) * 100.0
          / NULLIF(COUNT(*), 0), 1)                   AS pct_sem_data
FROM fato_contratos;
 
SELECT
    COUNT(*) FILTER (WHERE valor_global IS NULL)  AS valor_nulo,
    COUNT(*) FILTER (WHERE valor_global = 0)      AS valor_zero,
    COUNT(*) FILTER (WHERE valor_global > 0)      AS valor_positivo
FROM fato_contratos;
 
SELECT
    ano_mes_coleta,
    COUNT(*)                           AS qtd,
    ROUND(SUM(valor_global) / 1e9, 2) AS valor_bi
FROM fato_contratos
WHERE valor_global > 0
GROUP BY ano_mes_coleta
ORDER BY ano_mes_coleta;
 
 
-- =============================================================================
-- Q1 — Evolução por Modalidade
-- Tendência histórica de tipos de contratação por ano.
-- =============================================================================
SELECT
    EXTRACT(YEAR FROM f.data_assinatura)::INT    AS ano,
    COALESCE(m.nome_modalidade, 'Não informado') AS modalidade,
    COUNT(*)                                     AS qtd_contratos,
    ROUND(SUM(f.valor_global) / 1e9, 2)         AS valor_total_bilhoes,
    ROUND(AVG(f.valor_global), 2)                AS valor_medio
FROM fato_contratos f
LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
WHERE f.valor_global > 0
  AND f.data_assinatura IS NOT NULL
GROUP BY ano, modalidade
ORDER BY ano, valor_total_bilhoes DESC;
 
 
-- =============================================================================
-- Q2 — Top 10 Órgãos Gastadores
-- Identificação dos maiores centros de custo do país.
-- =============================================================================
SELECT
    COALESCE(o.nome_orgao, 'ID: ' || f.orgao_entidade_id,
             'Não identificado')                  AS orgao,
    COALESCE(o.nome_unidade, '')                  AS unidade,
    COUNT(*)                                      AS qtd_contratos,
    ROUND(SUM(f.valor_global) / 1e9, 2)          AS valor_total_bilhoes,
    ROUND(AVG(f.valor_global), 2)                 AS ticket_medio,
    ROUND(
        SUM(f.valor_global) * 100.0
        / NULLIF(SUM(SUM(f.valor_global)) OVER (), 0),
    2)                                            AS pct_total
FROM fato_contratos f
LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
WHERE f.valor_global > 0
GROUP BY o.nome_orgao, f.orgao_entidade_id, o.nome_unidade
ORDER BY valor_total_bilhoes DESC
LIMIT 10;
 
 
-- =============================================================================
-- Q3 — Pareto de Fornecedores
-- Análise de dependência e concentração de mercado.
-- =============================================================================
WITH ranking AS (
    SELECT
        COALESCE(forn.nome_contratada,
                 f.cnpj_contratada,
                 'Não identificado')             AS nome,
        f.cnpj_contratada,
        COUNT(*)                                 AS qtd_contratos,
        SUM(f.valor_global)                      AS valor_total,
        ROUND(SUM(f.valor_global) / 1e6, 2)     AS valor_milhoes,
        ROW_NUMBER() OVER (
            ORDER BY SUM(f.valor_global) DESC
        )                                        AS rank
    FROM fato_contratos f
    LEFT JOIN dim_fornecedores forn
           ON f.cnpj_contratada = forn.cnpj_contratada
    WHERE f.valor_global > 0
    GROUP BY f.cnpj_contratada, forn.nome_contratada
),
total AS (SELECT SUM(valor_total) AS grand_total FROM ranking)
SELECT
    r.rank,
    r.nome,
    r.cnpj_contratada,
    r.qtd_contratos,
    r.valor_milhoes,
    ROUND(
        SUM(r.valor_total) OVER (ORDER BY r.rank)
        * 100.0 / NULLIF(t.grand_total, 0),
    2)                                           AS pct_acumulado
FROM ranking r, total t
WHERE r.rank <= 20
ORDER BY r.rank;
 
 
-- =============================================================================
-- Q4 — Sazonalidade Trimestral
-- Pressão orçamentária por período do ano.
-- =============================================================================
SELECT
    EXTRACT(YEAR  FROM data_assinatura)::INT   AS ano,
    EXTRACT(MONTH FROM data_assinatura)::INT   AS mes,
    TO_CHAR(data_assinatura, 'TMMonth')        AS nome_mes,
    EXTRACT(QUARTER FROM data_assinatura)::INT AS trimestre,
    COUNT(*)                                   AS qtd_contratos,
    ROUND(SUM(valor_global) / 1e6, 2)         AS valor_milhoes,
    ROUND(AVG(valor_global), 2)               AS valor_medio,
    ROUND(
        SUM(valor_global) * 100.0
        / NULLIF(
            SUM(SUM(valor_global)) OVER (
                PARTITION BY EXTRACT(YEAR FROM data_assinatura)
            ), 0),
    2)                                         AS pct_no_ano
FROM fato_contratos
WHERE valor_global    >  0
  AND data_assinatura IS NOT NULL
  AND EXTRACT(YEAR FROM data_assinatura) BETWEEN 2021 AND 2026
GROUP BY ano, mes, nome_mes, trimestre
ORDER BY ano, mes;
 
 
-- =============================================================================
-- Q5 — Compromisso Ativo
-- Estoque de contratos vigentes (fluxo de caixa futuro).
-- =============================================================================
SELECT
    COALESCE(s.nome_situacao,
             'Situação ' || f.id_situacao::TEXT,
             'Não informado')                    AS situacao,
    EXTRACT(YEAR FROM f.data_assinatura)::INT    AS ano_assinatura,
    COUNT(*)                                     AS qtd_total,
    ROUND(SUM(f.valor_global) / 1e9, 2)         AS valor_total_bilhoes,
    ROUND(AVG(
        CASE
            WHEN f.data_vigencia_fim IS NOT NULL
             AND f.data_assinatura   IS NOT NULL
            THEN f.data_vigencia_fim - f.data_assinatura
        END
    ), 0)                                        AS duracao_media_dias,
    COUNT(*) FILTER (
        WHERE f.data_vigencia_fim > CURRENT_DATE
    )                                            AS contratos_ainda_ativos,
    ROUND(
        COALESCE(SUM(f.valor_global) FILTER (
            WHERE f.data_vigencia_fim > CURRENT_DATE
        ), 0) / 1e9,
    2)                                           AS valor_ativos_bilhoes
FROM fato_contratos f
LEFT JOIN dim_situacoes s ON f.id_situacao = s.id_situacao
WHERE f.valor_global    >  0
  AND f.data_assinatura IS NOT NULL
GROUP BY s.nome_situacao, f.id_situacao, ano_assinatura
ORDER BY ano_assinatura DESC, valor_total_bilhoes DESC;
 
 
-- =============================================================================
-- Q6 — IFs e Universidades
-- Recorte setorial de Educação Superior e Técnica.
-- =============================================================================
SELECT
    o.nome_orgao,
    o.nome_unidade,
    f.id_contrato_pncp,
    f.numero_contrato,
    f.processo,
    f.categoria_processo,
    COALESCE(m.nome_modalidade, 'Não informado')  AS modalidade,
    f.data_assinatura,
    f.data_vigencia_fim,
    ROUND(f.valor_global, 2)                       AS valor_global,
    ROUND(f.valor_inicial, 2)                      AS valor_inicial,
    CASE
        WHEN f.data_vigencia_fim IS NOT NULL
         AND f.data_assinatura   IS NOT NULL
        THEN f.data_vigencia_fim - f.data_assinatura
    END                                             AS duracao_dias
FROM fato_contratos f
JOIN dim_orgaos o
  ON f.orgao_entidade_id = o.orgao_entidade_id
LEFT JOIN dim_modalidades m
  ON f.id_modalidade = m.id_modalidade
WHERE f.valor_global > 0
  AND (
       o.nome_orgao ILIKE '%universidade%'
    OR o.nome_orgao ILIKE '%instituto federal%'
    OR o.nome_orgao ILIKE '%centro federal%'
    OR o.nome_orgao ILIKE '%CEFET%'
    OR o.nome_orgao ILIKE '%centro universitário%'
    OR o.nome_unidade ILIKE '%universidade%'
    OR o.nome_unidade ILIKE '%instituto federal%'
  )
ORDER BY f.valor_global DESC;
 
-- Q6b — Resumo agregado por instituição
SELECT
    o.nome_orgao,
    COUNT(*)                                       AS qtd_contratos,
    ROUND(SUM(f.valor_global) / 1e6, 2)           AS valor_total_milhoes,
    ROUND(AVG(f.valor_global), 2)                  AS valor_medio,
    ROUND(MIN(f.valor_global), 2)                  AS valor_minimo,
    ROUND(MAX(f.valor_global), 2)                  AS valor_maximo,
    MIN(f.data_assinatura)                         AS primeiro_contrato,
    MAX(f.data_assinatura)                         AS ultimo_contrato
FROM fato_contratos f
JOIN dim_orgaos o
  ON f.orgao_entidade_id = o.orgao_entidade_id
WHERE f.valor_global > 0
  AND (
       o.nome_orgao ILIKE '%universidade%'
    OR o.nome_orgao ILIKE '%instituto federal%'
    OR o.nome_orgao ILIKE '%centro universitário%'
    OR o.nome_orgao ILIKE '%CEFET%'
  )
GROUP BY o.nome_orgao
ORDER BY valor_total_milhoes DESC;
 
 
-- =============================================================================
-- Q7 — Top 100 USP
-- Análise detalhada dos contratos da Universidade de São Paulo.
-- =============================================================================
SELECT
    ROW_NUMBER() OVER (ORDER BY f.valor_global DESC)  AS ranking,
    o.nome_orgao,
    o.nome_unidade,
    f.id_contrato_pncp,
    f.numero_contrato,
    f.processo,
    f.categoria_processo,
    COALESCE(m.nome_modalidade, 'Não informado')       AS modalidade,
    forn.nome_contratada                               AS fornecedor,
    forn.cnpj_contratada,
    f.data_assinatura,
    f.data_vigencia_inicio,
    f.data_vigencia_fim,
    CASE
        WHEN f.data_vigencia_fim IS NOT NULL
         AND f.data_assinatura   IS NOT NULL
        THEN f.data_vigencia_fim - f.data_assinatura
    END                                                AS duracao_dias,
    ROUND(f.valor_global,   2)                         AS valor_global,
    ROUND(f.valor_inicial,  2)                         AS valor_inicial,
    ROUND(f.valor_parcelas, 2)                         AS valor_parcelas
FROM fato_contratos f
JOIN dim_orgaos o
  ON f.orgao_entidade_id = o.orgao_entidade_id
LEFT JOIN dim_modalidades m
  ON f.id_modalidade = m.id_modalidade
LEFT JOIN dim_fornecedores forn
  ON f.cnpj_contratada = forn.cnpj_contratada
WHERE f.valor_global > 0
  AND o.nome_orgao ILIKE '%universidade de são paulo%'
ORDER BY f.valor_global DESC
LIMIT 100;
 
 
-- =============================================================================
-- Q8 — Variação de Valor (Aditivos)
-- Comparação entre Valor Inicial e Global para detectar aditivos.
-- Aditivo = contratos onde valor_global > valor_inicial.
-- =============================================================================
SELECT
    COALESCE(m.nome_modalidade, 'Não informado')     AS modalidade,
    EXTRACT(YEAR FROM f.data_assinatura)::INT        AS ano,
    COUNT(*)                                         AS total_contratos,
    COUNT(*) FILTER (
        WHERE f.valor_global > f.valor_inicial
          AND f.valor_inicial > 0
    )                                                AS com_aditivo,
    ROUND(
        COUNT(*) FILTER (
            WHERE f.valor_global > f.valor_inicial
              AND f.valor_inicial > 0
        ) * 100.0 / NULLIF(COUNT(*), 0),
    2)                                               AS pct_com_aditivo,
    ROUND(AVG(
        CASE
            WHEN f.valor_inicial > 0
            THEN (f.valor_global - f.valor_inicial) / f.valor_inicial * 100
        END
    ), 2)                                            AS variacao_media_pct,
    ROUND(SUM(f.valor_global - f.valor_inicial)
          FILTER (WHERE f.valor_global > f.valor_inicial
                    AND f.valor_inicial > 0) / 1e6,
    2)                                               AS total_aditivos_milhoes
FROM fato_contratos f
LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
WHERE f.valor_global > 0
  AND f.data_assinatura IS NOT NULL
GROUP BY modalidade, ano
ORDER BY ano, total_aditivos_milhoes DESC NULLS LAST;
 
 
-- =============================================================================
-- Q9 — Delay de Publicação
-- Eficiência e transparência: dias entre assinatura e publicação no PNCP.
-- =============================================================================
SELECT
    EXTRACT(YEAR FROM f.data_assinatura)::INT        AS ano,
    COALESCE(m.nome_modalidade, 'Não informado')     AS modalidade,
    COUNT(*)                                         AS total_contratos,
    ROUND(AVG(f.data_publicacao - f.data_assinatura), 1)   AS delay_medio_dias,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY f.data_publicacao - f.data_assinatura
    )                                                AS delay_mediano_dias,
    MAX(f.data_publicacao - f.data_assinatura)       AS delay_maximo_dias,
    COUNT(*) FILTER (
        WHERE f.data_publicacao - f.data_assinatura > 20
    )                                                AS publicacao_tardia_20d,
    COUNT(*) FILTER (
        WHERE f.data_publicacao < f.data_assinatura
    )                                                AS publicacao_antes_assinatura
FROM fato_contratos f
LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
WHERE f.data_assinatura IS NOT NULL
  AND f.data_publicacao IS NOT NULL
  AND f.data_publicacao >= f.data_assinatura
  AND EXTRACT(YEAR FROM f.data_assinatura) BETWEEN 2021 AND 2026
GROUP BY ano, modalidade
ORDER BY ano, delay_medio_dias DESC NULLS LAST;
 
 
-- =============================================================================
-- Q10 — Mediana por Modalidade
-- Perfil financeiro real de cada tipo de licitação.
-- =============================================================================
SELECT
    COALESCE(m.nome_modalidade, 'Não informado')        AS modalidade,
    COUNT(*)                                             AS qtd_contratos,
    ROUND(AVG(f.valor_global), 2)                        AS media,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY f.valor_global
    )                                                    AS mediana,
    PERCENTILE_CONT(0.25) WITHIN GROUP (
        ORDER BY f.valor_global
    )                                                    AS p25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (
        ORDER BY f.valor_global
    )                                                    AS p75,
    ROUND(MIN(f.valor_global), 2)                        AS minimo,
    ROUND(MAX(f.valor_global), 2)                        AS maximo,
    ROUND(SUM(f.valor_global) / 1e9, 2)                 AS total_bilhoes
FROM fato_contratos f
LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
WHERE f.valor_global > 0
GROUP BY modalidade
ORDER BY mediana DESC;
 
 
-- =============================================================================
-- Q11 — Fracionamento
-- Identifica múltiplos contratos com mesmo fornecedor no mesmo mês.
-- Possível indício de fracionamento para fugir de modalidade mais rigorosa.
-- =============================================================================
SELECT
    f.cnpj_contratada,
    forn.nome_contratada,
    f.orgao_entidade_id,
    o.nome_orgao,
    f.ano_mes_coleta                                     AS ano_mes,
    COUNT(*)                                             AS qtd_contratos_mes,
    ROUND(SUM(f.valor_global), 2)                        AS valor_total_mes,
    ROUND(AVG(f.valor_global), 2)                        AS valor_medio_contrato,
    ROUND(MAX(f.valor_global), 2)                        AS maior_contrato
FROM fato_contratos f
LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada
LEFT JOIN dim_orgaos o          ON f.orgao_entidade_id = o.orgao_entidade_id
WHERE f.valor_global > 0
  AND f.cnpj_contratada IS NOT NULL
GROUP BY
    f.cnpj_contratada, forn.nome_contratada,
    f.orgao_entidade_id, o.nome_orgao,
    f.ano_mes_coleta
HAVING COUNT(*) >= 5
ORDER BY qtd_contratos_mes DESC, valor_total_mes DESC
LIMIT 50;
 
 
-- =============================================================================
-- Q12 — Ticket Médio Anual por Categoria
-- Evolução do custo médio dos contratos por categoria.
-- =============================================================================
SELECT
    EXTRACT(YEAR FROM f.data_assinatura)::INT            AS ano,
    f.categoria_processo,
    COUNT(*)                                             AS qtd_contratos,
    ROUND(AVG(f.valor_global), 2)                        AS ticket_medio,
    ROUND(
        AVG(f.valor_global) - LAG(AVG(f.valor_global)) OVER (
            PARTITION BY f.categoria_processo
            ORDER BY EXTRACT(YEAR FROM f.data_assinatura)
        ),
    2)                                                   AS variacao_abs_vs_ano_ant,
    ROUND(
        (AVG(f.valor_global) - LAG(AVG(f.valor_global)) OVER (
            PARTITION BY f.categoria_processo
            ORDER BY EXTRACT(YEAR FROM f.data_assinatura)
        )) * 100.0
        / NULLIF(LAG(AVG(f.valor_global)) OVER (
            PARTITION BY f.categoria_processo
            ORDER BY EXTRACT(YEAR FROM f.data_assinatura)
        ), 0),
    2)                                                   AS variacao_pct_vs_ano_ant,
    ROUND(SUM(f.valor_global) / 1e9, 2)                 AS total_bilhoes
FROM fato_contratos f
WHERE f.valor_global    > 0
  AND f.data_assinatura IS NOT NULL
  AND f.categoria_processo IS NOT NULL
  AND EXTRACT(YEAR FROM f.data_assinatura) BETWEEN 2021 AND 2026
GROUP BY ano, f.categoria_processo
ORDER BY f.categoria_processo, ano;
 
 
-- =============================================================================
-- Q13 — Geografia por CNPJ Raiz do Órgão
-- Concentração de gastos por esfera administrativa (CNPJ raiz = 8 dígitos).
-- =============================================================================
SELECT
    SUBSTRING(f.orgao_entidade_id, 1, 8)               AS cnpj_raiz,
    o.nome_orgao,
    COUNT(*)                                            AS total_contratos,
    ROUND(SUM(f.valor_global) / 1e9, 2)                AS total_bilhoes,
    ROUND(AVG(f.valor_global), 2)                       AS ticket_medio,
    COUNT(DISTINCT f.cnpj_contratada)                   AS fornecedores_distintos,
    MIN(f.data_assinatura)                              AS primeiro_contrato,
    MAX(f.data_assinatura)                              AS ultimo_contrato
FROM fato_contratos f
JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
WHERE f.valor_global > 0
GROUP BY cnpj_raiz, o.nome_orgao
ORDER BY total_bilhoes DESC
LIMIT 15;
