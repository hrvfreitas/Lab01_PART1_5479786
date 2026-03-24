-- =============================================================================
-- gold_queries.sql — Diagnóstico + 5 Métricas de Negócio
-- Laboratório 01-A — PNCP Contratos Públicos
-- Hercules Ramos Veloso de Freitas
-- hercules.veloso@gmail.com

-- =============================================================================


-- ============================================================
-- DIAGNÓSTICO — Rode PRIMEIRO para entender o estado dos dados
-- ============================================================

-- 1. Total de registros na fato
SELECT COUNT(*) AS total_registros FROM fato_contratos;

-- 2. Cobertura de data_assinatura (causa #1 de resultados em branco)
SELECT
    COUNT(*)                                         AS total,
    COUNT(data_assinatura)                           AS com_data,
    COUNT(*) - COUNT(data_assinatura)                AS sem_data,
    ROUND((COUNT(*) - COUNT(data_assinatura)) * 100.0
          / NULLIF(COUNT(*), 0), 1)                  AS pct_sem_data
FROM fato_contratos;

-- 3. Cobertura de valor_global
SELECT
    COUNT(*) FILTER (WHERE valor_global IS NULL) AS valor_nulo,
    COUNT(*) FILTER (WHERE valor_global = 0)     AS valor_zero,
    COUNT(*) FILTER (WHERE valor_global > 0)     AS valor_positivo
FROM fato_contratos;

-- 4. Cobertura das FKs (NULL = JOIN vai excluir a linha)
SELECT
    COUNT(*) FILTER (WHERE orgao_entidade_id IS NULL) AS orgao_nulo,
    COUNT(*) FILTER (WHERE cnpj_contratada   IS NULL) AS cnpj_nulo,
    COUNT(*) FILTER (WHERE id_modalidade     IS NULL) AS modalidade_nula,
    COUNT(*) FILTER (WHERE id_situacao       IS NULL) AS situacao_nula
FROM fato_contratos;

-- 5. Distribuição por ano_mes sem JOIN (valida que a carga funcionou)
SELECT
    ano_mes_coleta,
    COUNT(*)                           AS qtd,
    ROUND(SUM(valor_global) / 1e9, 2) AS valor_bi
FROM fato_contratos
WHERE valor_global > 0
GROUP BY ano_mes_coleta
ORDER BY ano_mes_coleta;


-- ============================================================
-- ============================================================
SELECT
    EXTRACT(YEAR FROM f.data_assinatura)::INT    AS ano,
    COALESCE(m.nome_modalidade, 'Não informado') AS modalidade,
    COUNT(*)                                     AS qtd_contratos,
    ROUND(SUM(f.valor_global) / 1e9, 2)         AS valor_total_bilhoes,
    ROUND(AVG(f.valor_global), 2)                AS valor_medio
FROM fato_contratos f
LEFT JOIN dim_modalidades m ON f.id_modalidade = m.id_modalidade
WHERE f.valor_global    >  0
  AND f.data_assinatura IS NOT NULL
GROUP BY ano, modalidade
ORDER BY ano, valor_total_bilhoes DESC;


-- ============================================================
-- QUERY 2: Top 10 órgãos contratantes por valor total
--
-- ============================================================
SELECT
    COALESCE(o.nome_orgao,
             'ID: ' || f.orgao_entidade_id,
             'Não identificado')                  AS orgao,
    COALESCE(o.nome_unidade, '')                 AS unidade,
    COUNT(*)                                     AS qtd_contratos,
    ROUND(SUM(f.valor_global) / 1e9, 2)         AS valor_total_bilhoes,
    ROUND(AVG(f.valor_global), 2)                AS ticket_medio,
    ROUND(
        SUM(f.valor_global) * 100.0
        / NULLIF(SUM(SUM(f.valor_global)) OVER (), 0),
    2)                                           AS pct_total
FROM fato_contratos f
LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id
WHERE f.valor_global > 0
GROUP BY o.nome_orgao, f.orgao_entidade_id, o.nome_unidade
ORDER BY valor_total_bilhoes DESC
LIMIT 10;


-- ============================================================
-- QUERY 3: Concentração de mercado — Pareto dos fornecedores
--
-- ============================================================
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


-- ============================================================
-- QUERY 4: Sazonalidade — contratações por mês/trimestre
--
-- ============================================================
SELECT
    EXTRACT(YEAR  FROM data_assinatura)::INT   AS ano,
    EXTRACT(MONTH FROM data_assinatura)::INT   AS mes,
    TO_CHAR(data_assinatura, 'TMMonth')        AS nome_mes,  -- localizado
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


-- ============================================================
-- QUERY 5: Contratos ativos e compromisso financeiro futuro
--
-
-- ============================================================
SELECT
    EXTRACT(YEAR  FROM data_assinatura)::INT   AS ano,
    EXTRACT(MONTH FROM data_assinatura)::INT   AS mes,
    TO_CHAR(data_assinatura, 'TMMonth')        AS nome_mes,
    EXTRACT(QUARTER FROM data_assinatura)::INT AS trimestre,
    COUNT(*)                                   AS qtd_contratos,
    ROUND(SUM(valor_global) / 1e6, 2)          AS valor_milhoes,
    ROUND(AVG(valor_global), 2)                AS valor_medio,
    ROUND(
        SUM(valor_global) * 100.0 
        / NULLIF(SUM(SUM(valor_global)) OVER (PARTITION BY EXTRACT(YEAR FROM data_assinatura)), 0), 
        2
    ) AS pct_no_ano
FROM fato_contratos
WHERE valor_global > 0 
  AND data_assinatura IS NOT NULL
  AND EXTRACT(YEAR FROM data_assinatura) BETWEEN 2021 AND 2026
GROUP BY 
    EXTRACT(YEAR FROM data_assinatura), 
    EXTRACT(MONTH FROM data_assinatura), 
    TO_CHAR(data_assinatura, 'TMMonth'),
    EXTRACT(QUARTER FROM data_assinatura)
ORDER BY ano, mes;

-- ============================================================
-- QUERY 6: Contratos de Universidades
-- Busca órgãos cujo nome contém termos relacionados a
-- universidades, institutos federais e centros universitários,
-- ordenados por valor_global decrescente.
-- ============================================================
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
    OR o.nome_orgao ILIKE '%UFMG%'
    OR o.nome_orgao ILIKE '%UFRJ%'
    OR o.nome_orgao ILIKE '%USP%'
    OR o.nome_orgao ILIKE '%UNICAMP%'
    OR o.nome_orgao ILIKE '%UNESP%'
    OR o.nome_orgao ILIKE '%UFSC%'
    OR o.nome_orgao ILIKE '%UNIFESP%'
    OR o.nome_orgao ILIKE '%instituto federal%'
    OR o.nome_orgao ILIKE '%IF federal%'
    OR o.nome_orgao ILIKE '%centro federal%'
    OR o.nome_orgao ILIKE '%CEFET%'
    OR o.nome_orgao ILIKE '%centro universitário%'
    OR o.nome_unidade ILIKE '%universidade%'
    OR o.nome_unidade ILIKE '%instituto federal%'
  )
ORDER BY f.valor_global DESC;


-- ============================================================
-- QUERY 6b: Resumo por universidade (totais agregados)
-- ============================================================
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
    OR o.nome_unidade ILIKE '%universidade%'
    OR o.nome_unidade ILIKE '%instituto federal%'
  )
GROUP BY o.nome_orgao
ORDER BY valor_total_milhoes DESC;


-- ============================================================
-- QUERY 7: Top 100 maiores contratos da USP
-- ============================================================
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
    ROUND(f.valor_global,  2)                          AS valor_global,
    ROUND(f.valor_inicial, 2)                          AS valor_inicial,
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
