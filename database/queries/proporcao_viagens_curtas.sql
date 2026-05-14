-- =============================================================================
-- Proporção de viagens curtas (≤5 minutos) que NÃO envolvem garagem
-- Útil para avaliar qualidade da detecção de sentido
-- =============================================================================

-- Viagens curtas sem garagem indicam possíveis flip-flops na detecção de sentido
-- (mudanças rápidas entre IDA/VOLTA que não correspondem a viagens reais)

WITH viagens_completas AS (
    SELECT
        id,
        ordem,
        linha,
        nome_terminal_origem,
        nome_terminal_destino,
        metodo_inferencia_origem,
        metodo_inferencia_destino,
        timestamp_inicio,
        timestamp_fim,
        duracao_viagem,
        EXTRACT(EPOCH FROM duracao_viagem) / 60.0 AS duracao_minutos
    FROM gps_historico_viagens
    WHERE timestamp_fim IS NOT NULL
      AND duracao_viagem IS NOT NULL
      -- Filtro para período equivalente semana passada (Terça 05/05 a Quarta 06/05)
      AND (
        (timestamp_inicio >= '2026-05-05 17:42:47'::timestamp AND timestamp_inicio <= '2026-05-06 11:59:49'::timestamp)
        OR
        -- Mantém período atual para comparação (comentar se quiser só semana passada)
        (timestamp_inicio >= '2026-05-12 17:42:47'::timestamp AND timestamp_inicio <= '2026-05-13 11:59:49'::timestamp)
      )
),

estatisticas AS (
    SELECT
        COUNT(*) AS total_viagens,
        COUNT(*) FILTER (WHERE duracao_minutos <= 5) AS viagens_curtas_total,
        COUNT(*) FILTER (
            WHERE duracao_minutos <= 5
              AND nome_terminal_origem NOT ILIKE '%garagem%'
              AND nome_terminal_destino NOT ILIKE '%garagem%'
              AND metodo_inferencia_origem != 'garagem_por_distancia'
              AND metodo_inferencia_destino != 'garagem_por_distancia'
        ) AS viagens_curtas_sem_garagem,
        MIN(timestamp_inicio) AS data_minima,
        MAX(timestamp_fim) AS data_maxima
    FROM viagens_completas
)

SELECT
    total_viagens,
    viagens_curtas_total,
    viagens_curtas_sem_garagem,
    data_minima,
    data_maxima,
    ROUND(100.0 * viagens_curtas_total / NULLIF(total_viagens, 0), 2) AS pct_viagens_curtas,
    ROUND(100.0 * viagens_curtas_sem_garagem / NULLIF(total_viagens, 0), 2) AS pct_curtas_sem_garagem,
    ROUND(100.0 * viagens_curtas_sem_garagem / NULLIF(viagens_curtas_total, 0), 2) AS pct_curtas_que_nao_sao_garagem
FROM estatisticas;


-- =============================================================================
-- Detalhamento por linha (opcional)
-- =============================================================================

-- SELECT
--     linha,
--     COUNT(*) AS total_viagens,
--     COUNT(*) FILTER (WHERE duracao_minutos <= 5) AS viagens_curtas,
--     COUNT(*) FILTER (
--         WHERE duracao_minutos <= 5
--           AND nome_terminal_origem NOT ILIKE '%garagem%'
--           AND nome_terminal_destino NOT ILIKE '%garagem%'
--           AND metodo_inferencia_origem != 'garagem_por_distancia'
--           AND metodo_inferencia_destino != 'garagem_por_distancia'
--     ) AS curtas_sem_garagem,
--     ROUND(100.0 * COUNT(*) FILTER (
--         WHERE duracao_minutos <= 5
--           AND nome_terminal_origem NOT ILIKE '%garagem%'
--           AND nome_terminal_destino NOT ILIKE '%garagem%'
--     ) / NULLIF(COUNT(*), 0), 2) AS pct_curtas_sem_garagem
-- FROM viagens_completas
-- GROUP BY linha
-- ORDER BY pct_curtas_sem_garagem DESC;
