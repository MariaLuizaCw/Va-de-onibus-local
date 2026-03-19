-- ============================================================================
-- SCRIPT DE EXECUÇÃO
-- Este script executa a função analyze_bus_clusters para todas as linhas
-- e armazena os resultados em uma tabela permanente
-- ============================================================================

-- 1. CRIAR TABELA PARA ARMAZENAR OS RESULTADOS
-- ============================================================================

DROP TABLE IF EXISTS analise_clusters_onibus CASCADE;

CREATE TABLE analise_clusters_onibus (
    id SERIAL PRIMARY KEY,
    linha VARCHAR NOT NULL,
    cluster_id INT,
    num_paradas BIGINT,
    primeira_parada TIMESTAMP,
    ultima_parada TIMESTAMP,
    tempo_total_parado_segundos NUMERIC,
    tempo_total_parado_minutos NUMERIC,
    media_duracao_minutos NUMERIC,
    lat_cluster NUMERIC,
    lon_cluster NUMERIC,
    max_distance_metros NUMERIC,
    hora_mediana_cluster INT,
    tipo_cluster VARCHAR,
    data_analise TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(linha, cluster_id)
);

-- Criar índices para melhor performance
CREATE INDEX idx_analise_clusters_linha ON analise_clusters_onibus(linha);
CREATE INDEX idx_analise_clusters_tipo ON analise_clusters_onibus(tipo_cluster);
CREATE INDEX idx_analise_clusters_data ON analise_clusters_onibus(data_analise);


-- 2. OBTER LISTA DE TODAS AS LINHAS ÚNICAS
-- ============================================================================

-- Verificar quais linhas existem na base
-- SELECT DISTINCT linha FROM rio_gps_api_history ORDER BY linha;


-- 3. EXECUTAR A FUNÇÃO PARA TODAS AS LINHAS
-- ============================================================================

-- Opção A: Usando uma função procedural para automatizar
CREATE OR REPLACE FUNCTION populate_analise_clusters_onibus()
RETURNS TABLE(mensagem TEXT, linhas_processadas INT, total_clusters INT) AS $$
DECLARE
    v_linha VARCHAR;
    v_cursor REFCURSOR;
    v_linhas_count INT := 0;
    v_clusters_count INT := 0;
BEGIN
    -- Limpar dados antigos
    DELETE FROM analise_clusters_onibus;
    
    -- Obter todas as linhas únicas
    FOR v_linha IN 
        SELECT DISTINCT linha 
        FROM rio_gps_api_history 
        WHERE linha IS NOT NULL
        ORDER BY linha
    LOOP
        BEGIN
            -- Inserir resultados da análise para cada linha
            INSERT INTO analise_clusters_onibus (
                linha,
                cluster_id,
                num_paradas,
                primeira_parada,
                ultima_parada,
                tempo_total_parado_segundos,
                tempo_total_parado_minutos,
                media_duracao_minutos,
                lat_cluster,
                lon_cluster,
                max_distance_metros,
                hora_mediana_cluster,
                tipo_cluster
            )
            SELECT
                linha,
                cluster_id,
                num_paradas,
                primeira_parada,
                ultima_parada,
                tempo_total_parado_segundos,
                tempo_total_parado_minutos,
                media_duracao_minutos,
                lat_cluster,
                lon_cluster,
                max_distance_metros,
                hora_mediana_cluster,
                tipo_cluster
            FROM analyze_bus_clusters(v_linha);
            
            v_linhas_count := v_linhas_count + 1;
            
            RAISE NOTICE 'Processada linha: %', v_linha;
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Erro ao processar linha %: %', v_linha, SQLERRM;
        END;
    END LOOP;
    
    -- Contar total de clusters inseridos
    SELECT COUNT(*) INTO v_clusters_count FROM analise_clusters_onibus;
    
    RETURN QUERY SELECT 
        'Análise de clusters concluída com sucesso'::TEXT,
        v_linhas_count,
        v_clusters_count;
END;
$$ LANGUAGE plpgsql;


-- 4. EXECUTAR A POPULAÇÃO DA TABELA
-- ============================================================================

-- Descomentar para executar:
-- SELECT * FROM populate_analise_clusters_onibus();


-- 5. CONSULTAS ÚTEIS PARA ANALISAR OS RESULTADOS
-- ============================================================================

-- Ver resumo geral
-- SELECT
--     tipo_cluster,
--     COUNT(*) AS qtd_clusters,
--     COUNT(DISTINCT linha) AS qtd_linhas,
--     ROUND(AVG(media_duracao_minutos)::numeric, 1) AS media_duracao_avg,
--     ROUND(AVG(num_paradas)::numeric, 0) AS media_paradas_avg,
--     MIN(primeira_parada) AS primeira_parada_geral,
--     MAX(ultima_parada) AS ultima_parada_geral
-- FROM analise_clusters_onibus
-- GROUP BY tipo_cluster
-- ORDER BY qtd_clusters DESC;


-- Ver clusters por linha
-- SELECT
--     linha,
--     tipo_cluster,
--     COUNT(*) AS qtd_clusters,
--     ROUND(SUM(tempo_total_parado_minutos)::numeric, 0) AS tempo_total_minutos,
--     ROUND(AVG(media_duracao_minutos)::numeric, 1) AS media_duracao,
--     ROUND(AVG(num_paradas)::numeric, 0) AS media_paradas
-- FROM analise_clusters_onibus
-- GROUP BY linha, tipo_cluster
-- ORDER BY linha, tipo_cluster;


-- Ver todas as garagens detectadas
-- SELECT
--     linha,
--     cluster_id,
--     num_paradas,
--     media_duracao_minutos,
--     hora_mediana_cluster,
--     lat_cluster,
--     lon_cluster,
--     primeira_parada,
--     ultima_parada
-- FROM analise_clusters_onibus
-- WHERE tipo_cluster = 'Garagem'
-- ORDER BY linha, primeira_parada;


-- Ver todos os terminais detectados
-- SELECT
--     linha,
--     cluster_id,
--     num_paradas,
--     media_duracao_minutos,
--     hora_mediana_cluster,
--     lat_cluster,
--     lon_cluster
-- FROM analise_clusters_onibus
-- WHERE tipo_cluster = 'Terminal'
-- ORDER BY linha, media_duracao_minutos DESC;