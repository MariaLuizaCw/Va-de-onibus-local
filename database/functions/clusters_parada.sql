-- =============================================================================
-- FUNÇÕES PARA ANÁLISE DE CLUSTERS DE PARADA
-- Identifica terminais, garagens e pontos de parada a partir de dados GPS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fn_analisar_clusters_linha
-- Analisa clusters de parada para uma linha específica usando DBSCAN
-- Retorna clusters classificados como Terminal, Garagem ou Indefinido
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_analisar_clusters_linha(
    p_linha_numero text,
    p_dbscan_eps_metros integer DEFAULT 50,
    p_dbscan_minpoints integer DEFAULT 5,
    p_duracao_minima_segundos integer DEFAULT 480,
    p_min_paradas_cluster integer DEFAULT 20,
    p_duracao_garagem_minutos numeric DEFAULT 30
)
RETURNS TABLE (
    cluster_id integer,
    num_paradas bigint,
    primeira_parada timestamp with time zone,
    ultima_parada timestamp with time zone,
    tempo_total_parado_minutos numeric,
    mediana_duracao_minutos numeric,
    lat_cluster numeric,
    lon_cluster numeric,
    max_distance_metros numeric,
    hora_mediana_cluster integer,
    tipo_cluster text,
    sentido text,
    itinerario_id integer,
    geom_cluster geography,
    linha_analisada text,
    dbscan_eps_metros_usado integer,
    dbscan_minpoints_usado integer,
    duracao_minima_segundos_usado integer,
    min_paradas_cluster_usado integer,
    duracao_garagem_minutos_usado numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH base AS (
        SELECT DISTINCT ON (r.ordem, (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp with time zone)
            r.ordem,
            r.linha,
            REPLACE(r.latitude, ',', '.')::float AS lat,
            REPLACE(r.longitude, ',', '.')::float AS lon,
            (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp with time zone AS ts
        FROM rio_gps_api_history r
        WHERE r.linha = p_linha_numero
        ORDER BY r.ordem, (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp with time zone, r.datahora DESC
    ),

    ordenado AS (
        SELECT
            b.*,
            LAG(b.lat) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS lat_prev,
            LAG(b.lon) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS lon_prev,
            LAG(b.ts)  OVER (PARTITION BY b.ordem ORDER BY b.ts) AS ts_prev
        FROM base b
    ),

    parado_flag AS (
        SELECT
            o.*,
            ST_Distance(
                ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
            ) AS dist_metros,
            EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) AS delta_t,
            CASE 
                WHEN EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) = 0
                THEN NULL
                ELSE ST_Distance(
                        ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
                     ) / EXTRACT(EPOCH FROM (o.ts - o.ts_prev))
            END AS velocidade_ms,
            CASE 
                WHEN EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) = 0
                THEN 0
                WHEN (ST_Distance(
                        ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
                     ) / EXTRACT(EPOCH FROM (o.ts - o.ts_prev))) < 0.556
                THEN 1 ELSE 0
            END AS parado,
            LAG(
                CASE 
                    WHEN EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) = 0
                    THEN 0
                    WHEN (ST_Distance(
                            ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                            ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
                         ) / EXTRACT(EPOCH FROM (o.ts - o.ts_prev))) < 0.556
                    THEN 1 ELSE 0
                END
            ) OVER (PARTITION BY o.ordem ORDER BY o.ts) AS parado_prev
        FROM ordenado o
    ),

    grupos AS (
        SELECT
            pf.*,
            SUM(
                CASE 
                    WHEN pf.parado = 1 AND pf.parado_prev = 1
                    THEN 0 ELSE 1
                END
            ) OVER (PARTITION BY pf.ordem ORDER BY pf.ts) AS grupo
        FROM parado_flag pf
    ),

    duracao AS (
        SELECT
            g.ordem,
            g.grupo,
            MIN(g.ts) AS inicio,
            MAX(g.ts) AS fim,
            EXTRACT(EPOCH FROM (MAX(g.ts) - MIN(g.ts))) AS duracao_segundos,
            AVG(g.lat) AS lat,
            AVG(g.lon) AS lon,
            COUNT(*) AS num_pontos
        FROM grupos g
        WHERE g.parado = 1
        GROUP BY g.ordem, g.grupo
    ),

    paradas_validas AS (
        SELECT
            d.ordem,
            d.grupo,
            d.inicio,
            d.fim,
            d.duracao_segundos,
            d.num_pontos,
            d.lat,
            d.lon,
            ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326)::geography AS geom
        FROM duracao d
        WHERE d.duracao_segundos >= p_duracao_minima_segundos
    ),

    clusters AS (
        SELECT
            pv.*,
            ST_ClusterDBSCAN(
                ST_Transform(pv.geom::geometry, 31983),
                eps := p_dbscan_eps_metros,
                minpoints := p_dbscan_minpoints
            ) OVER () AS cid
        FROM paradas_validas pv
    ),

    horas_cluster AS (
        SELECT
            cl.cid,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM cl.inicio)) AS hora_mediana
        FROM clusters cl
        WHERE cl.cid IS NOT NULL
        GROUP BY cl.cid
    ),

    duracao_cluster AS (
        SELECT
            cl.cid,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cl.duracao_segundos) AS mediana_duracao_segundos
        FROM clusters cl
        WHERE cl.cid IS NOT NULL
        GROUP BY cl.cid
    ),

    resultado_temp AS (
        SELECT
            c.cid,
            COUNT(*) AS num_paradas,
            MIN(c.inicio) AS primeira_parada,
            MAX(c.fim) AS ultima_parada,
            SUM(c.duracao_segundos) AS tempo_total_parado_segundos,
            ROUND(SUM(c.duracao_segundos)::numeric / 60, 1) AS tempo_total_parado_minutos,
            ROUND(dc.mediana_duracao_segundos::numeric / 60, 1) AS mediana_duracao_minutos,
            ROUND(AVG(c.lat)::numeric, 6) AS lat_cluster,
            ROUND(AVG(c.lon)::numeric, 6) AS lon_cluster,
            ROUND(
                ST_MaxDistance(
                    ST_Transform(ST_Centroid(ST_Collect(c.geom::geometry)), 31983),
                    ST_Transform(ST_Collect(c.geom::geometry), 31983)
                )::numeric,
                2
            ) AS max_distance_metros,
            ROUND(hc.hora_mediana::numeric, 0)::int AS hora_mediana_cluster,
            ROUND(dc.mediana_duracao_segundos::numeric / 60, 1) AS mediana_duracao_minutos_temp,
            ST_Collect(c.geom::geometry) AS geom_coletado
        FROM clusters c
        LEFT JOIN horas_cluster hc ON c.cid = hc.cid
        LEFT JOIN duracao_cluster dc ON c.cid = dc.cid
        WHERE c.cid IS NOT NULL
        GROUP BY c.cid, hc.hora_mediana, dc.mediana_duracao_segundos
    ),

    classificacao_inicial AS (
        SELECT
            rt.*,
            CASE 
                WHEN rt.num_paradas < p_min_paradas_cluster THEN 'Indefinido'
                WHEN rt.mediana_duracao_minutos_temp > p_duracao_garagem_minutos THEN 'Garagem'
                ELSE 'Potencial_Terminal'
            END AS tipo_temp
        FROM resultado_temp rt
    ),

    clusters_com_buffer AS (
        SELECT
            ci.cid,
            ST_Buffer(
                ST_Centroid(ci.geom_coletado), 
                ST_MaxDistance(
                    ST_Centroid(ci.geom_coletado), 
                    ci.geom_coletado
                )
            )::geography AS geom_cluster
        FROM classificacao_inicial ci
    ),

    itinerarios_linha AS (
        SELECT
            ST_SetSRID(ST_StartPoint(i.the_geom), 4326) AS ponto_inicio,
            i.numero_linha,
            i.sentido,
            i.id AS itinerario_id
        FROM public.itinerario i
        WHERE i.numero_linha = p_linha_numero
            AND i.habilitado = true
    ),

    clusters_validados AS (
        SELECT DISTINCT ON (cb.cid)
            cb.cid,
            TRUE AS eh_terminal_validado,
            il.sentido,
            il.itinerario_id
        FROM clusters_com_buffer cb
        INNER JOIN itinerarios_linha il ON 
            ST_DWithin(cb.geom_cluster, il.ponto_inicio::geography, 50)
        ORDER BY cb.cid, il.itinerario_id
    )

    SELECT
        ci.cid AS cluster_id,
        ci.num_paradas,
        ci.primeira_parada,
        ci.ultima_parada,
        ci.tempo_total_parado_minutos,
        ci.mediana_duracao_minutos,
        ci.lat_cluster,
        ci.lon_cluster,
        ci.max_distance_metros,
        ci.hora_mediana_cluster,
        CASE 
            WHEN ci.tipo_temp = 'Garagem' THEN 'Garagem'
            WHEN ci.tipo_temp = 'Potencial_Terminal' 
                AND cv.eh_terminal_validado = true THEN 'Terminal'
            ELSE 'Indefinido'
        END AS tipo_cluster,
        cv.sentido,
        cv.itinerario_id,
        ccb.geom_cluster,
        p_linha_numero AS linha_analisada,
        p_dbscan_eps_metros AS dbscan_eps_metros_usado,
        p_dbscan_minpoints AS dbscan_minpoints_usado,
        p_duracao_minima_segundos AS duracao_minima_segundos_usado,
        p_min_paradas_cluster AS min_paradas_cluster_usado,
        p_duracao_garagem_minutos AS duracao_garagem_minutos_usado
    FROM classificacao_inicial ci
    LEFT JOIN clusters_com_buffer ccb ON ci.cid = ccb.cid
    LEFT JOIN clusters_validados cv ON ci.cid = cv.cid
    ORDER BY ci.tempo_total_parado_segundos DESC;
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_analisar_clusters_todas_linhas
-- Executa a análise de clusters para todas as linhas distintas da tabela
-- rio_gps_api_history, imprimindo o progresso via RAISE NOTICE
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_analisar_clusters_todas_linhas(
    p_dbscan_eps_metros integer DEFAULT 50,
    p_dbscan_minpoints integer DEFAULT 5,
    p_duracao_minima_segundos integer DEFAULT 480,
    p_min_paradas_cluster integer DEFAULT 20,
    p_duracao_garagem_minutos numeric DEFAULT 30
)
RETURNS TABLE (
    cluster_id integer,
    num_paradas bigint,
    primeira_parada timestamp with time zone,
    ultima_parada timestamp with time zone,
    tempo_total_parado_minutos numeric,
    mediana_duracao_minutos numeric,
    lat_cluster numeric,
    lon_cluster numeric,
    max_distance_metros numeric,
    hora_mediana_cluster integer,
    tipo_cluster text,
    sentido text,
    itinerario_id integer,
    geom_cluster geography,
    linha_analisada text,
    dbscan_eps_metros_usado integer,
    dbscan_minpoints_usado integer,
    duracao_minima_segundos_usado integer,
    min_paradas_cluster_usado integer,
    duracao_garagem_minutos_usado numeric
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_linha text;
    v_total_linhas integer;
    v_contador integer := 0;
    v_clusters_encontrados integer;
    v_start_time timestamp;
    v_linha_start timestamp;
    v_rec record;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Contar total de linhas distintas
    SELECT COUNT(DISTINCT linha) INTO v_total_linhas
    FROM rio_gps_api_history
    WHERE linha IS NOT NULL AND linha != '';
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'INICIANDO ANÁLISE DE CLUSTERS PARA % LINHAS', v_total_linhas;
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Parâmetros: eps=%m, minpoints=%, duracao_min=%s, min_paradas=%, garagem=%min',
        p_dbscan_eps_metros, p_dbscan_minpoints, p_duracao_minima_segundos, 
        p_min_paradas_cluster, p_duracao_garagem_minutos;
    RAISE NOTICE '------------------------------------------------------------';
    
    -- Iterar sobre cada linha distinta
    FOR v_linha IN 
        SELECT DISTINCT rh.linha 
        FROM rio_gps_api_history rh
        WHERE rh.linha IS NOT NULL AND rh.linha != ''
        ORDER BY rh.linha
    LOOP
        v_contador := v_contador + 1;
        v_linha_start := clock_timestamp();
        v_clusters_encontrados := 0;
        
        RAISE NOTICE '[%/%] Processando linha %...', v_contador, v_total_linhas, v_linha;
        
        -- Executar análise para esta linha, contar e retornar resultados
        FOR v_rec IN 
            SELECT * FROM fn_analisar_clusters_linha(
                v_linha,
                p_dbscan_eps_metros,
                p_dbscan_minpoints,
                p_duracao_minima_segundos,
                p_min_paradas_cluster,
                p_duracao_garagem_minutos
            )
        LOOP
            v_clusters_encontrados := v_clusters_encontrados + 1;
            
            -- Retornar cada registro
            cluster_id := v_rec.cluster_id;
            num_paradas := v_rec.num_paradas;
            primeira_parada := v_rec.primeira_parada;
            ultima_parada := v_rec.ultima_parada;
            tempo_total_parado_minutos := v_rec.tempo_total_parado_minutos;
            mediana_duracao_minutos := v_rec.mediana_duracao_minutos;
            lat_cluster := v_rec.lat_cluster;
            lon_cluster := v_rec.lon_cluster;
            max_distance_metros := v_rec.max_distance_metros;
            hora_mediana_cluster := v_rec.hora_mediana_cluster;
            tipo_cluster := v_rec.tipo_cluster;
            sentido := v_rec.sentido;
            itinerario_id := v_rec.itinerario_id;
            geom_cluster := v_rec.geom_cluster;
            linha_analisada := v_rec.linha_analisada;
            dbscan_eps_metros_usado := v_rec.dbscan_eps_metros_usado;
            dbscan_minpoints_usado := v_rec.dbscan_minpoints_usado;
            duracao_minima_segundos_usado := v_rec.duracao_minima_segundos_usado;
            min_paradas_cluster_usado := v_rec.min_paradas_cluster_usado;
            duracao_garagem_minutos_usado := v_rec.duracao_garagem_minutos_usado;
            RETURN NEXT;
        END LOOP;
        
        RAISE NOTICE '    -> % clusters encontrados (%.2fs)', 
            v_clusters_encontrados, 
            EXTRACT(EPOCH FROM (clock_timestamp() - v_linha_start));
    END LOOP;
    
    RAISE NOTICE '------------------------------------------------------------';
    RAISE NOTICE 'ANÁLISE CONCLUÍDA em %.2f segundos', 
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    RAISE NOTICE '============================================================';
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_salvar_clusters_todas_linhas
-- Executa análise de clusters e salva resultados na tabela clusters_parada_resultado
-- Imprime progresso via RAISE NOTICE e retorna estatísticas da execução
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_salvar_clusters_todas_linhas(
    p_dbscan_eps_metros integer DEFAULT 50,
    p_dbscan_minpoints integer DEFAULT 5,
    p_duracao_minima_segundos integer DEFAULT 480,
    p_min_paradas_cluster integer DEFAULT 20,
    p_duracao_garagem_minutos numeric DEFAULT 30,
    p_limpar_anteriores boolean DEFAULT true
)
RETURNS TABLE (
    linhas_processadas integer,
    total_clusters integer,
    clusters_terminal integer,
    clusters_garagem integer,
    clusters_indefinido integer,
    tempo_execucao_segundos numeric
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_linha text;
    v_total_linhas integer;
    v_contador integer := 0;
    v_clusters_encontrados integer;
    v_start_time timestamp;
    v_linha_start timestamp;
    v_rec record;
    v_total_clusters integer := 0;
    v_total_terminal integer := 0;
    v_total_garagem integer := 0;
    v_total_indefinido integer := 0;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Limpar análises anteriores se solicitado
    IF p_limpar_anteriores THEN
        DELETE FROM clusters_parada_resultado;
        RAISE NOTICE 'Tabela clusters_parada_resultado limpa para nova análise';
    END IF;
    
    -- Contar total de linhas distintas
    SELECT COUNT(DISTINCT linha) INTO v_total_linhas
    FROM rio_gps_api_history
    WHERE linha IS NOT NULL AND linha != '';
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'INICIANDO ANÁLISE E SALVAMENTO DE CLUSTERS PARA % LINHAS', v_total_linhas;
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Parâmetros: eps=%m, minpoints=%, duracao_min=%s, min_paradas=%, garagem=%min',
        p_dbscan_eps_metros, p_dbscan_minpoints, p_duracao_minima_segundos, 
        p_min_paradas_cluster, p_duracao_garagem_minutos;
    RAISE NOTICE '------------------------------------------------------------';
    
    -- Iterar sobre cada linha distinta
    FOR v_linha IN 
        SELECT DISTINCT rh.linha 
        FROM rio_gps_api_history rh
        WHERE rh.linha IS NOT NULL AND rh.linha != ''
        ORDER BY rh.linha
    LOOP
        v_contador := v_contador + 1;
        v_linha_start := clock_timestamp();
        v_clusters_encontrados := 0;
        
        RAISE NOTICE '[%/%] Processando linha %...', v_contador, v_total_linhas, v_linha;
        
        -- Executar análise para esta linha, contar e salvar resultados
        FOR v_rec IN 
            SELECT * FROM fn_analisar_clusters_linha(
                v_linha,
                p_dbscan_eps_metros,
                p_dbscan_minpoints,
                p_duracao_minima_segundos,
                p_min_paradas_cluster,
                p_duracao_garagem_minutos
            )
        LOOP
            v_clusters_encontrados := v_clusters_encontrados + 1;
            v_total_clusters := v_total_clusters + 1;
            
            -- Contar por tipo
            CASE v_rec.tipo_cluster
                WHEN 'Terminal' THEN v_total_terminal := v_total_terminal + 1;
                WHEN 'Garagem' THEN v_total_garagem := v_total_garagem + 1;
                ELSE v_total_indefinido := v_total_indefinido + 1;
            END CASE;
            
            -- Inserir na tabela de resultados
            INSERT INTO clusters_parada_resultado (
                cluster_id,
                linha_analisada,
                num_paradas,
                primeira_parada,
                ultima_parada,
                tempo_total_parado_minutos,
                mediana_duracao_minutos,
                lat_cluster,
                lon_cluster,
                max_distance_metros,
                hora_mediana_cluster,
                tipo_cluster,
                sentido,
                itinerario_id,
                geom_cluster,
                dbscan_eps_metros_usado,
                dbscan_minpoints_usado,
                duracao_minima_segundos_usado,
                min_paradas_cluster_usado,
                duracao_garagem_minutos_usado
            ) VALUES (
                v_rec.cluster_id,
                v_rec.linha_analisada,
                v_rec.num_paradas,
                v_rec.primeira_parada,
                v_rec.ultima_parada,
                v_rec.tempo_total_parado_minutos,
                v_rec.mediana_duracao_minutos,
                v_rec.lat_cluster,
                v_rec.lon_cluster,
                v_rec.max_distance_metros,
                v_rec.hora_mediana_cluster,
                v_rec.tipo_cluster,
                v_rec.sentido,
                v_rec.itinerario_id,
                v_rec.geom_cluster,
                v_rec.dbscan_eps_metros_usado,
                v_rec.dbscan_minpoints_usado,
                v_rec.duracao_minima_segundos_usado,
                v_rec.min_paradas_cluster_usado,
                v_rec.duracao_garagem_minutos_usado
            );
        END LOOP;
        
        RAISE NOTICE '    -> % clusters encontrados (%.2fs)', 
            v_clusters_encontrados, 
            EXTRACT(EPOCH FROM (clock_timestamp() - v_linha_start));
    END LOOP;
    
    RAISE NOTICE '------------------------------------------------------------';
    RAISE NOTICE 'ANÁLISE CONCLUÍDA em %.2f segundos', 
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    RAISE NOTICE 'RESUMO: % clusters total (% Terminal, % Garagem, % Indefinido)', 
        v_total_clusters, v_total_terminal, v_total_garagem, v_total_indefinido;
    RAISE NOTICE '============================================================';
    
    -- Retornar estatísticas
    linhas_processadas := v_total_linhas;
    total_clusters := v_total_clusters;
    clusters_terminal := v_total_terminal;
    clusters_garagem := v_total_garagem;
    clusters_indefinido := v_total_indefinido;
    tempo_execucao_segundos := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    
    RETURN NEXT;
END;
$$;


-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
