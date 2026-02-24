-- Functions para GTFS-RT
-- Inspirado em angra.sql e rio.sql

-- Function para identificar sentido de uma linha GTFS-RT
CREATE OR REPLACE FUNCTION fn_identificar_sentido_gtfs(
    p_numero_linha TEXT,
    p_start_lon NUMERIC,
    p_start_lat NUMERIC,
    p_end_lon NUMERIC,
    p_end_lat NUMERIC,
    p_max_distance NUMERIC DEFAULT 300
)
RETURNS TABLE (
    itinerario_id BIGINT,
    sentido TEXT,
    route_name TEXT,
    dist_start DOUBLE PRECISION,
    dist_end DOUBLE PRECISION,
    dist_total DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        i.id::BIGINT,
        i.sentido::TEXT,
        i.route_name::TEXT as route_name,
        ST_Distance(
            ST_StartPoint(i.the_geom)::geography,
            ST_SetSRID(ST_MakePoint(p_start_lon, p_start_lat), 4326)::geography
        ) as dist_start,
        ST_Distance(
            ST_EndPoint(i.the_geom)::geography,
            ST_SetSRID(ST_MakePoint(p_end_lon, p_end_lat), 4326)::geography
        ) as dist_end,
        ST_Distance(
            ST_StartPoint(i.the_geom)::geography,
            ST_SetSRID(ST_MakePoint(p_start_lon, p_start_lat), 4326)::geography
        ) + ST_Distance(
            ST_EndPoint(i.the_geom)::geography,
            ST_SetSRID(ST_MakePoint(p_end_lon, p_end_lat), 4326)::geography
        ) as dist_total
    FROM itinerario i
    WHERE i.numero_linha = SPLIT_PART(p_numero_linha, '.', 1)
    AND (
        ST_Distance(
            ST_StartPoint(i.the_geom)::geography,
            ST_SetSRID(ST_MakePoint(p_start_lon, p_start_lat), 4326)::geography
        ) <= p_max_distance
        OR ST_Distance(
            ST_EndPoint(i.the_geom)::geography,
            ST_SetSRID(ST_MakePoint(p_end_lon, p_end_lat), 4326)::geography
        ) <= p_max_distance
    )
    ORDER BY dist_total
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function para salvar múltiplos registros GTFS-RT em batch
CREATE OR REPLACE FUNCTION fn_save_gtfs_gps_batch(
    p_records JSONB
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
    v_record JSONB;
    v_token TEXT;
    v_linha TEXT;
    v_ordem TEXT;
    v_latitude NUMERIC;
    v_longitude NUMERIC;
    v_datahora TIMESTAMP;
    v_sentido TEXT;
    v_sentido_itinerario_id INTEGER;
    v_route_name TEXT;
    v_velocidade DOUBLE PRECISION;
BEGIN
    -- Itera sobre os registros no JSON
    FOR v_record IN SELECT value FROM jsonb_array_elements(p_records) AS value
    LOOP
        -- Extrai dados do registro
        v_token := COALESCE((v_record->>'token'), 'GTFS');
        v_linha := v_record->>'linha';
        v_ordem := v_record->>'ordem';
        v_latitude := (v_record->>'latitude')::NUMERIC;
        v_longitude := (v_record->>'longitude')::NUMERIC;
        v_datahora := (v_record->>'datahora')::TIMESTAMP;
        v_sentido := v_record->>'sentido';
        v_sentido_itinerario_id := CASE WHEN (v_record->>'sentido_itinerario_id') IS NOT NULL THEN (v_record->>'sentido_itinerario_id')::INTEGER ELSE NULL END;
        v_route_name := v_record->>'route_name';
        v_velocidade := CASE WHEN (v_record->>'velocidade') IS NOT NULL THEN (v_record->>'velocidade')::DOUBLE PRECISION ELSE NULL END;
        
        -- Insere ou atualiza registro
        INSERT INTO gps_sentido (
            token, 
            linha, 
            ordem, 
            latitude, 
            longitude, 
            datahora, 
            sentido, 
            sentido_itinerario_id,
            route_name,
            velocidade
        ) VALUES (
            v_token,
            v_linha,
            v_ordem,
            v_latitude,
            v_longitude,
            v_datahora,
            v_sentido,
            v_sentido_itinerario_id,
            v_route_name,
            v_velocidade
        )
        ON CONFLICT (ordem, token) 
        DO UPDATE SET 
            datahora = EXCLUDED.datahora,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            linha = EXCLUDED.linha,
            sentido = EXCLUDED.sentido,
            sentido_itinerario_id = EXCLUDED.sentido_itinerario_id,
            route_name = EXCLUDED.route_name,
            velocidade = EXCLUDED.velocidade;
        
        v_count := v_count + 1;
    END LOOP;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function para identificar sentido em batch usando JOINs (sem loop)
-- Recebe array de registros com coordenadas do shape já incluídas
CREATE OR REPLACE FUNCTION fn_identificar_sentido_gtfs_batch(
    p_records JSONB,
    p_max_distance NUMERIC DEFAULT 300
)
RETURNS TABLE (
    record_index INTEGER,
    itinerario_id BIGINT,
    sentido TEXT,
    route_name TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH records AS (
        -- Expande o JSON em linhas com índice
        SELECT 
            (row_number() OVER ())::INTEGER as idx,
            SPLIT_PART((r->>'numeroLinha')::TEXT, '.', 1) as numero_linha,
            (r->'startCoord'->>'lon')::NUMERIC as start_lon,
            (r->'startCoord'->>'lat')::NUMERIC as start_lat,
            (r->'endCoord'->>'lon')::NUMERIC as end_lon,
            (r->'endCoord'->>'lat')::NUMERIC as end_lat
        FROM jsonb_array_elements(p_records) AS r
    ),
    matched AS (
        -- JOIN com itinerario e calcula distâncias
        SELECT 
            rec.idx,
            i.id as itin_id,
            i.sentido as itin_sentido,
            i.route_name as itin_route_name,
            ST_Distance(
                ST_StartPoint(i.the_geom)::geography,
                ST_SetSRID(ST_MakePoint(rec.start_lon, rec.start_lat), 4326)::geography
            ) + ST_Distance(
                ST_EndPoint(i.the_geom)::geography,
                ST_SetSRID(ST_MakePoint(rec.end_lon, rec.end_lat), 4326)::geography
            ) as dist_total
        FROM records rec
        JOIN itinerario i ON i.numero_linha = rec.numero_linha
        WHERE 
            rec.numero_linha IS NOT NULL
            AND rec.start_lon IS NOT NULL
            AND rec.start_lat IS NOT NULL
            AND rec.end_lon IS NOT NULL
            AND rec.end_lat IS NOT NULL
            AND (
                ST_Distance(
                    ST_StartPoint(i.the_geom)::geography,
                    ST_SetSRID(ST_MakePoint(rec.start_lon, rec.start_lat), 4326)::geography
                ) <= p_max_distance
                OR ST_Distance(
                    ST_EndPoint(i.the_geom)::geography,
                    ST_SetSRID(ST_MakePoint(rec.end_lon, rec.end_lat), 4326)::geography
                ) <= p_max_distance
            )
    )
    -- DISTINCT ON para pegar apenas o melhor match por registro
    SELECT DISTINCT ON (m.idx)
        m.idx as record_index,
        m.itin_id::BIGINT as itinerario_id,
        m.itin_sentido::TEXT as sentido,
        m.itin_route_name::TEXT as route_name
    FROM matched m
    ORDER BY m.idx, m.dist_total ASC;
END;
$$ LANGUAGE plpgsql;
