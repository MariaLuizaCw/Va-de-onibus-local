#!/usr/bin/env python3
"""
Visualiza uma viagem do gps_historico_viagens com cores diferentes para cada elemento.
Usa folium para gerar um mapa interativo.

Uso:
    python scripts/visualizar_viagem.py <id_viagem>
    
Exemplo:
    python scripts/visualizar_viagem.py 909865
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import folium
from shapely import wkb
from shapely.geometry import mapping
import json

# Cores para cada tipo de elemento
CORES = {
    'itinerario_origem': '#2196F3',      # Azul
    'itinerario_destino': '#4CAF50',     # Verde
    'pontos_origem': '#FF5722',          # Laranja
    'pontos_destino': '#9C27B0',         # Roxo
    'cluster_terminal': '#FFC107',       # Amarelo (buffer)
    'cluster_garagem': '#795548',        # Marrom (buffer)
}


def get_db_connection():
    """Conecta ao banco usando variáveis de ambiente ou .env"""
    # Tenta carregar do .env se existir
    env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key, value)
    
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', '5434'),
        database=os.environ.get('DB_NAME', 'vadeonibus'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'postgres')
    )


def fetch_viagem_data(conn, viagem_id):
    """Busca dados da viagem e geometrias relacionadas"""
    query = """
    WITH viagem AS (
        SELECT * FROM public.gps_historico_viagens WHERE id = %s
    ),
    pontos_origem AS (
        SELECT 
            (p->>'seq')::int AS seq,
            (p->>'latitude')::float AS lat,
            (p->>'longitude')::float AS lon
        FROM viagem, 
             jsonb_array_elements(metadados_origem->'pontos_avaliados') AS p
    ),
    pontos_destino AS (
        SELECT 
            (p->>'seq')::int AS seq,
            (p->>'latitude')::float AS lat,
            (p->>'longitude')::float AS lon
        FROM viagem, 
             jsonb_array_elements(metadados_destino->'pontos_avaliados') AS p
    )

    -- 1. Itinerário de origem
    SELECT 
        'itinerario_origem' AS tipo,
        'Itinerário Origem: ' || i.sentido AS descricao,
        ST_AsBinary(ST_SetSRID(i.the_geom, 4326)) AS geom
    FROM viagem v
    JOIN public.itinerario i ON i.id = v.itinerario_id_origem

    UNION ALL

    -- 2. Itinerário de destino
    SELECT 
        'itinerario_destino' AS tipo,
        'Itinerário Destino: ' || i.sentido AS descricao,
        ST_AsBinary(ST_SetSRID(i.the_geom, 4326)) AS geom
    FROM viagem v
    JOIN public.itinerario i ON i.id = v.itinerario_id_destino

    UNION ALL

    -- 3. Pontos avaliados na origem
    SELECT 
        'pontos_origem' AS tipo,
        'Ponto Origem #' || seq AS descricao,
        ST_AsBinary(ST_SetSRID(ST_MakePoint(lon, lat), 4326)) AS geom
    FROM pontos_origem

    UNION ALL

    -- 4. Pontos avaliados no destino
    SELECT 
        'pontos_destino' AS tipo,
        'Ponto Destino #' || seq AS descricao,
        ST_AsBinary(ST_SetSRID(ST_MakePoint(lon, lat), 4326)) AS geom
    FROM pontos_destino

    UNION ALL

    -- 5. Clusters Terminal da linha
    SELECT 
        'cluster_terminal' AS tipo,
        'Cluster Terminal #' || cpr.cluster_id || ' (' || COALESCE(cpr.sentido, '?') || ')' AS descricao,
        ST_AsBinary(cpr.geom_cluster::geometry) AS geom
    FROM viagem v
    JOIN clusters_parada_resultado cpr ON cpr.linha_analisada = v.linha
    WHERE cpr.tipo_cluster = 'Terminal'

    UNION ALL

    -- 6. Clusters Garagem da linha
    SELECT 
        'cluster_garagem' AS tipo,
        'Cluster Garagem #' || cpr.cluster_id AS descricao,
        ST_AsBinary(cpr.geom_cluster::geometry) AS geom
    FROM viagem v
    JOIN clusters_parada_resultado cpr ON cpr.linha_analisada = v.linha
    WHERE cpr.tipo_cluster = 'Garagem';
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (viagem_id,))
        return cur.fetchall()


def fetch_viagem_info(conn, viagem_id):
    """Busca informações básicas da viagem"""
    query = """
    SELECT 
        id, ordem, linha, token,
        timestamp_inicio, timestamp_fim,
        nome_terminal_origem, nome_terminal_destino,
        metadados_origem->>'metodo_deteccao' AS metodo_origem,
        metadados_destino->>'metodo_deteccao' AS metodo_destino,
        EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 AS duracao_minutos
    FROM gps_historico_viagens
    WHERE id = %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (viagem_id,))
        return cur.fetchone()


def create_map(viagem_info, geometries):
    """Cria mapa folium com as geometrias coloridas"""
    
    # Encontrar centro do mapa baseado nas geometrias
    all_coords = []
    for row in geometries:
        if row['geom']:
            # Converter memoryview para bytes se necessário
            geom_data = bytes(row['geom']) if isinstance(row['geom'], memoryview) else row['geom']
            geom = wkb.loads(geom_data, hex=False)
            if geom.geom_type == 'Point':
                all_coords.append((geom.y, geom.x))
            elif geom.geom_type == 'LineString':
                all_coords.extend([(c[1], c[0]) for c in geom.coords])
            elif geom.geom_type in ('Polygon', 'MultiPolygon'):
                centroid = geom.centroid
                all_coords.append((centroid.y, centroid.x))
    
    if not all_coords:
        print("Nenhuma geometria encontrada!")
        return None
    
    # Centro do mapa
    center_lat = sum(c[0] for c in all_coords) / len(all_coords)
    center_lon = sum(c[1] for c in all_coords) / len(all_coords)
    
    # Criar mapa
    m = folium.Map(location=[center_lat, center_lon], zoom_start=14)
    
    # Adicionar título
    title = f"Viagem #{viagem_info['id']} - Linha {viagem_info['linha']} - {viagem_info['duracao_minutos']:.1f} min"
    title_html = f'''
        <div style="position: fixed; top: 10px; left: 60px; z-index: 1000; 
                    background: white; padding: 10px; border-radius: 5px; 
                    box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
            <b>{title}</b><br>
            <small>Origem: {viagem_info['nome_terminal_origem'] or 'N/A'} ({viagem_info['metodo_origem']})</small><br>
            <small>Destino: {viagem_info['nome_terminal_destino'] or 'N/A'} ({viagem_info['metodo_destino']})</small>
        </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    # Adicionar geometrias
    for row in geometries:
        if not row['geom']:
            continue
        
        # Converter memoryview para bytes se necessário
        geom_data = bytes(row['geom']) if isinstance(row['geom'], memoryview) else row['geom']
        geom = wkb.loads(geom_data, hex=False)
        tipo = row['tipo']
        descricao = row['descricao']
        cor = CORES.get(tipo, '#000000')
        
        if geom.geom_type == 'Point':
            folium.CircleMarker(
                location=[geom.y, geom.x],
                radius=8,
                color=cor,
                fill=True,
                fillColor=cor,
                fillOpacity=0.7,
                popup=descricao,
                tooltip=descricao
            ).add_to(m)
            
        elif geom.geom_type == 'LineString':
            coords = [(c[1], c[0]) for c in geom.coords]
            folium.PolyLine(
                locations=coords,
                color=cor,
                weight=4,
                opacity=0.8,
                popup=descricao,
                tooltip=descricao
            ).add_to(m)
            
        elif geom.geom_type in ('Polygon', 'MultiPolygon'):
            # Clusters como polígonos preenchidos
            folium.GeoJson(
                mapping(geom),
                style_function=lambda x, cor=cor: {
                    'fillColor': cor,
                    'color': cor,
                    'weight': 2,
                    'fillOpacity': 0.3
                },
                popup=descricao,
                tooltip=descricao
            ).add_to(m)
    
    # Adicionar legenda
    legend_html = '''
    <div style="position: fixed; bottom: 30px; right: 30px; z-index: 1000;
                background: white; padding: 10px; border-radius: 5px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
        <b>Legenda</b><br>
        <i style="background: #2196F3; width: 12px; height: 12px; display: inline-block;"></i> Itinerário Origem<br>
        <i style="background: #4CAF50; width: 12px; height: 12px; display: inline-block;"></i> Itinerário Destino<br>
        <i style="background: #FF5722; width: 12px; height: 12px; display: inline-block; border-radius: 50%;"></i> Pontos Origem<br>
        <i style="background: #9C27B0; width: 12px; height: 12px; display: inline-block; border-radius: 50%;"></i> Pontos Destino<br>
        <i style="background: #FFC107; width: 12px; height: 12px; display: inline-block; opacity: 0.5;"></i> Cluster Terminal<br>
        <i style="background: #795548; width: 12px; height: 12px; display: inline-block; opacity: 0.5;"></i> Cluster Garagem
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


def main():
    if len(sys.argv) < 2:
        print("Uso: python visualizar_viagem.py <id_viagem>")
        print("Exemplo: python visualizar_viagem.py 909865")
        sys.exit(1)
    
    viagem_id = int(sys.argv[1])
    
    print(f"Buscando dados da viagem #{viagem_id}...")
    
    conn = get_db_connection()
    
    try:
        viagem_info = fetch_viagem_info(conn, viagem_id)
        if not viagem_info:
            print(f"Viagem #{viagem_id} não encontrada!")
            sys.exit(1)
        
        print(f"Linha: {viagem_info['linha']}, Duração: {viagem_info['duracao_minutos']:.1f} min")
        
        geometries = fetch_viagem_data(conn, viagem_id)
        print(f"Encontradas {len(geometries)} geometrias")
        
        m = create_map(viagem_info, geometries)
        if m:
            # Criar pasta maps se não existir
            maps_dir = os.path.join(os.path.dirname(__file__), '..', 'maps')
            os.makedirs(maps_dir, exist_ok=True)
            
            output_file = os.path.join(maps_dir, f"viagem_{viagem_id}.html")
            m.save(output_file)
            print(f"Mapa salvo em: {output_file}")
            print(f"Abra no navegador: file://{os.path.abspath(output_file)}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
