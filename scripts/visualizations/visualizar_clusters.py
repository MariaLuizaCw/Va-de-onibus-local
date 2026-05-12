#!/usr/bin/env python3
"""
Visualiza clusters de parada e itinerários de uma linha de ônibus.
Gera um mapa HTML interativo com Folium.

Uso:
    python visualizar_clusters.py <linha> [--eps 50] [--percentil 0.9]
    
Exemplo:
    python visualizar_clusters.py 774
    python visualizar_clusters.py SP918 --eps 30 --percentil 0.9
"""

import argparse
import os
import sys
import folium
from folium import plugins
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from shapely import wkb
from shapely.geometry import mapping

def get_db_connection():
    """Conecta ao banco usando variáveis de ambiente ou .env"""
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
        password=os.environ.get('DB_PASSWORD', 'postgres'),
        cursor_factory=RealDictCursor
    )

# Cores para os tipos de cluster
CORES_CLUSTER = {
    'Terminal': '#2ecc71',      # verde
    'Garagem': '#e74c3c',       # vermelho
    'Indefinido': '#95a5a6',    # cinza
}

# Cores para sentidos
CORES_SENTIDO = {
    'IDA': '#3498db',           # azul
    'VOLTA': '#9b59b6',         # roxo
}


def conectar_db():
    """Conecta ao banco de dados PostgreSQL."""
    return get_db_connection()


def buscar_clusters(conn, linha, eps=50, minpoints=5, duracao_min=480, 
                    min_paradas=20, duracao_garagem=30, percentil=0.9, velocidade_parado=1.0):
    """Busca clusters de parada para uma linha."""
    query = """
        SELECT * FROM fn_analisar_clusters_linha(
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    with conn.cursor() as cur:
        cur.execute(query, (linha, eps, minpoints, duracao_min, 
                           min_paradas, duracao_garagem, percentil, velocidade_parado))
        return cur.fetchall()


def buscar_itinerarios(conn, linha):
    """Busca itinerários habilitados para uma linha."""
    query = """
        SELECT 
            id,
            numero_linha,
            sentido,
            route_name,
            ST_AsGeoJSON(the_geom) as geojson
        FROM itinerario
        WHERE numero_linha = %s AND habilitado = true
    """
    with conn.cursor() as cur:
        cur.execute(query, (linha,))
        return cur.fetchall()


def criar_mapa(clusters, itinerarios, linha):
    """Cria mapa Folium com clusters e itinerários."""
    
    # Determinar centro do mapa
    if clusters:
        lat_center = sum(c['lat_cluster'] for c in clusters) / len(clusters)
        lon_center = sum(c['lon_cluster'] for c in clusters) / len(clusters)
    elif itinerarios:
        # Usar primeiro ponto do primeiro itinerário
        geojson = json.loads(itinerarios[0]['geojson'])
        coords = geojson['coordinates']
        if geojson['type'] == 'LineString':
            lon_center, lat_center = coords[len(coords)//2]
        else:
            lon_center, lat_center = coords[0][len(coords[0])//2]
    else:
        lat_center, lon_center = -22.9, -43.2  # Rio de Janeiro default
    
    # Criar mapa
    m = folium.Map(
        location=[lat_center, lon_center],
        zoom_start=14,
        tiles='cartodbpositron'
    )
    
    # Adicionar itinerários
    fg_itinerarios = folium.FeatureGroup(name='Itinerários')
    for it in itinerarios:
        geojson = json.loads(it['geojson'])
        cor = CORES_SENTIDO.get(it['sentido'], '#333333')
        
        folium.GeoJson(
            geojson,
            style_function=lambda x, cor=cor: {
                'color': cor,
                'weight': 4,
                'opacity': 0.7
            },
            tooltip=f"{it['route_name']} ({it['sentido']})"
        ).add_to(fg_itinerarios)
        
        # Marcar início do itinerário
        if geojson['type'] == 'LineString':
            inicio = geojson['coordinates'][0]
        else:
            inicio = geojson['coordinates'][0][0]
        
        folium.CircleMarker(
            location=[inicio[1], inicio[0]],
            radius=8,
            color=cor,
            fill=True,
            fillColor=cor,
            fillOpacity=0.8,
            tooltip=f"Início: {it['route_name']} ({it['sentido']})"
        ).add_to(fg_itinerarios)
    
    fg_itinerarios.add_to(m)
    
    # Adicionar clusters
    fg_clusters = folium.FeatureGroup(name='Clusters')
    for cl in clusters:
        cor = CORES_CLUSTER.get(cl['tipo_cluster'], '#333333')
        
        # Desenhar o polígono do cluster (geom_cluster)
        if cl['geom_cluster']:
            try:
                # geom_cluster é geography, converter para GeoJSON
                geom = wkb.loads(cl['geom_cluster'], hex=True)
                geojson = mapping(geom)
                
                folium.GeoJson(
                    geojson,
                    style_function=lambda x, cor=cor: {
                        'color': cor,
                        'weight': 2,
                        'fillColor': cor,
                        'fillOpacity': 0.3
                    },
                    tooltip=f"{cl['tipo_cluster']} - {cl['sentido'] or 'N/A'}"
                ).add_to(fg_clusters)
            except Exception as e:
                print(f"Erro ao processar geometria do cluster {cl['cluster_id']}: {e}")
        
        # Marcar centro do cluster
        popup_html = f"""
        <b>Cluster {cl['cluster_id']}</b><br>
        Tipo: {cl['tipo_cluster']}<br>
        Sentido: {cl['sentido'] or 'N/A'}<br>
        Paradas: {cl['num_paradas']}<br>
        Pontos GPS: {cl.get('total_pontos_gps', 'N/A')}<br>
        Mediana duração: {cl['mediana_duracao_minutos']} min<br>
        Raio (p90): {cl['max_distance_metros']} m<br>
        Hora mediana: {cl['hora_mediana_cluster']}h
        """
        
        folium.Marker(
            location=[float(cl['lat_cluster']), float(cl['lon_cluster'])],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(
                color='green' if cl['tipo_cluster'] == 'Terminal' else 
                      'red' if cl['tipo_cluster'] == 'Garagem' else 'gray',
                icon='bus' if cl['tipo_cluster'] == 'Terminal' else 
                     'home' if cl['tipo_cluster'] == 'Garagem' else 'question',
                prefix='fa'
            ),
            tooltip=f"{cl['tipo_cluster']} - {cl['sentido'] or 'N/A'}"
        ).add_to(fg_clusters)
    
    fg_clusters.add_to(m)
    
    # Adicionar controle de camadas
    folium.LayerControl().add_to(m)
    
    # Adicionar título
    title_html = f'''
        <div style="position: fixed; 
                    top: 10px; left: 50px; 
                    z-index: 1000;
                    background-color: white;
                    padding: 10px;
                    border-radius: 5px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.3);">
            <h4 style="margin: 0;">Linha {linha}</h4>
            <small>{len(clusters)} clusters | {len(itinerarios)} itinerários</small>
        </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    return m


def main():
    parser = argparse.ArgumentParser(description='Visualiza clusters e itinerários de uma linha')
    parser.add_argument('linha', help='Número da linha (ex: 774, SP918)')
    parser.add_argument('--eps', type=int, default=50, help='DBSCAN eps em metros (default: 50)')
    parser.add_argument('--minpoints', type=int, default=5, help='DBSCAN min points (default: 5)')
    parser.add_argument('--duracao-min', type=int, default=480, help='Duração mínima parado em segundos (default: 480)')
    parser.add_argument('--min-paradas', type=int, default=20, help='Mínimo de paradas para cluster (default: 20)')
    parser.add_argument('--duracao-garagem', type=float, default=30, help='Duração para classificar como garagem em minutos (default: 30)')
    parser.add_argument('--percentil', type=float, default=0.9, help='Percentil para convex hull (default: 0.9)')
    parser.add_argument('--velocidade-parado', type=float, default=1.0, help='Velocidade máxima para considerar parado em m/s (default: 1.0 = ~3.6 km/h)')
    parser.add_argument('--output', '-o', help='Arquivo de saída (default: clusters_<linha>.html)')
    
    args = parser.parse_args()
    
    # Conectar ao banco
    print(f"Conectando ao banco de dados...")
    try:
        conn = conectar_db()
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        sys.exit(1)
    
    try:
        # Buscar dados
        print(f"Buscando clusters da linha {args.linha}...")
        clusters = buscar_clusters(
            conn, args.linha, 
            eps=args.eps,
            minpoints=args.minpoints,
            duracao_min=args.duracao_min,
            min_paradas=args.min_paradas,
            duracao_garagem=args.duracao_garagem,
            percentil=args.percentil,
            velocidade_parado=args.velocidade_parado
        )
        print(f"  -> {len(clusters)} clusters encontrados")
        
        for cl in clusters:
            pontos_gps = cl.get('total_pontos_gps', '?')
            print(f"     - Cluster {cl['cluster_id']}: {cl['tipo_cluster']} ({cl['sentido'] or 'N/A'}), {cl['num_paradas']} paradas, {pontos_gps} pontos GPS, raio {cl['max_distance_metros']}m")
        
        print(f"Buscando itinerários da linha {args.linha}...")
        itinerarios = buscar_itinerarios(conn, args.linha)
        print(f"  -> {len(itinerarios)} itinerários encontrados")
        
        for it in itinerarios:
            print(f"     - {it['route_name']} ({it['sentido']})")
        
        # Criar mapa
        print("Gerando mapa...")
        mapa = criar_mapa(clusters, itinerarios, args.linha)
        
        # Salvar
        output_file = args.output or f"clusters_{args.linha}.html"
        mapa.save(output_file)
        print(f"Mapa salvo em: {output_file}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
