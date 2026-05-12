#!/usr/bin/env python3
"""
Visualiza hotspots de viagens curtas para uma linha específica.
Agrupa pontos de origem/destino por proximidade e mostra onde mais ocorrem viagens curtas.

Uso:
    python scripts/visualizar_hotspots.py <linha> [minutos_max]
    
Exemplo:
    python scripts/visualizar_hotspots.py 774 5
    python scripts/visualizar_hotspots.py 774 3
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import folium
from folium.plugins import HeatMap

# Imports para geometria
from shapely import wkb
from shapely.geometry import mapping


def get_db_connection():
    """Conecta ao banco usando variáveis de ambiente ou .env"""
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
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


def fetch_hotspots(conn, linha, minutos_max=5, min_ocorrencias=3, eps_metros=10):
    """
    Busca hotspots de viagens curtas usando DBSCAN (densidade).
    
    DBSCAN agrupa pontos por proximidade:
    - eps_metros: distância máxima entre pontos do mesmo cluster
    - min_ocorrencias: mínimo de pontos para formar um cluster
    """
    query = """
    WITH viagens_curtas AS (
        SELECT 
            id,
            linha,
            metadados_destino,
            EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 AS duracao_minutos,
            -- Extrair diferença de scores dos metadados
            -- fallback: diff_scores em detalhes_metodo
            -- score: calcular diferença entre 1º e 2º candidato em sentidos_candidatos
            COALESCE(
                (metadados_destino->'detalhes_metodo'->>'diff_scores')::float,
                (metadados_destino->'sentidos_candidatos'->0->>'score')::float 
                - (metadados_destino->'sentidos_candidatos'->1->>'score')::float
            ) AS diff_score
        FROM gps_historico_viagens
        WHERE linha = %s
          AND timestamp_inicio IS NOT NULL 
          AND timestamp_fim IS NOT NULL
          AND timestamp_fim >= timestamp_inicio
          AND EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 < %s
          AND (metadados_origem->>'metodo_deteccao') != 'garagem_por_distancia'
          AND (metadados_destino->>'metodo_deteccao') != 'garagem_por_distancia'
    ),
    -- Apenas pontos de DESTINO com geometria
    pontos_destino AS (
        SELECT 
            v.id AS viagem_id,
            (p->>'latitude')::float AS lat,
            (p->>'longitude')::float AS lon,
            v.duracao_minutos,
            v.diff_score,
            ST_SetSRID(ST_MakePoint(
                (p->>'longitude')::float, 
                (p->>'latitude')::float
            ), 4326) AS geom
        FROM viagens_curtas v,
             jsonb_array_elements(v.metadados_destino->'pontos_avaliados') WITH ORDINALITY AS t(p, seq)
        WHERE t.seq = 1
    ),
    -- DBSCAN clustering baseado em densidade
    pontos_com_cluster AS (
        SELECT 
            viagem_id,
            lat,
            lon,
            duracao_minutos,
            diff_score,
            geom,
            ST_ClusterDBSCAN(geom, eps := %s / 111000.0, minpoints := %s) OVER () AS cluster_id
        FROM pontos_destino
    ),
    -- Calcular centro de cada cluster primeiro
    cluster_centros AS (
        SELECT 
            cluster_id,
            AVG(lat) AS lat_centro,
            AVG(lon) AS lon_centro
        FROM pontos_com_cluster
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
    ),
    -- Calcular raio (distância máxima do centro)
    cluster_com_raio AS (
        SELECT 
            p.cluster_id,
            c.lat_centro,
            c.lon_centro,
            p.viagem_id,
            p.duracao_minutos,
            p.diff_score,
            ST_Distance(
                p.geom::geography,
                ST_SetSRID(ST_MakePoint(c.lon_centro, c.lat_centro), 4326)::geography
            ) AS dist_centro
        FROM pontos_com_cluster p
        JOIN cluster_centros c ON c.cluster_id = p.cluster_id
    ),
    -- Agregar por cluster
    clusters_agregados AS (
        SELECT 
            cluster_id,
            lat_centro,
            lon_centro,
            COUNT(*) AS total_ocorrencias,
            COUNT(DISTINCT viagem_id) AS viagens_distintas,
            AVG(duracao_minutos) AS duracao_media_min,
            MAX(dist_centro) AS raio_metros,
            -- Estatísticas de diferença de score
            AVG(diff_score) AS diff_score_media,
            STDDEV(diff_score) AS diff_score_stddev,
            COUNT(diff_score) AS viagens_com_score
        FROM cluster_com_raio
        GROUP BY cluster_id, lat_centro, lon_centro
    )
    SELECT 
        cluster_id,
        lat_centro,
        lon_centro,
        total_ocorrencias,
        viagens_distintas,
        ROUND(duracao_media_min::numeric, 1) AS duracao_media_min,
        COALESCE(raio_metros, 50) AS raio_metros,
        ROUND(diff_score_media::numeric, 3) AS diff_score_media,
        ROUND(diff_score_stddev::numeric, 3) AS diff_score_stddev,
        viagens_com_score
    FROM clusters_agregados
    ORDER BY total_ocorrencias DESC;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (linha, minutos_max, eps_metros, min_ocorrencias))
        return cur.fetchall()


def fetch_clusters_linha(conn, linha):
    """Busca clusters Terminal e Garagem da linha com geometria original"""
    query = """
    SELECT 
        tipo_cluster,
        cluster_id,
        sentido,
        ST_Y(ST_Centroid(geom_cluster::geometry)) AS lat,
        ST_X(ST_Centroid(geom_cluster::geometry)) AS lon,
        ST_AsBinary(geom_cluster::geometry) AS geom
    FROM clusters_parada_resultado
    WHERE linha_analisada = %s
      AND tipo_cluster IN ('Terminal', 'Garagem');
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (linha,))
        return cur.fetchall()


def fetch_itinerarios(conn, linha):
    """Busca itinerários (rotas) da linha"""
    query = """
    SELECT 
        id,
        sentido,
        route_name,
        ST_AsBinary(the_geom) AS geom
    FROM itinerario
    WHERE numero_linha = %s
      AND habilitado = true;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (linha,))
        return cur.fetchall()


def fetch_cluster_trips(conn, linha, minutos_max, eps_metros, cluster_id):
    """Busca todas as viagens de um cluster específico com metadados completos"""
    query = """
    WITH viagens_curtas AS (
        SELECT 
            id,
            ordem,
            linha,
            token,
            timestamp_inicio,
            timestamp_fim,
            EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 AS duracao_minutos,
            nome_terminal_origem,
            nome_terminal_destino,
            metodo_inferencia_origem,
            metodo_inferencia_destino,
            metadados_origem,
            metadados_destino,
            -- Extrair diferença de scores
            COALESCE(
                (metadados_destino->'detalhes_metodo'->>'diff_scores')::float,
                (metadados_destino->'sentidos_candidatos'->0->>'score')::float 
                - (metadados_destino->'sentidos_candidatos'->1->>'score')::float
            ) AS diff_score
        FROM gps_historico_viagens
        WHERE linha = %s
          AND timestamp_inicio IS NOT NULL 
          AND timestamp_fim IS NOT NULL
          AND timestamp_fim >= timestamp_inicio
          AND EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 < %s
          AND (metadados_origem->>'metodo_deteccao') != 'garagem_por_distancia'
          AND (metadados_destino->>'metodo_deteccao') != 'garagem_por_distancia'
    ),
    pontos_destino AS (
        SELECT 
            v.id AS viagem_id,
            v.*,
            (p->>'latitude')::float AS lat,
            (p->>'longitude')::float AS lon,
            ST_SetSRID(ST_MakePoint(
                (p->>'longitude')::float, 
                (p->>'latitude')::float
            ), 4326) AS geom
        FROM viagens_curtas v,
             jsonb_array_elements(v.metadados_destino->'pontos_avaliados') WITH ORDINALITY AS t(p, seq)
        WHERE t.seq = 1
    ),
    pontos_com_cluster AS (
        SELECT 
            *,
            ST_ClusterDBSCAN(geom, eps := %s / 111000.0, minpoints := 3) OVER () AS cluster_id
        FROM pontos_destino
    )
    SELECT 
        id,
        ordem,
        token,
        timestamp_inicio,
        timestamp_fim,
        duracao_minutos,
        nome_terminal_origem,
        nome_terminal_destino,
        metodo_inferencia_origem,
        metodo_inferencia_destino,
        metadados_origem,
        metadados_destino,
        diff_score,
        lat,
        lon
    FROM pontos_com_cluster
    WHERE cluster_id = %s
    ORDER BY timestamp_inicio DESC;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (linha, minutos_max, eps_metros, cluster_id))
        return cur.fetchall()


def fetch_stats(conn, linha, minutos_max=5):
    """Busca estatísticas gerais da linha"""
    query = """
    SELECT 
        COUNT(*) AS total_viagens,
        COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 < %s) AS viagens_curtas,
        ROUND(
            (COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (timestamp_fim - timestamp_inicio)) / 60 < %s)::numeric / COUNT(*)) * 100, 
            1
        ) AS percentual_curtas
    FROM gps_historico_viagens
    WHERE linha = %s
      AND timestamp_inicio IS NOT NULL 
      AND timestamp_fim IS NOT NULL
      AND timestamp_fim >= timestamp_inicio;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (minutos_max, minutos_max, linha))
        return cur.fetchone()


def create_map(linha, hotspots, clusters, itinerarios, stats, minutos_max):
    """Cria mapa folium com hotspots, clusters e itinerários"""
    
    if not hotspots:
        print("Nenhum hotspot encontrado!")
        return None
    
    # Centro do mapa baseado nos hotspots
    center_lat = sum(h['lat_centro'] for h in hotspots) / len(hotspots)
    center_lon = sum(h['lon_centro'] for h in hotspots) / len(hotspots)
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=14)
    
    # Título com estatísticas
    title = f"Hotspots Viagens Curtas - Linha {linha}"
    stats_text = f"Total: {stats['total_viagens']} | Curtas (<{minutos_max}min): {stats['viagens_curtas']} ({stats['percentual_curtas']}%)"
    title_html = f'''
        <div style="position: fixed; top: 10px; left: 60px; z-index: 1000; 
                    background: white; padding: 10px; border-radius: 5px; 
                    box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
            <b>{title}</b><br>
            <small>{stats_text}</small><br>
            <small>Hotspots: {len(hotspots)} | Itinerários: {len(itinerarios)}</small>
        </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    # PRIMEIRO: Desenhar itinerários (rotas) - camada mais baixa
    # Cores bem distintas e estilos diferentes para cada sentido
    estilos_itinerario = [
        {'cor': '#0066CC', 'dash': None, 'weight': 5},      # Azul escuro, sólido
        {'cor': '#CC6600', 'dash': '10, 10', 'weight': 5},  # Laranja, tracejado
        {'cor': '#006600', 'dash': '5, 5', 'weight': 4},    # Verde escuro, tracejado curto
        {'cor': '#660066', 'dash': '15, 5', 'weight': 4},   # Roxo, tracejado longo
    ]
    for i, it in enumerate(itinerarios):
        if it.get('geom'):
            geom_data = bytes(it['geom']) if isinstance(it['geom'], memoryview) else it['geom']
            geom = wkb.loads(geom_data, hex=False)
            estilo = estilos_itinerario[i % len(estilos_itinerario)]
            
            # Converter LineString para coordenadas
            if geom.geom_type == 'LineString':
                coords = [(c[1], c[0]) for c in geom.coords]
                folium.PolyLine(
                    locations=coords,
                    color=estilo['cor'],
                    weight=estilo['weight'],
                    opacity=0.8,
                    dash_array=estilo['dash'],
                    popup=f"Itinerário {it['sentido']} - {it['route_name'] or ''}",
                    tooltip=f"Sentido: {it['sentido']}"
                ).add_to(m)
    
    # SEGUNDO: Adicionar clusters Terminal e Garagem
    for c in clusters:
        if c['tipo_cluster'] == 'Terminal':
            cor = '#FFC107'  # Amarelo
        else:
            cor = '#795548'  # Marrom
        
        # Renderizar geometria original do cluster (buffer real com todos os pontos)
        if c.get('geom'):
            geom_data = bytes(c['geom']) if isinstance(c['geom'], memoryview) else c['geom']
            geom = wkb.loads(geom_data, hex=False)
            
            folium.GeoJson(
                mapping(geom),
                style_function=lambda x, cor=cor: {
                    'fillColor': cor,
                    'color': cor,
                    'weight': 2,
                    'fillOpacity': 0.3
                },
                popup=f"Cluster {c['tipo_cluster']} #{c['cluster_id']} ({c['sentido'] or '?'})",
                tooltip=f"{c['tipo_cluster']} #{c['cluster_id']}"
            ).add_to(m)
    
    # DEPOIS: Adicionar hotspots como círculos (ficam por cima, clicáveis)
    # Raio real do cluster calculado pelo DBSCAN
    max_ocorrencias = max(h['total_ocorrencias'] for h in hotspots)
    
    for h in hotspots:
        # Intensidade da cor baseada em ocorrências (0.3 a 1.0)
        intensidade = 0.3 + 0.7 * (h['total_ocorrencias'] / max_ocorrencias)
        
        # Cor vermelha com intensidade variável
        r = int(255 * intensidade)
        cor = f'#{r:02x}0000'
        
        # Raio real do cluster (calculado pelo DBSCAN)
        raio = max(20, float(h['raio_metros']))  # mínimo 20m para visibilidade
        
        # Info de score (se disponível)
        score_info = ""
        if h.get('viagens_com_score') and h['viagens_com_score'] > 0:
            diff_media = h.get('diff_score_media') or 0
            diff_stddev = h.get('diff_score_stddev') or 0
            score_info = f"""
            <hr style="margin: 5px 0;">
            <b>Scores ({h['viagens_com_score']} viagens)</b><br>
            Diff média: {diff_media:.3f}<br>
            Desvio padrão: {diff_stddev:.3f}
            """
        
        popup_text = f"""
        <b>Hotspot Destino (DBSCAN)</b><br>
        Cluster #{h['cluster_id']}<br>
        Ocorrências: {h['total_ocorrencias']}<br>
        Viagens distintas: {h['viagens_distintas']}<br>
        Duração média: {h['duracao_media_min']} min<br>
        Raio: {raio:.0f}m
        {score_info}
        <br><br>
        <button onclick="showClusterModal({h['cluster_id']}, {h['lat_centro']}, {h['lon_centro']}, '{linha}', {minutos_max}, 10)" 
                style="background: #2196F3; color: white; padding: 5px 10px; border: none; border-radius: 3px; cursor: pointer;">
            Ver Todas as Viagens
        </button>
        """
        
        # Circle com raio REAL em metros (tamanho do cluster DBSCAN)
        folium.Circle(
            location=[h['lat_centro'], h['lon_centro']],
            radius=raio,
            color=cor,
            fill=True,
            fillColor=cor,
            fillOpacity=0.6,
            popup=folium.Popup(popup_text, max_width=400),
            tooltip=f"Cluster #{h['cluster_id']}: {h['total_ocorrencias']} ocorrências ({raio:.0f}m)"
        ).add_to(m)
    
    # Legenda
    legend_html = '''
    <div style="position: fixed; bottom: 30px; right: 30px; z-index: 1000;
                background: white; padding: 10px; border-radius: 5px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
        <b>Hotspots Destino</b><br>
        <i style="background: #4d0000; width: 12px; height: 12px; display: inline-block; border-radius: 50%;"></i> Poucas ocorrências<br>
        <i style="background: #ff0000; width: 12px; height: 12px; display: inline-block; border-radius: 50%;"></i> Muitas ocorrências<br>
        <small>(raio = tamanho real DBSCAN)</small><br>
        <hr style="margin: 5px 0;">
        <b>Itinerários</b><br>
        <i style="background: #0066CC; width: 20px; height: 3px; display: inline-block;"></i> Sentido 1 (sólido)<br>
        <i style="background: #CC6600; width: 20px; height: 3px; display: inline-block; border-top: 2px dashed #CC6600;"></i> Sentido 2 (tracejado)<br>
        <hr style="margin: 5px 0;">
        <b>Clusters</b><br>
        <i style="background: #FFC107; width: 12px; height: 12px; display: inline-block; opacity: 0.6;"></i> Terminal<br>
        <i style="background: #795548; width: 12px; height: 12px; display: inline-block; opacity: 0.6;"></i> Garagem
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Adicionar modal e JavaScript para detalhes do cluster
    modal_html = f'''
    <!-- Modal para detalhes do cluster -->
    <div id="clusterModal" class="modal" style="display: none; position: fixed; z-index: 9999; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.7);">
        <div class="modal-content" style="background-color: white; margin: 5% auto; padding: 20px; border: 1px solid #888; width: 90%; max-width: 1200px; max-height: 80%; overflow-y: auto;">
            <span class="close" style="color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer;">&times;</span>
            <h2 id="modalTitle">Cluster Details</h2>
            <div id="modalBody">Loading...</div>
        </div>
    </div>
    
    <script>
    // Dados pré-carregados dos clusters (limitado para não sobrecarregar o HTML)
    window.clusterTripsData = {{}};
    
    function showClusterModal(clusterId, lat, lon, linha, minutosMax, epsMetros) {{
        document.getElementById('modalTitle').innerHTML = `Cluster #${{clusterId}} - Linha ${{linha}}`;
        document.getElementById('modalBody').innerHTML = `
            <p><strong>Location:</strong> ${{lat.toFixed(6)}}, ${{lon.toFixed(6)}}</p>
            <p><strong>Nota:</strong> Para ver os detalhes completos das viagens, execute:</p>
            <pre style="background: #f5f5f5; padding: 10px; border-radius: 3px;">
python scripts/cluster_api_server.py 5000
# Depois clique no botão "Carregar Detalhes" abaixo
            </pre>
            <button onclick="loadClusterDetails(${{clusterId}}, '${{linha}}', ${{minutosMax}}, ${{epsMetros}})" 
                    style="background: #4CAF50; color: white; padding: 8px 15px; border: none; border-radius: 3px; cursor: pointer; margin-top: 10px;">
                Carregar Detalhes
            </button>
        `;
        document.getElementById('clusterModal').style.display = 'block';
    }}
    
    function loadClusterDetails(clusterId, linha, minutosMax, epsMetros) {{
        document.getElementById('modalBody').innerHTML = '<p>Buscando detalhes das viagens...</p>';
        
        fetch(`http://localhost:5000/api/cluster-trips?linha=${{linha}}&cluster_id=${{clusterId}}&minutos_max=${{minutosMax}}&eps_metros=${{epsMetros}}`)
            .then(response => {{
                if (!response.ok) {{
                    throw new Error('Servidor API não está rodando. Execute: python scripts/cluster_api_server.py 5000');
                }}
                return response.json();
            }})
            .then(data => {{
                let html = `<p><strong>Location:</strong> ${{window.clusterData[${{clusterId}}].lat_centro.toFixed(6)}}, ${{window.clusterData[${{cluster_id}}].lon_centro.toFixed(6)}}<br>`;
                html += `<strong>Total Trips:</strong> ${{data.trips.length}}</p>`;
                html += '<table border="1" style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<tr style="background-color: #f2f2f2;"><th>ID</th><th>Ordem</th><th>Token</th><th>Duration</th><th>Origin</th><th>Destination</th><th>Method</th><th>Diff Score</th><th>Metadata</th></tr>';
                
                data.trips.forEach(trip => {{
                    html += `<tr>`;
                    html += `<td>${{trip.id}}</td>`;
                    html += `<td>${{trip.ordem}}</td>`;
                    html += `<td>${{trip.token}}</td>`;
                    html += `<td>${{trip.duracao_minutos?.toFixed(1) || 'N/A'}} min</td>`;
                    html += `<td>${{trip.nome_terminal_origem || 'N/A'}}</td>`;
                    html += `<td>${{trip.nome_terminal_destino || 'N/A'}}</td>`;
                    html += `<td>${{trip.metodo_inferencia_destino || 'N/A'}}</td>`;
                    html += `<td>${{trip.diff_score ? trip.diff_score.toFixed(3) : 'N/A'}}</td>`;
                    html += `<td><details><summary>View</summary><pre style="font-size: 10px; max-height: 200px; overflow-y: auto;">${{JSON.stringify(trip.metadados_destino, null, 2)}}</pre></details></td>`;
                    html += `</tr>`;
                }});
                
                html += '</table>';
                document.getElementById('modalBody').innerHTML = html;
            }})
            .catch(error => {{
                document.getElementById('modalBody').innerHTML = `
                    <p><strong>Erro:</strong> ${{error.message}}</p>
                    <p><strong>Solução:</strong></p>
                    <ol>
                        <li>Execute: <code>python scripts/cluster_api_server.py 5000</code></li>
                        <li>Depois clique em "Carregar Detalhes" novamente</li>
                    </ol>
                `;
            }});
    }}
    
    // Close modal
    document.addEventListener('DOMContentLoaded', function() {{
        const modal = document.getElementById('clusterModal');
        const span = document.getElementsByClassName('close')[0];
        
        span.onclick = function() {{
            modal.style.display = 'none';
        }}
        
        window.onclick = function(event) {{
            if (event.target == modal) {{
                modal.style.display = 'none';
            }}
        }}
    }});
    </script>
    '''
    
    m.get_root().html.add_child(folium.Element(modal_html))
    
    return m


def main():
    if len(sys.argv) < 2:
        print("Uso: python visualizar_hotspots.py <linha> [minutos_max]")
        print("Exemplo: python visualizar_hotspots.py 774 5")
        print("Exemplo: python visualizar_hotspots.py 774 3")
        sys.exit(1)
    
    linha = sys.argv[1]
    minutos_max = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    
    print(f"Buscando hotspots de viagens curtas (<{minutos_max}min) para linha {linha}...")
    
    conn = get_db_connection()
    
    try:
        stats = fetch_stats(conn, linha, minutos_max)
        if not stats or stats['total_viagens'] == 0:
            print(f"Nenhuma viagem encontrada para linha {linha}")
            sys.exit(1)
        
        print(f"Total viagens: {stats['total_viagens']}, Curtas (<{minutos_max}min): {stats['viagens_curtas']} ({stats['percentual_curtas']}%)")
        
        hotspots = fetch_hotspots(conn, linha, minutos_max)
        print(f"Hotspots encontrados: {len(hotspots)}")
        
        clusters = fetch_clusters_linha(conn, linha)
        print(f"Clusters da linha: {len(clusters)}")
        
        itinerarios = fetch_itinerarios(conn, linha)
        print(f"Itinerários da linha: {len(itinerarios)}")
        
        m = create_map(linha, hotspots, clusters, itinerarios, stats, minutos_max)
        if m:
            maps_dir = os.path.join(os.path.dirname(__file__), '..', 'maps')
            os.makedirs(maps_dir, exist_ok=True)
            
            output_file = os.path.join(maps_dir, f"hotspots_linha_{linha}_{minutos_max}min.html")
            m.save(output_file)
            print(f"Mapa salvo em: {output_file}")
            print(f"Abra no navegador: file://{os.path.abspath(output_file)}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
