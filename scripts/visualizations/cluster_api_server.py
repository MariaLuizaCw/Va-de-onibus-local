#!/usr/bin/env python3
"""
Servidor Flask simples para servir dados dos clusters via API
Usado pelo modal do visualizador de hotspots
"""

import sys
import os
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)  # Habilitar CORS para requisições do HTML

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
        password=os.environ.get('DB_PASSWORD', 'postgres')
    )

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

@app.route('/api/cluster-trips')
def get_cluster_trips():
    """API endpoint para buscar viagens de um cluster"""
    linha = request.args.get('linha')
    cluster_id = request.args.get('cluster_id')
    minutos_max = request.args.get('minutos_max', 5, type=int)
    eps_metros = request.args.get('eps_metros', 10, type=int)
    
    if not linha or not cluster_id:
        return jsonify({'error': 'Missing linha or cluster_id parameter'}), 400
    
    try:
        conn = get_db_connection()
        trips = fetch_cluster_trips(conn, linha, minutos_max, eps_metros, int(cluster_id))
        conn.close()
        
        # Converter datetime para string para JSON
        for trip in trips:
            if trip['timestamp_inicio']:
                trip['timestamp_inicio'] = trip['timestamp_inicio'].isoformat()
            if trip['timestamp_fim']:
                trip['timestamp_fim'] = trip['timestamp_fim'].isoformat()
        
        return jsonify({'trips': trips})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    print(f"Servidor API rodando em http://localhost:{port}")
    print("Use Ctrl+C para parar")
    app.run(host='0.0.0.0', port=port, debug=True)
