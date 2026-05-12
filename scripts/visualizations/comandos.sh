#!/bin/bash
# =============================================================================
# COMANDOS PARA VISUALIZAÇÃO DE DADOS
# Execute estes comandos a partir da pasta scripts/visualizations//
# Os HTMLs gerados vão para a pasta maps/
# =============================================================================

# -----------------------------------------------------------------------------
# VISUALIZAR CLUSTERS DE UMA LINHA
# -----------------------------------------------------------------------------

# Linha 774 com eps=30 (clusters mais compactos)
python ./scripts/visualizations/visualizar_clusters.py 774 --eps 30 --percentil 0.95 -o ./maps/clusters_774.html

# Linha SP918 com eps=50 (padrão)
python ./scripts/visualizations/visualizar_clusters.py SP918 --velocidade-parado 1.5 --eps 50 --percentil 0.95 -o ./maps/clusters_SP918.html 

# Linha 457 
python ./scripts/visualizations/visualizar_clusters.py 457 --eps 50 --percentil 0.95 -o ./maps/clusters_457.html

# -----------------------------------------------------------------------------
# VISUALIZAR HOTSPOTS (script existente)
# -----------------------------------------------------------------------------

# Hotspots da linha 774 com janela de 5 minutos
python ./scripts/visualizations/visualizar_hotspots.py 774 --janela 5 -o ./maps/hotspots_774_5min.html

# -----------------------------------------------------------------------------
# VISUALIZAR UMA VIAGEM ESPECÍFICA
# -----------------------------------------------------------------------------

# Visualiza viagem pelo ID (do gps_historico_viagens)
python ./scripts/visualizations/visualizar_viagem.py 909865 -o ./maps/viagem_909865.html

# -----------------------------------------------------------------------------
# SERVIDOR API PARA HOTSPOTS (usado pelo HTML de hotspots)
# -----------------------------------------------------------------------------

# Iniciar servidor Flask (necessário para o modal de detalhes dos clusters)
python ./scripts/visualizations/cluster_api_server.py

# -----------------------------------------------------------------------------
# PARÂMETROS DISPONÍVEIS PARA visualizar_clusters.py
# -----------------------------------------------------------------------------
# 
# python ./scripts/visualizations/visualizar_clusters.py <linha> [opções]
#
# Opções:
#   --eps N              DBSCAN eps em metros (default: 50)
#   --minpoints N        DBSCAN min points (default: 5)
#   --duracao-min N      Duração mínima parado em segundos (default: 480 = 8min)
#   --min-paradas N      Mínimo de paradas para cluster (default: 20)
#   --duracao-garagem N  Duração para classificar como garagem em minutos (default: 30)
#   --percentil N        Percentil para convex hull (default: 0.9 = 90%)
#   --output, -o FILE    Arquivo de saída HTML
#
# Exemplos:
#   python ./scripts/visualizations/visualizar_clusters.py 774 --eps 30 --percentil 0.9
#   python ./scripts/visualizations/visualizar_clusters.py SP918 --eps 50 --min-paradas 10
#   python ./scripts/visualizations/visualizar_clusters.py 100 --duracao-garagem 60

# -----------------------------------------------------------------------------
# DEPENDÊNCIAS
# -----------------------------------------------------------------------------
# pip install folium shapely psycopg2-binary

# -----------------------------------------------------------------------------
# VARIÁVEIS DE AMBIENTE (opcional)
# -----------------------------------------------------------------------------
# export DB_HOST=localhost
# export DB_PORT=5432
# export DB_NAME=vadeonibus
# export DB_USER=postgres
# export DB_PASSWORD=postgres
