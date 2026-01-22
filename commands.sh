

# Túnel Banco
nohup ssh -f -N -R 5434:localhost:5434 marialuiza@galeao.cos.ufrj.br >> /dev/null 2>&1 &

# Importar functions e tabelas
./scripts/apply-functions.sh container

# Compose com arquivo db
sudo docker compose -f docker-compose.yml.dev up --build db


# Compose com um arquivo backend
sudo docker compose -f docker-compose.yml.dev up --build backend

# Compose com um arquivo frontend
sudo docker compose -f docker-compose.yml.dev up --build frontend