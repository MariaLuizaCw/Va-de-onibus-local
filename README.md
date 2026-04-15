
## Deploy em Produção

### 1. Preparação do Ambiente

```bash
# Clone o repositório
git clone <URL_DO_REPOSITORIO>
cd Va-de-onibus-local

# Copie o arquivo .env fornecido para o ambiente de produção
# O arquivo .env já deve conter todas as variáveis configuradas
# Se necessário, ajuste as variáveis específicas do seu ambiente
```

### 2. Configuração das Variáveis de Ambiente

O arquivo `.env` fornecido já contém as configurações principais. Verifique e ajuste se necessário e correlacione com o docker-compose.yml. Observe principalmente as portas em que os serviços serão levantados e as credenciais do banco de dados fornecidadas para o serviço do backend.

**Variáveis GTFS-RT:**
- `GTFSRTURL`: URL base da API GTFS-RT (ex: `http://localhost:3333` ou `https://seu-servidor-gtfs.com`) 


### 3. Configuração do Banco de Dados

O backend depende de tabelas e functions PostgreSQL. Use o script `apply-functions.sh` para aplicar automaticamente todos os arquivos SQL dos diretórios `database/creates/`, `database/functions/` e `database/seeds/` (se existir).

#### Uso do Script

```bash
./scripts/apply-functions.sh [container|remote]
```

| Parâmetro | Descrição |
|-----------|-----------|
| `container` | **(padrão)** Executa via `docker exec` no container Postgres local (`vadeonibus-db`) |
| `remote` | Conecta diretamente a um banco PostgreSQL remoto via `psql` |

#### Exemplos

**Modo Container (padrão):**
```bash
./scripts/apply-functions.sh container
# ou simplesmente
./scripts/apply-functions.sh
```

**Modo Remoto:**
```bash
./scripts/apply-functions.sh remote
```

#### Variáveis de Ambiente Necessárias (.env)

| Variável | Container | Remote | Descrição |
|----------|:---------:|:------:|-----------|
| `DB_NAME` | ✓ | ✓ | Nome do banco de dados |
| `DB_USER` | ✓ | ✓ | Usuário do banco |
| `DB_PASSWORD` | ✓ | ✓ | Senha do banco |
| `DB_HOST` | | ✓ | Host do banco remoto |
| `DB_PORT` | | ✓ | Porta do banco remoto |

#### O que o script faz

1. Carrega as variáveis do arquivo `.env`
2. Executa todos os arquivos `.sql` de `database/creates/` e `database/functions/` (em ordem alfabética)
3. Se existir o diretório `database/seeds/`, carrega os dados de seed (truncando as tabelas antes)

### 4. Execução dos Serviços

**Apenas o backend:**
```bash
docker compose up --build backend
```

**Apenas o frontend:**
```bash
docker compose up --build frontend
```


### 5. Configuração do Frontend

O frontend precisa receber a URL do backend via variável de ambiente:
- `PUBLIC_BACKEND_URL`: URL completa do backend (ex: `https://seu-dominio.com/api`)

