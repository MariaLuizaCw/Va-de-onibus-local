
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


### 3. Importação das Functions do Banco

O backend depende de functions PostgreSQL. Importe manualmente os arquivos essenciais:

**Functions essenciais para os jobs de Angra:**
- `database/functions/angra.sql`
- `database/functions/itinerarioStore.sql`

**Para importar todas as functions:**

O script `apply-functions.sh` suporta dois modos de execução:

**Modo Container (padrão):**
```bash
./scripts/apply-functions.sh container
# ou simplesmente
./scripts/apply-functions.sh
```
Executa as functions via `docker exec` no container Postgres local.

**Modo Remoto:**
```bash
./scripts/apply-functions.sh remote
```
Conecta diretamente a um banco PostgreSQL em outra máquina na rede.

**Variáveis necessárias no .env:**
- **Modo container**: `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- **Modo remote**: `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`

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

