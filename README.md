# OPS

Sistema para gerenciamento de dados de fundos usando a API Maravi.

## Configuração do Ambiente

### 1. Criar ambiente virtual

```console
conda create -n ops python=3.12
conda activate ops
```

### 2. Instalar dependências

```console
pip install -r requirements.txt
```

### 3. Configurar variáveis de ambiente

Copie o arquivo `.env.example` para `.env` e preencha com suas credenciais:

```console
copy .env.example .env
```

Edite o arquivo `.env` com suas credenciais reais:

```
MARAVI_USER=seu_usuario@exemplo.com
MARAVI_PASS=sua_senha
MARAVI_CLIENT_ID=seu_client_id
MARAVI_CLIENT_SECRET=seu_client_secret

DB_HOST=ip_do_servidor
DB_USER=seu_usuario_db
DB_PASS=sua_senha_db
DB_BASE=nome_do_banco
```

**IMPORTANTE:** Nunca commite o arquivo `.env` no Git!
