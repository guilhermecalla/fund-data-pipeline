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

## Uso

```console
python manage.py NOME_CÓDIGO
```


Fundos locais: (45 fundos)
                    875,1158,1159,1160,1576,1308,843,
                    427,984,144,732,506,161,964,685,499,
                    775,1298,934,1215,1299,1213,
                    657,1211,980,616,1184,1137,1277,
                    1212,1216,774,1303,159,1274,824,1569,
                    653,950,879,164,505,145,1924,1987

Fundos offshore:

1605,1606,1609,1610,1611,1617,1621,1622,1624,1626,
1628,1680,1686,1687,1688,1692,1698,1699,1700,1704,
1705,1706,1707,1708,1710,1713,1722,1723,1724,1726,
1727,1728,1731,1733,1734,1735,1755,1756,1765,1766,
1767,1769,1772,1774,1775,1776,1777,1778,1779,1780,
1788,1789,1790,1792,1793,1794,1797,1805,1806,1807,
1810,1811,1812,1813,1814,1815,1816,1817

Fundos carteiras: (46 fundos)

            875,1158,1159,1160,1576,1308,843,
            427,984,144,732,506,161,964,685,499,
            775,1298,934,1215,1299,1213,
            657,1211,980,616,1184,1137,1277,
            1212,1216,774,1303,159,1274,824,1569,
            653,950,879,164,505,145,1924,1987,1539