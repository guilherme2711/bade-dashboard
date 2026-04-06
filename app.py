from flask import Flask, jsonify, send_from_directory, session, redirect, request
import mysql.connector
from datetime import date, datetime
import os

app = Flask(__name__, static_folder='static')
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'bade_giro_2026_secret_key_xk92'

# ─── Credenciais de acesso ───────────────────────────────────────────────────
USUARIO = 'bade'
SENHA   = '2026giro'

# ─── Banco de dados ──────────────────────────────────────────────────────────
DB_CONFIG = {
    'host':        '177.23.66.116',
    'port':        3306,
    'user':        'root',
    'password':    'UIWBDINA00TRCG50',
    'database':    'zada',
    'charset':     'utf8mb4',
    'use_unicode': True,
}

ESTABS = {
    1: 'Bade Viasul',
    2: 'Bade Aviação',
    3: 'Bade Centro',
    4: 'Bade Gressler',
    5: 'Bade Arroio do Meio',
    7: 'Bade Esportes',
}

DATAREF = '2026-03-15'

SQL_GIRO = """
    SELECT 
      TABITENS.CODPFABRIC AS codigo,
      TABITENS.DESCRICAO  AS descricao,
      COR.DESCRICAO       AS cor,
      MARC.DESCRICAO      AS marca,
      TABEMP.ESTAB        AS estab,
      SUM(
        COALESCE((SELECT SUM(FATBI.QUANTIDADE)
          FROM FATBI
          WHERE FATBI.ESTAB = TABEMP.ESTAB 
            AND FATBI.CODITEM = TABITENS.CODIGO 
            AND FATBI.DATA >= @DATAREF),0)
      ) AS qtd_vendida,
      SUM(
        COALESCE((SELECT ESTSALDO.SALDOFISICO
          FROM ESTSALDO
          WHERE ESTSALDO.ESTAB = TABEMP.ESTAB 
            AND ESTSALDO.ITEM = TABITENS.CODIGO 
          ORDER BY ESTSALDO.DATA DESC
          LIMIT 1),0)
      ) AS estoque,
      MAX(
        (SELECT MAX(ESTLANC.DATA)
         FROM ESTLANC
         WHERE ESTLANC.ESTAB = TABEMP.ESTAB 
           AND ESTLANC.ITEM = TABITENS.CODIGO 
           AND ESTLANC.TIPO = 'E')
      ) AS ult_entrada,
      (
        SUM(
          COALESCE((SELECT SUM(FATBI.QUANTIDADE)
            FROM FATBI
            WHERE FATBI.ESTAB = TABEMP.ESTAB 
              AND FATBI.CODITEM = TABITENS.CODIGO 
              AND FATBI.DATA >= @DATAREF),0)
        )
        /
        NULLIF(
          SUM(
            COALESCE((SELECT ESTSALDO.SALDOFISICO
              FROM ESTSALDO
              WHERE ESTSALDO.ESTAB = TABEMP.ESTAB 
                AND ESTSALDO.ITEM = TABITENS.CODIGO 
              ORDER BY ESTSALDO.DATA DESC
              LIMIT 1),0)
          )
          +
          SUM(
            COALESCE((SELECT SUM(FATBI.QUANTIDADE)
              FROM FATBI
              WHERE FATBI.ESTAB = TABEMP.ESTAB 
                AND FATBI.CODITEM = TABITENS.CODIGO 
                AND FATBI.DATA >= @DATAREF),0)
          )
        , 0)
      ) * 100 AS pct_vendido
    FROM TABITENS
    LEFT JOIN TABCOR COR   ON COR.CODIGO  = TABITENS.COR
    LEFT JOIN TABMARC MARC ON MARC.CODIGO = TABITENS.MARCA
    CROSS JOIN TABEMP 
    WHERE TABEMP.ESTAB IN (1, 2, 3, 4, 5, 7)
      AND TABITENS.COLECAO = 10
    GROUP BY 
      TABITENS.CODPFABRIC,
      TABITENS.DESCRICAO,
      COR.DESCRICAO,
      MARC.DESCRICAO,
      TABEMP.ESTAB
    HAVING qtd_vendida > 0 OR estoque > 0
    ORDER BY 
      TABEMP.ESTAB,
      TABITENS.CODPFABRIC,
      COR.DESCRICAO
"""

# ─── Helpers ─────────────────────────────────────────────────────────────────
def logado():
    return session.get('logado') is True

def parse_date(val):
    if val is None: return None
    if isinstance(val, datetime): return val.date()
    if isinstance(val, date): return val
    try: return datetime.strptime(str(val)[:10], '%Y-%m-%d').date()
    except: return None

def dias_desde_entrada(ult_entrada):
    d = parse_date(ult_entrada)
    if d is None: return None
    return (date.today() - d).days

def fetch_giro():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SET @DATAREF := '{DATAREF}'")
    cursor.execute(SQL_GIRO)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    result = []
    for r in rows:
        pct  = float(r['pct_vendido']) if r['pct_vendido'] is not None else 0.0
        estab = int(r['estab'])
        ult   = r['ult_entrada']
        dias  = dias_desde_entrada(ult)
        result.append({
            'codigo':       str(r['codigo']),
            'descricao':    str(r['descricao']) if r['descricao'] else '—',
            'cor':          str(r['cor']) if r['cor'] else '—',
            'marca':        str(r['marca']) if r['marca'] else '—',
            'estab':        estab,
            'estab_nome':   ESTABS.get(estab, f'Estab {estab}'),
            'qtd_vendida':  int(r['qtd_vendida']),
            'estoque':      int(r['estoque']),
            'ult_entrada':  str(ult) if ult else None,
            'dias_estoque': dias,
            'elegivel':     dias is not None and dias >= 15,
            'pct_vendido':  round(pct, 1),
        })
    return result

def calcular_transferencias(dados):
    from collections import defaultdict
    DIAS_MINIMOS = 15
    DIFF_PCT_MIN = 20.0
    grupos = defaultdict(list)
    for d in dados:
        grupos[(d['codigo'], d['cor'])].append(d)
    sugestoes = []
    vistos = set()
    for (codigo, cor), lojas in grupos.items():
        if len(lojas) < 2: continue
        for origem in lojas:
            dias = origem.get('dias_estoque')
            if dias is None or dias < DIAS_MINIMOS: continue
            for destino in lojas:
                if origem['estab'] == destino['estab']: continue
                chave = (codigo, cor, origem['estab'], destino['estab'])
                if chave in vistos: continue
                diff_pct     = destino['pct_vendido'] - origem['pct_vendido']
                diff_estoque = origem['estoque'] - destino['estoque']
                if diff_pct <= 0: continue
                criterio_pct     = diff_pct >= DIFF_PCT_MIN
                criterio_estoque = diff_estoque >= 1 and origem['pct_vendido'] < destino['pct_vendido']
                if not (criterio_pct or criterio_estoque): continue
                vistos.add(chave)
                qtd_sugerida = max(1, diff_estoque // 2) if diff_estoque > 0 else 1
                sugestoes.append({
                    'codigo':          codigo,
                    'descricao':       origem.get('descricao', '—'),
                    'cor':             cor,
                    'marca':           origem.get('marca', '—'),
                    'origem_estab':    origem['estab'],
                    'origem_nome':     origem['estab_nome'],
                    'origem_estoque':  origem['estoque'],
                    'origem_pct':      origem['pct_vendido'],
                    'origem_dias':     dias,
                    'destino_estab':   destino['estab'],
                    'destino_nome':    destino['estab_nome'],
                    'destino_estoque': destino['estoque'],
                    'destino_pct':     destino['pct_vendido'],
                    'diff_pct':        round(diff_pct, 1),
                    'qtd_sugerida':    int(qtd_sugerida),
                    'prioridade':      round(diff_pct + max(0, diff_estoque), 1),
                })
    sugestoes.sort(key=lambda x: x['prioridade'], reverse=True)
    return sugestoes

# ─── Rotas de autenticação ───────────────────────────────────────────────────
@app.route('/login', methods=['GET', 'POST'])
def login():
    erro = ''
    if request.method == 'POST':
        usuario = request.form.get('usuario', '')
        senha   = request.form.get('senha', '')
        if usuario == USUARIO and senha == SENHA:
            session['logado'] = True
            return redirect('/')
        erro = 'Usuário ou senha incorretos.'
    return send_from_directory('static', 'login.html'), 200 if not erro else 401

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

# ─── Rotas protegidas ────────────────────────────────────────────────────────
@app.route('/')
def index():
    if not logado():
        return redirect('/login')
    return send_from_directory('static', 'index.html')

@app.route('/api/giro')
def get_giro():
    if not logado():
        return jsonify({'erro': 'Não autorizado'}), 401
    try:
        return jsonify(fetch_giro())
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/transferencias')
def get_transferencias():
    if not logado():
        return jsonify({'erro': 'Não autorizado'}), 401
    try:
        return jsonify(calcular_transferencias(fetch_giro()))
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', debug=False, port=port)
