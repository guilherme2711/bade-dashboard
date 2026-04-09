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
    3: 'Centro / Esportes',  # estab 3 e 7 mesclados
    4: 'Bade Gressler',
    5: 'Bade Arroio do Meio',
}

# Estabs que serão mesclados em um único
ESTAB_MESCLAR_EM   = 3   # estab que representa o grupo (e dono do estoque)
ESTAB_MESCLAR_DE   = 7   # estab que será absorvido (soma vendas, ignora estoque)

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
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SET @DATAREF := '{DATAREF}'")
        cursor.execute(SQL_GIRO)
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    # ── Primeira passagem: monta registros individuais ──────────────────────
    brutos = []
    for r in rows:
        pct   = float(r['pct_vendido']) if r['pct_vendido'] is not None else 0.0
        estab = int(r['estab'])
        ult   = r['ult_entrada']
        dias  = dias_desde_entrada(ult)
        brutos.append({
            'codigo':       str(r['codigo']),
            'descricao':    str(r['descricao']) if r['descricao'] else '—',
            'cor':          str(r['cor']) if r['cor'] else '—',
            'marca':        str(r['marca']) if r['marca'] else '—',
            'estab':        estab,
            'qtd_vendida':  int(r['qtd_vendida']),
            'estoque':      int(r['estoque']),
            'ult_entrada':  str(ult) if ult else None,
            'dias_estoque': dias,
        })

    # ── Mesclagem: Centro (3) + Esportes (7) ────────────────────────────────
    # Agrupa por codigo+cor+estab para facilitar a mesclagem
    from collections import defaultdict
    por_chave = defaultdict(list)
    for b in brutos:
        # Estab 7 é renomeado para 3 para agrupar junto
        estab_key = ESTAB_MESCLAR_EM if b['estab'] == ESTAB_MESCLAR_DE else b['estab']
        por_chave[(b['codigo'], b['cor'], estab_key)].append(b)

    result = []
    for (codigo, cor, estab), itens in por_chave.items():
        # Vendas: soma de todos os itens do grupo
        qtd_vendida = sum(i['qtd_vendida'] for i in itens)

        # Estoque:
        # - Para Centro/Esportes (estab 3): apenas do estab 3, ignora estab 7
        # - Para todas as outras lojas: usa o estoque normalmente
        if estab == ESTAB_MESCLAR_EM:
            estoque = sum(i['estoque'] for i in itens if i['estab'] == ESTAB_MESCLAR_EM)
        else:
            estoque = sum(i['estoque'] for i in itens)

        # Última entrada: a mais recente
        datas = [i['ult_entrada'] for i in itens if i['ult_entrada']]
        ult_entrada = max(datas) if datas else None
        dias = dias_desde_entrada(ult_entrada)

        # % vendido recalculado
        total = qtd_vendida + estoque
        pct = round((qtd_vendida / total * 100), 1) if total > 0 else 0.0

        result.append({
            'codigo':       codigo,
            'descricao':    itens[0]['descricao'],
            'cor':          cor,
            'marca':        itens[0]['marca'],
            'estab':        estab,
            'estab_nome':   ESTABS.get(estab, f'Estab {estab}'),
            'qtd_vendida':  qtd_vendida,
            'estoque':      estoque,
            'ult_entrada':  ult_entrada,
            'dias_estoque': dias,
            'elegivel':     dias is not None and dias >= 15,
            'pct_vendido':  pct,
        })

    return result

def calcular_transferencias(dados):
    from collections import defaultdict

    DIAS_MINIMOS    = 15
    DIFF_PCT_MIN    = 20.0
    ESTAB_PRINCIPAL = ESTAB_MESCLAR_EM  # Centro/Esportes = estab 3
    RESERVA_CENTRO  = 1.5  # Centro deve manter 50% a mais que a média das outras

    grupos = defaultdict(list)
    for d in dados:
        grupos[(d['codigo'], d['cor'])].append(d)

    sugestoes = []
    vistos = set()

    for (codigo, cor), lojas in grupos.items():
        if len(lojas) < 2:
            continue

        # Calcula média de estoque das lojas que NÃO são o Centro
        outras_lojas   = [l for l in lojas if l['estab'] != ESTAB_PRINCIPAL]
        centro         = next((l for l in lojas if l['estab'] == ESTAB_PRINCIPAL), None)

        # Só calcula reserva se houver outras lojas para comparar
        if outras_lojas:
            media_outras   = sum(l['estoque'] for l in outras_lojas) / len(outras_lojas)
            reserva_centro = round(media_outras * RESERVA_CENTRO)
        else:
            media_outras   = 0
            reserva_centro = 0

        for origem in lojas:
            dias = origem.get('dias_estoque')
            if dias is None or dias < DIAS_MINIMOS:
                continue

            for destino in lojas:
                if origem['estab'] == destino['estab']:
                    continue

                chave = (codigo, cor, origem['estab'], destino['estab'])
                if chave in vistos:
                    continue

                diff_pct     = destino['pct_vendido'] - origem['pct_vendido']
                diff_estoque = origem['estoque'] - destino['estoque']

                if diff_pct <= 0:
                    continue

                criterio_pct     = diff_pct >= DIFF_PCT_MIN
                criterio_estoque = diff_estoque >= 1 and origem['pct_vendido'] < destino['pct_vendido']

                if not (criterio_pct or criterio_estoque):
                    continue

                # ── Regra especial Centro/Esportes como ORIGEM ──────────────
                if origem['estab'] == ESTAB_PRINCIPAL:
                    excecao_giro = (origem['pct_vendido'] < 30 and 
                                   destino['pct_vendido'] > 70 and
                                   origem['estoque'] > reserva_centro)  # só se tiver excedente real

                    if not excecao_giro:
                        estoque_disponivel = origem['estoque'] - reserva_centro
                        if estoque_disponivel <= 0:
                            continue
                        qtd_sugerida = max(1, estoque_disponivel // 2)
                    else:
                        excedente = origem['estoque'] - reserva_centro
                        qtd_sugerida = max(1, excedente // 2)
                else:
                    # ── Outras lojas: lógica normal ─────────────────────────
                    qtd_sugerida = max(1, diff_estoque // 2) if diff_estoque > 0 else 1

                vistos.add(chave)

                prioridade = round(diff_pct + max(0, diff_estoque), 1)

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
                    'prioridade':      prioridade,
                    'reserva_centro':  reserva_centro if origem['estab'] == ESTAB_PRINCIPAL else None,
                    'media_outras':    round(media_outras, 1) if origem['estab'] == ESTAB_PRINCIPAL else None,
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

@app.route('/api/debug_ref/<path:codigo>')
def debug_ref(codigo):
    if not logado():
        return jsonify({'erro': 'Não autorizado'}), 401
    try:
        dados = fetch_giro()
        itens = [d for d in dados if d['codigo'].upper() == codigo.upper()]
        if not itens:
            return jsonify({'erro': f'Referência {codigo} não encontrada'})
        
        # Calcula a reserva para cada cor encontrada
        from collections import defaultdict
        grupos = defaultdict(list)
        for d in itens:
            grupos[d['cor']].append(d)
        
        resultado = {}
        for cor, lojas in grupos.items():
            outras = [l for l in lojas if l['estab'] != ESTAB_MESCLAR_EM]
            centro = next((l for l in lojas if l['estab'] == ESTAB_MESCLAR_EM), None)
            media_outras = (sum(l['estoque'] for l in outras) / len(outras)) if outras else 0
            reserva = round(media_outras * 1.5)
            resultado[cor] = {
                'lojas': lojas,
                'media_estoque_outras': round(media_outras, 2),
                'reserva_calculada_50pct': reserva,
                'centro_estoque': centro['estoque'] if centro else 0,
                'excedente_disponivel': max(0, (centro['estoque'] if centro else 0) - reserva),
            }
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@app.route('/api/debug')
def debug():
    if not logado():
        return jsonify({'erro': 'Não autorizado'}), 401
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SET @DATAREF := '{DATAREF}'")
            cursor.execute(SQL_GIRO)
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()
        from collections import defaultdict
        por_estab = defaultdict(list)
        for r in rows:
            por_estab[int(r['estab'])].append({
                'codigo':      str(r['codigo']),
                'cor':         str(r['cor']) if r['cor'] else '—',
                'qtd_vendida': int(r['qtd_vendida']),
                'estoque':     int(r['estoque']),
                'pct_vendido': round(float(r['pct_vendido']) if r['pct_vendido'] else 0, 1),
            })
        resumo = {}
        for estab, itens in sorted(por_estab.items()):
            resumo[f'estab_{estab}'] = {
                'total_registros': len(itens),
                'primeiros_3': itens[:3]
            }
        return jsonify(resumo)
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@app.route('/api/debug2')
def debug2():
    if not logado():
        return jsonify({'erro': 'Não autorizado'}), 401
    try:
        dados = fetch_giro()
        from collections import defaultdict
        por_estab = defaultdict(list)
        for d in dados:
            por_estab[d['estab_nome']].append({
                'codigo':      d['codigo'],
                'cor':         d['cor'],
                'qtd_vendida': d['qtd_vendida'],
                'estoque':     d['estoque'],
                'pct_vendido': d['pct_vendido'],
            })
        resumo = {}
        for estab, itens in sorted(por_estab.items()):
            com_venda = [i for i in itens if i['qtd_vendida'] > 0]
            resumo[estab] = {
                'total_registros':   len(itens),
                'com_venda':         len(com_venda),
                'zerados':           len(itens) - len(com_venda),
                'primeiros_3':       itens[:3],
                'primeiros_3_venda': com_venda[:3],
            }
        return jsonify(resumo)
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


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
