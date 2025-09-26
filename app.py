from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import tempfile
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuración
GEMINI_API_KEY = "AIzaSyCbNt5deM5N9zRbaSZAFkGmlbjHvuOuRgk"
PROJECT_ID = "esval-435215"
TABLE_ID = "esval-435215.webhooks.Adereso_WebhookTests"

# Configurar credenciales
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
if creds_json:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name

genai.configure(api_key=GEMINI_API_KEY)
bq_client = bigquery.Client(project=PROJECT_ID)

# Campos principales (reducidos para memoria)
MAIN_FIELDS = """ID, Fecha_de_inicio, Estado, Canal, Sentimiento_Inicial, Nick_del_Cliente, 
Mensajes, Texto_del_Primer_Mensaje, Texto_del_ultimo_Mensaje, Identifier, Escalado, 
Tipificacion_Bot, Menu_inicial, Tiempo_de_Abordaje__Segundos_, BOT_DERIVATION_Date"""

@app.route('/')
def home():
    return {"status": "Backend Gemini Puro - Optimizado"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test"], "values": [100]},
        "tickets": []
    })

def generate_dynamic_sql(user_query):
    """Genera SQL usando Gemini con PODER COMPLETO como antes"""
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql_prompt = f"""
    Eres experto en SQL y BigQuery. Genera una consulta SQL para la tabla `{TABLE_ID}` basada en: "{user_query}"

    CONTEXTO ACTUAL:
    - Fecha/Hora actual: {current_time}
    - Tabla: {TABLE_ID}
    
    CAMPOS DISPONIBLES:
    {MAIN_FIELDS}

    EJEMPLOS DE CONSULTAS INTELIGENTES:
    - "mensajes iniciales" → SELECT Identifier, Texto_del_Primer_Mensaje FROM `{TABLE_ID}` LIMIT 20
    - "tipificaciones bot" → SELECT Tipificacion_Bot, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Tipificacion_Bot
    - "tickets whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 20
    - "hoy" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE DATE(Fecha_de_inicio) = CURRENT_DATE() LIMIT 20
    - "sentimientos" → SELECT Sentimiento_Inicial, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Sentimiento_Inicial
    - "escalados" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE Escalado = 'true' LIMIT 20

    REGLAS IMPORTANTES:
    1. Para "hoy" usa: WHERE DATE(Fecha_de_inicio) = CURRENT_DATE()
    2. Para "ayer" usa: WHERE DATE(Fecha_de_inicio) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    3. Para conteos usa COUNT(*) as cantidad
    4. Para búsquedas de texto usa LOWER() y LIKE '%texto%'
    5. Incluye siempre Identifier cuando sea posible
    6. Para campos booleanos usa = 'true' o = '1'
    7. LIMIT 20 máximo para memoria

    IMPORTANTE: Solo devuelve la consulta SQL limpia, sin explicaciones ni markdown.

    SQL:
    """
    
    try:
        print(f"🤖 Gemini analizando: {user_query}")
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        
        if not response or not response.text:
            print("❌ Gemini sin respuesta")
            return None
            
        sql = response.text.strip()
        print(f"📝 SQL crudo: {sql}")
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        # Validaciones
        if not sql or len(sql) < 15:
            print("❌ SQL inválido")
            return None
            
        dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(word in sql.upper() for word in dangerous):
            print("❌ SQL peligroso")
            return None
        
        # Forzar LIMIT para memoria
        if 'LIMIT' not in sql.upper() and 'COUNT(' not in sql.upper():
            sql += ' LIMIT 20'
        
        print(f"✅ SQL inteligente generado: {sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error Gemini: {e}")
        return None

@app.route('/api/query', methods=['POST'])
def query():
    try:
        data = request.get_json()
        user_query = data.get('query', '').strip()
        
        print(f"🚀 Query recibido: {user_query}")
        
        if not user_query:
            return jsonify({"error": "Query vacío"}), 400
        
        # USAR GEMINI CON PODER COMPLETO
        sql = generate_dynamic_sql(user_query)
        if not sql:
            return jsonify({"error": "Gemini no pudo generar SQL"}), 400
        
        print(f"🔍 Ejecutando SQL de Gemini: {sql}")
        
        # Ejecutar consulta
        query_job = bq_client.query(sql)
        results = query_job.result(max_results=25)
        
        # Procesar con pandas para mantener funcionalidad original
        import pandas as pd
        
        # Convertir a DataFrame
        rows_data = []
        for row in results:
            row_dict = {}
            for i, field in enumerate(results.schema):
                row_dict[field.name] = row[i]
            rows_data.append(row_dict)
        
        if len(rows_data) == 0:
            return jsonify({
                "text": "No se encontraron resultados",
                "chart": {"labels": ["Sin datos"], "values": [0]},
                "tickets": []
            })
        
        df = pd.DataFrame(rows_data)
        print(f"📈 DataFrame creado con {len(df)} filas y columnas: {list(df.columns)}")
        
        # Generar gráfico inteligente
        chart = generate_chart_with_identifiers(df)
        
        # Generar tickets inteligentes
        tickets = generate_tickets_data(df, user_query)
        
        # Texto de respuesta inteligente
        response_text = generate_response_text(df, user_query)
        
        return jsonify({
            "text": response_text,
            "chart": chart,
            "tickets": tickets
        })
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return jsonify({"error": f"Error: {str(e)}"}), 500

def generate_chart_with_identifiers(df):
    """Genera gráfico inteligente como antes"""
    if len(df) == 0:
        return {"labels": ["Sin datos"], "values": [0]}
    
    # Para datos agregados
    if 'cantidad' in df.columns:
        return {
            "labels": df.iloc[:, 0].astype(str).tolist()[:10],
            "values": df['cantidad'].tolist()[:10]
        }
    
    if 'total' in df.columns:
        return {
            "labels": ["Total"],
            "values": [int(df['total'].iloc[0])]
        }
    
    # Para listados
    return {
        "labels": ["Registros Encontrados"],
        "values": [len(df)]
    }

def generate_tickets_data(df, user_query):
    """Genera tickets inteligentes"""
    if len(df) == 0 or len(df) > 20:
        return []
    
    tickets = []
    for _, row in df.head(8).iterrows():
        ticket = {
            "id": str(row.get('Identifier', 'N/A')),
            "estado": str(row.get('Estado', 'N/A')),
            "canal": str(row.get('Canal', 'N/A')),
            "fecha": str(row.get('Fecha_de_inicio', 'N/A')),
            "mensajes": str(row.get('Mensajes', 'N/A'))
        }
        tickets.append(ticket)
    
    return tickets

def generate_response_text(df, user_query):
    """Genera texto de respuesta inteligente"""
    if len(df) == 0:
        return "No se encontraron resultados para tu consulta"
    
    if 'cantidad' in df.columns:
        total = df['cantidad'].sum()
        return f"Análisis completado: {len(df)} categorías encontradas con {total} registros totales"
    
    if 'total' in df.columns:
        return f"Total de registros: {df['total'].iloc[0]}"
    
    return f"Se encontraron {len(df)} registros que coinciden con tu consulta"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
