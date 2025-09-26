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
    """Genera SQL usando SOLO Gemini con poder de análisis completo"""
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql_prompt = f"""
    Eres experto en SQL y BigQuery. Genera una consulta SQL para la tabla `{TABLE_ID}` basada en: "{user_query}"

    CONTEXTO ACTUAL:
    - Fecha/Hora actual: {current_time}
    - Tabla: {TABLE_ID}
    
    CAMPOS PRINCIPALES DISPONIBLES:
    {MAIN_FIELDS}

    EJEMPLOS DE CONSULTAS INTELIGENTES:
    - "mensajes iniciales" → SELECT Identifier, Texto_del_Primer_Mensaje FROM `{TABLE_ID}` LIMIT 30
    - "tipificaciones bot" → SELECT Tipificacion_Bot, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Tipificacion_Bot
    - "tickets whatsapp" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 30
    - "hoy" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE DATE(Fecha_de_inicio) = CURRENT_DATE() LIMIT 30

    REGLAS CRÍTICAS:
    1. SIEMPRE usar LIMIT 30 máximo para evitar memoria excesiva
    2. Para "hoy": WHERE DATE(Fecha_de_inicio) = CURRENT_DATE()
    3. Para conteos: COUNT(*) as cantidad
    4. Para texto: LOWER() y LIKE '%texto%'
    5. Incluir Identifier cuando sea posible
    6. NUNCA usar SELECT * sin LIMIT

    IMPORTANTE: Solo devuelve la consulta SQL limpia, sin explicaciones ni markdown.

    SQL:
    """
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        sql = response.text.strip()
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').replace('`', '').strip()
        
        # Validación de seguridad
        dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(word in sql.upper() for word in dangerous):
            print(f"❌ SQL peligroso detectado")
            return None
        
        # Forzar LIMIT si no existe (crítico para memoria)
        if 'LIMIT' not in sql.upper() and 'COUNT(' not in sql.upper():
            sql += ' LIMIT 30'
        
        print(f"✅ SQL generado: {sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error con Gemini: {e}")
        return None

@app.route('/api/query', methods=['POST'])
def query():
    try:
        data = request.get_json()
        user_query = data.get('query', '').strip()
        
        if not user_query:
            return jsonify({"error": "Query vacío"}), 400
        
        # Generar SQL con Gemini
        sql = generate_dynamic_sql(user_query)
        if not sql:
            return jsonify({"error": "No se pudo generar SQL válido"}), 400
        
        print(f"Ejecutando SQL: {sql}")
        
        # Ejecutar consulta con límites estrictos
        query_job = bq_client.query(sql)
        results = query_job.result(max_results=50)
        
        # Procesar resultados sin pandas (ahorro de memoria)
        rows = []
        columns = [field.name for field in results.schema]
        
        for row in results:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = str(value) if value is not None else ""
            rows.append(row_dict)
            
            # Límite estricto para memoria
            if len(rows) >= 30:
                break
        
        # Generar respuesta
        if len(rows) == 0:
            response_text = "No se encontraron resultados"
            chart = {"labels": ["Sin datos"], "values": [0]}
            tickets = []
        else:
            response_text = f"Se encontraron {len(rows)} registros"
            
            # Chart inteligente
            if 'cantidad' in columns:
                chart = {
                    "labels": [str(row[columns[0]]) for row in rows[:10]],
                    "values": [int(float(str(row.get('cantidad', 0)))) for row in rows[:10]]
                }
            else:
                chart = {
                    "labels": ["Registros"],
                    "values": [len(rows)]
                }
            
            # Tickets (máximo 10)
            tickets = []
            if len(rows) <= 20:
                for row in rows[:10]:
                    ticket = {
                        "id": row.get('Identifier', 'N/A'),
                        "estado": row.get('Estado', 'N/A'),
                        "canal": row.get('Canal', 'N/A'),
                        "fecha": row.get('Fecha_de_inicio', 'N/A')
                    }
                    tickets.append(ticket)
        
        return jsonify({
            "text": response_text,
            "chart": chart,
            "tickets": tickets
        })
        
    except Exception as e:
        print(f"Error en query: {e}")
        return jsonify({"error": f"Error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
