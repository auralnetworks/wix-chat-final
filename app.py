from flask import Flask, request, jsonify
import google.generativeai as genai
from google.cloud import bigquery
import os
import tempfile

app = Flask(__name__)

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

# CORS manual - más simple
@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route('/api/query', methods=['OPTIONS'])
def handle_options():
    response = jsonify({})
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route('/')
def home():
    return jsonify({"status": "Backend CORS Fix"})

@app.route('/api/test', methods=['POST'])
def test():
    return jsonify({
        "text": "✅ Backend funcionando!",
        "chart": {"labels": ["Test"], "values": [100]},
        "tickets": []
    })

@app.route('/api/query', methods=['POST'])
def query():
    try:
        print("🚀 Query recibido")
        
        # Obtener datos
        data = request.get_json() or {}
        user_query = data.get('query', 'test')
        
        print(f"Query: {user_query}")
        
        # SQL simple por defecto
        sql = f"SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` LIMIT 3"
        
        # Gemini con poder completo
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            prompt = f"""
            Genera SQL para BigQuery tabla `{TABLE_ID}` basada en: "{user_query}"
            
            Campos: Identifier, Estado, Canal, Fecha_de_inicio, Mensajes, Sentimiento_Inicial, Escalado, Tipificacion_Bot
            
            Ejemplos:
            - "whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 10
            - "hoy" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE DATE(Fecha_de_inicio) = CURRENT_DATE() LIMIT 10
            - "total" → SELECT COUNT(*) as total FROM `{TABLE_ID}`
            - "por canal" → SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Canal LIMIT 10
            - "escalados" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE Escalado = 'true' LIMIT 10
            
            REGLAS:
            1. SIEMPRE LIMIT 10 máximo
            2. Para fechas usar DATE(Fecha_de_inicio)
            3. Para conteos usar COUNT(*) as cantidad
            4. Solo SQL válido
            
            SQL:
            """
            
            response = model.generate_content(prompt)
            
            if response and response.text:
                gemini_sql = response.text.strip()
                gemini_sql = gemini_sql.replace('```sql', '').replace('```', '').strip()
                
                if 'SELECT' in gemini_sql.upper() and len(gemini_sql) > 15:
                    # Forzar LIMIT si no existe
                    if 'LIMIT' not in gemini_sql.upper() and 'COUNT(' not in gemini_sql.upper():
                        gemini_sql += ' LIMIT 10'
                    
                    sql = gemini_sql
                    print(f"🤖 Gemini SQL inteligente: {sql}")
                else:
                    print(f"❌ SQL inválido de Gemini: {gemini_sql}")
        except Exception as e:
            print(f"❌ Error Gemini: {e}")
        
        # Ejecutar consulta
        query_job = bq_client.query(sql)
        results = query_job.result()
        
        # Procesar resultados
        rows = []
        for row in results:
            rows.append({
                "id": str(row[0]) if row[0] else "N/A",
                "estado": str(row[1]) if len(row) > 1 else "N/A", 
                "canal": str(row[2]) if len(row) > 2 else "N/A"
            })
        
        # Respuesta inteligente
        if len(rows) == 0:
            text = "No se encontraron resultados para tu consulta"
            chart = {"labels": ["Sin datos"], "values": [0]}
            tickets = []
        else:
            # Detectar tipo de consulta para respuesta inteligente
            columns = [field.name for field in results.schema] if hasattr(results, 'schema') else []
            
            if 'cantidad' in columns:
                total = sum(int(row.get('cantidad', 0)) for row in rows if 'cantidad' in row)
                text = f"Análisis completado: {len(rows)} categorías con {total} registros totales"
                chart = {
                    "labels": [str(row.get(columns[0], 'N/A')) for row in rows[:8]],
                    "values": [int(row.get('cantidad', 0)) for row in rows[:8]]
                }
                tickets = []
            elif 'total' in columns:
                total = int(rows[0].get('total', 0)) if rows else 0
                text = f"Total de registros encontrados: {total}"
                chart = {"labels": ["Total"], "values": [total]}
                tickets = []
            else:
                text = f"Se encontraron {len(rows)} registros que coinciden con tu consulta: '{user_query}'"
                chart = {"labels": ["Registros"], "values": [len(rows)]}
                tickets = rows[:8]
        
        response_data = {
            "text": text,
            "chart": chart,
            "tickets": tickets
        }
        
        print(f"Enviando respuesta: {len(rows)} registros")
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({
            "text": f"Error: {str(e)[:50]}",
            "chart": {"labels": ["Error"], "values": [0]},
            "tickets": []
        })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
