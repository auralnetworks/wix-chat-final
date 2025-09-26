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
    
    print(f"🔍 Generando SQL para: {user_query}")
    
    sql_prompt = f"""
    Genera SQL para BigQuery tabla `{TABLE_ID}` basada en: "{user_query}"

    Campos: Identifier, Estado, Canal, Fecha_de_inicio, Mensajes, Texto_del_Primer_Mensaje

    Ejemplos:
    - "whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 20
    - "hoy" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE DATE(Fecha_de_inicio) = CURRENT_DATE() LIMIT 20
    - "total" → SELECT COUNT(*) as total FROM `{TABLE_ID}`

    REGLAS:
    1. Usar backticks en nombres de tabla: `{TABLE_ID}`
    2. SIEMPRE LIMIT 20 máximo
    3. Solo SQL válido, sin explicaciones

    SQL:
    """
    
    try:
        print("🤖 Llamando a Gemini...")
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        
        if not response or not response.text:
            print("❌ Gemini no devolvió respuesta")
            return None
            
        sql = response.text.strip()
        print(f"📝 Respuesta cruda de Gemini: {sql}")
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        # Validación básica
        if not sql or len(sql) < 10:
            print("❌ SQL muy corto o vacío")
            return None
            
        # Validación de seguridad
        dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(word in sql.upper() for word in dangerous):
            print(f"❌ SQL peligroso detectado: {sql}")
            return None
        
        # Forzar LIMIT si no existe
        if 'LIMIT' not in sql.upper() and 'COUNT(' not in sql.upper():
            sql += ' LIMIT 20'
        
        print(f"✅ SQL final: {sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error completo con Gemini: {str(e)}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        return None

@app.route('/api/query', methods=['POST'])
def query():
    # Respuesta por defecto para evitar undefined
    default_response = {
        "text": "Procesando consulta...",
        "chart": {"labels": ["Cargando"], "values": [1]},
        "tickets": []
    }
    
    try:
        data = request.get_json()
        if not data:
            return jsonify(default_response)
            
        user_query = data.get('query', '').strip()
        print(f"Query: {user_query}")
        
        if not user_query:
            default_response["text"] = "Query vacío"
            return jsonify(default_response)
        
        # Generar SQL simple primero
        sql = f"SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` LIMIT 10"
        
        # Intentar con Gemini
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            prompt = f"SQL para `{TABLE_ID}` query '{user_query}': SELECT campos FROM tabla LIMIT 10"
            response = model.generate_content(prompt)
            if response and response.text:
                gemini_sql = response.text.strip().replace('```sql', '').replace('```', '')
                if 'SELECT' in gemini_sql.upper() and len(gemini_sql) > 10:
                    sql = gemini_sql
                    if 'LIMIT' not in sql.upper():
                        sql += ' LIMIT 10'
        except:
            pass  # Usar SQL por defecto
        
        print(f"SQL: {sql}")
        
        # Ejecutar consulta
        query_job = bq_client.query(sql)
        results = query_job.result(max_results=15)
        
        # Procesar resultados
        rows = []
        for row in results:
            row_data = {}
            for i, value in enumerate(row):
                field_name = results.schema[i].name if i < len(results.schema) else f"col_{i}"
                row_data[field_name] = str(value) if value is not None else ""
            rows.append(row_data)
            if len(rows) >= 10:
                break
        
        # Generar respuesta garantizada
        if len(rows) == 0:
            response_data = {
                "text": "No se encontraron resultados",
                "chart": {"labels": ["Sin datos"], "values": [0]},
                "tickets": []
            }
        else:
            response_data = {
                "text": f"Encontrados {len(rows)} registros",
                "chart": {"labels": ["Registros"], "values": [len(rows)]},
                "tickets": [{
                    "id": row.get('Identifier', 'N/A'),
                    "estado": row.get('Estado', 'N/A'),
                    "canal": row.get('Canal', 'N/A')
                } for row in rows[:5]]
            }
        
        print(f"Respuesta: {len(rows)} registros")
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error: {e}")
        error_response = {
            "text": f"Error: {str(e)}",
            "chart": {"labels": ["Error"], "values": [0]},
            "tickets": []
        }
        return jsonify(error_response), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
