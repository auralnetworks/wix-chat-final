from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import tempfile

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

@app.route('/')
def home():
    return {"status": "Backend Simple Funcional"}

@app.route('/api/test', methods=['POST'])
def test():
    return jsonify({
        "text": "✅ Backend funcionando!",
        "chart": {"labels": ["Test"], "values": [100]},
        "tickets": []
    })

@app.route('/api/query', methods=['POST'])
def query():
    print("🚀 Endpoint /api/query llamado")
    
    try:
        # Obtener datos
        data = request.get_json()
        print(f"📥 Datos recibidos: {data}")
        
        user_query = data.get('query', '') if data else ''
        print(f"🔍 Query: '{user_query}'")
        
        # Generar SQL con Gemini
        sql = None
        try:
            prompt = f"SQL para BigQuery tabla `{TABLE_ID}` query '{user_query}': SELECT Identifier, Estado, Canal FROM tabla LIMIT 5"
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = model.generate_content(prompt)
            
            if response and response.text:
                sql = response.text.strip().replace('```sql', '').replace('```', '')
                if 'SELECT' not in sql.upper():
                    sql = None
                print(f"🤖 SQL Gemini: {sql}")
        except Exception as e:
            print(f"❌ Gemini error: {e}")
        
        # SQL por defecto si Gemini falla
        if not sql:
            sql = f"SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` LIMIT 5"
            print(f"🔧 SQL por defecto: {sql}")
        
        # Ejecutar consulta
        print("🔍 Ejecutando consulta...")
        query_job = bq_client.query(sql)
        results = query_job.result()
        
        # Procesar resultados
        rows = []
        for row in results:
            rows.append({
                "Identifier": str(row[0]) if row[0] else "N/A",
                "Estado": str(row[1]) if len(row) > 1 and row[1] else "N/A",
                "Canal": str(row[2]) if len(row) > 2 and row[2] else "N/A"
            })
        
        print(f"📊 Encontrados {len(rows)} registros")
        
        # Crear respuesta SIMPLE
        response_data = {
            "text": f"Encontrados {len(rows)} registros para: {user_query}",
            "chart": {
                "labels": ["Registros"],
                "values": [len(rows)]
            },
            "tickets": [
                {
                    "id": row["Identifier"],
                    "estado": row["Estado"],
                    "canal": row["Canal"]
                } for row in rows[:3]
            ]
        }
        
        print(f"✅ Respuesta: {response_data}")
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ ERROR TOTAL: {str(e)}")
        
        # Respuesta de emergencia
        emergency_response = {
            "text": f"Error: {str(e)[:50]}",
            "chart": {"labels": ["Error"], "values": [0]},
            "tickets": []
        }
        
        print(f"🚨 Respuesta de emergencia: {emergency_response}")
        return jsonify(emergency_response), 500

if __name__ == '__main__':
    print("🚀 Iniciando servidor...")
    app.run(host='0.0.0.0', port=5000, debug=True)
