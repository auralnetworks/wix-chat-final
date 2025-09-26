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
        
        # Intentar Gemini
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            prompt = f"SQL BigQuery para '{user_query}': SELECT campos FROM `{TABLE_ID}` LIMIT 3"
            response = model.generate_content(prompt)
            
            if response and response.text:
                gemini_sql = response.text.strip().replace('```', '').replace('sql', '')
                if 'SELECT' in gemini_sql.upper():
                    sql = gemini_sql
                    print(f"Gemini SQL: {sql}")
        except:
            print("Gemini falló, usando SQL por defecto")
        
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
        
        # Respuesta simple
        response_data = {
            "text": f"Encontrados {len(rows)} registros",
            "chart": {"labels": ["Registros"], "values": [len(rows)]},
            "tickets": rows
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
