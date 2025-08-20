from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuración
GEMINI_API_KEY = "AIzaSyC7OceU-fwISiyihJsDDv51kMQEAkzEQ0k"
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
    return {"status": "Backend con BigQuery + Gráficos Inteligentes"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Webhooks Exitosos", "Webhooks Fallidos", "Pendientes"], "values": [45, 8, 2]}
    })

def generate_smart_chart(results, query):
    """Genera gráficos inteligentes basados en los datos y la consulta"""
    if len(results) == 0:
        return None
    
    # Si es una consulta de conteo
    if 'total' in results.columns:
        return {
            "labels": ["Total de Registros"],
            "values": [int(results['total'].iloc[0])]
        }
    
    # Si hay columnas de fecha/tiempo
    date_cols = [col for col in results.columns if 'date' in col.lower() or 'time' in col.lower() or 'created' in col.lower()]
    if date_cols:
        # Agrupar por fecha
        date_col = date_cols[0]
        try:
            results[date_col] = pd.to_datetime(results[date_col])
            daily_counts = results.groupby(results[date_col].dt.date).size()
            return {
                "labels": [str(date) for date in daily_counts.index[-7:]],  # Últimos 7 días
                "values": daily_counts.values[-7:].tolist()
            }
        except:
            pass
    
    # Si hay columnas de estado
    status_cols = [col for col in results.columns if 'status' in col.lower() or 'state' in col.lower()]
    if status_cols:
        status_counts = results[status_cols[0]].value_counts()
        return {
            "labels": status_counts.index.tolist()[:5],
            "values": status_counts.values.tolist()[:5]
        }
    
    # Si hay columnas numéricas
    numeric_cols = results.select_dtypes(include=['int64', 'float64']).columns
    if len(numeric_cols) > 0:
        return {
            "labels": [f"Registro {i+1}" for i in range(min(10, len(results)))],
            "values": results[numeric_cols[0]].head(10).tolist()
        }
    
    # Fallback: conteo de registros
    return {
        "labels": ["Registros Encontrados"],
        "values": [len(results)]
    }

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query'].lower()
        
        # Consultas inteligentes más específicas
        if any(word in user_query for word in ['total', 'count', 'cuántos', 'cantidad']):
            sql = f"SELECT COUNT(*) as total FROM `{TABLE_ID}`"
        elif any(word in user_query for word in ['hoy', 'today']):
            sql = f"SELECT * FROM `{TABLE_ID}` WHERE DATE(timestamp) = CURRENT_DATE() ORDER BY timestamp DESC"
        elif any(word in user_query for word in ['ayer', 'yesterday']):
            sql = f"SELECT * FROM `{TABLE_ID}` WHERE DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ORDER BY timestamp DESC"
        elif any(word in user_query for word in ['semana', 'week']):
            sql = f"SELECT DATE(timestamp) as fecha, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY DATE(timestamp) ORDER BY fecha"
        elif any(word in user_query for word in ['mes', 'month']):
            sql = f"SELECT DATE(timestamp) as fecha, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY DATE(timestamp) ORDER BY fecha"
        elif any(word in user_query for word in ['últimos', 'recientes', 'latest']):
            sql = f"SELECT * FROM `{TABLE_ID}` ORDER BY timestamp DESC LIMIT 10"
        elif any(word in user_query for word in ['status', 'estado']):
            sql = f"SELECT status, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY status ORDER BY cantidad DESC"
        else:
            sql = f"SELECT * FROM `{TABLE_ID}` ORDER BY timestamp DESC LIMIT 10"
        
        print(f"Ejecutando SQL: {sql}")  # Para debug
        
        # Ejecutar consulta BigQuery
        results = bq_client.query(sql).to_dataframe()
        
        # Generar gráfico inteligente
        chart_data = generate_smart_chart(results, user_query)
        
        # Procesar con Gemini
        model = genai.GenerativeModel('gemini-1.5-flash')
        data_summary = results.head(5).to_string() if len(results) > 0 else "No hay datos"
        
        prompt = f"""
        Usuario pregunta: {user_query}
        Datos de BigQuery (tabla Adereso_WebhookTests): {data_summary}
        Total de registros encontrados: {len(results)}
        
        Responde en español de forma conversacional y clara. Si hay datos, explica qué muestran.
        Si es una consulta de tiempo (hoy, ayer, semana), menciona las fechas específicas.
        Sé específico con los números y fechas encontrados.
        """
        
        response = model.generate_content(prompt)
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "data_count": len(results)
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")  # Para debug
        return jsonify({"text": f"Error consultando datos: {str(e)}", "chart": None}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
