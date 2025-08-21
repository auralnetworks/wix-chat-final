
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile

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
    return {"status": "Backend Adereso - Consultas Inteligentes"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend Adereso funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Tickets Abiertos", "Tickets Cerrados", "En Proceso"], "values": [25, 45, 8]}
    })

def generate_chart(results, query_type):
    """Genera gráficos basados en el tipo de consulta y datos"""
    if len(results) == 0:
        return None
    
    if query_type == "count":
        return {
            "labels": ["Total de Tickets"],
            "values": [int(results.iloc[0, 0])]
        }
    elif query_type == "estado":
        return {
            "labels": results['Estado'].tolist(),
            "values": results['cantidad'].tolist()
        }
    elif query_type == "canal":
        return {
            "labels": results['Canal'].tolist(),
            "values": results['cantidad'].tolist()
        }
    elif query_type == "sentimiento":
        return {
            "labels": results['Sentimiento_Inicial'].tolist(),
            "values": results['cantidad'].tolist()
        }
    elif query_type == "fecha":
        return {
            "labels": results['Fecha_de_inicio'].astype(str).tolist(),
            "values": results['cantidad'].tolist()
        }
    else:
        # Gráfico por defecto con mensajes
        if 'Mensajes' in results.columns:
            return {
                "labels": [f"Ticket {i+1}" for i in range(min(10, len(results)))],
                "values": results['Mensajes'].head(10).fillna(0).tolist()
            }
        return {
            "labels": ["Registros Encontrados"],
            "values": [len(results)]
        }

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query'].lower()
        query_type = "default"
        
        # Consultas inteligentes con campos reales
        if any(word in user_query for word in ['total', 'count', 'cuántos', 'cantidad']):
            sql = f"SELECT COUNT(*) as total FROM `{TABLE_ID}`"
            query_type = "count"
            
        elif any(word in user_query for word in ['estado', 'status']):
            sql = f"SELECT Estado, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Estado IS NOT NULL GROUP BY Estado ORDER BY cantidad DESC"
            query_type = "estado"
            
        elif any(word in user_query for word in ['canal', 'canales']):
            sql = f"SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Canal IS NOT NULL GROUP BY Canal ORDER BY cantidad DESC"
            query_type = "canal"
            
        elif any(word in user_query for word in ['sentimiento', 'sentiment']):
            sql = f"SELECT Sentimiento_Inicial, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Sentimiento_Inicial IS NOT NULL GROUP BY Sentimiento_Inicial ORDER BY cantidad DESC"
            query_type = "sentimiento"
            
        elif any(word in user_query for word in ['hoy', 'today']):
            sql = f"SELECT * FROM `{TABLE_ID}` WHERE Fecha_de_inicio = CURRENT_DATE() ORDER BY Hora_de_inicio DESC LIMIT 10"
            
        elif any(word in user_query for word in ['ayer', 'yesterday']):
            sql = f"SELECT * FROM `{TABLE_ID}` WHERE Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ORDER BY Hora_de_inicio DESC LIMIT 10"
            
        elif any(word in user_query for word in ['semana', 'week']):
            sql = f"SELECT Fecha_de_inicio, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Fecha_de_inicio >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY Fecha_de_inicio ORDER BY Fecha_de_inicio DESC"
            query_type = "fecha"
            
        elif any(word in user_query for word in ['mensajes', 'messages']):
            sql = f"SELECT ID, Nick_del_Cliente, Mensajes, Mensajes_Enviados, Mensajes_Recibidos FROM `{TABLE_ID}` WHERE Mensajes > 0 ORDER BY Mensajes DESC LIMIT 10"
            
        elif any(word in user_query for word in ['clarita', 'cliente']):
            sql = f"SELECT * FROM `{TABLE_ID}` WHERE LOWER(Nick_del_Cliente) LIKE '%clarita%' ORDER BY Fecha_de_inicio DESC LIMIT 10"
            
        elif any(word in user_query for word in ['abordaje', 'sla']):
            sql = f"SELECT Abordado_en_SLA, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Abordado_en_SLA IS NOT NULL GROUP BY Abordado_en_SLA"
            
        elif any(word in user_query for word in ['últimos', 'recientes', 'latest']):
            sql = f"SELECT ID, Nick_del_Cliente, Estado, Canal, Fecha_de_inicio, Hora_de_inicio FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT 10"
            
        else:
            sql = f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Fecha_de_inicio FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC LIMIT 10"
        
        print(f"Ejecutando SQL: {sql}")
        
        # Ejecutar consulta BigQuery
        results = bq_client.query(sql).to_dataframe()
        
        # Generar gráfico
        chart_data = generate_chart(results, query_type)
        
        # Procesar con Gemini
        model = genai.GenerativeModel('gemini-1.5-flash')
        data_summary = results.head(5).to_string() if len(results) > 0 else "No hay datos"
        
        prompt = f"""
        Usuario pregunta sobre tickets de Adereso: {user_query}
        Datos encontrados: {data_summary}
        Total de registros: {len(results)}
        
        Responde en español de forma conversacional y profesional. 
        Si hay datos específicos como estados, canales, o clientes, menciónalos.
        Si hay números o estadísticas, resáltalos.
        Habla como un asistente de atención al cliente de Adereso.
        """
        
        response = model.generate_content(prompt)
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "data_count": len(results)
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({
            "text": f"Error consultando datos: {str(e)}", 
            "chart": None
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
