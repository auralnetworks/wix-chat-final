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
    return {"status": "Backend Adereso - Acceso Completo a BigQuery"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Tickets Abiertos", "Tickets Cerrados"], "values": [1250, 3840]}
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
            "labels": results['Estado'].tolist()[:10],
            "values": results['cantidad'].tolist()[:10]
        }
    elif query_type == "canal":
        return {
            "labels": results['Canal'].tolist()[:10],
            "values": results['cantidad'].tolist()[:10]
        }
    elif query_type == "sentimiento":
        return {
            "labels": results['Sentimiento_Inicial'].tolist()[:10],
            "values": results['cantidad'].tolist()[:10]
        }
    elif query_type == "departamento":
        return {
            "labels": results['Departamento'].tolist()[:10],
            "values": results['cantidad'].tolist()[:10]
        }
    elif query_type == "empresa":
        return {
            "labels": results['Empresa'].tolist()[:10],
            "values": results['cantidad'].tolist()[:10]
        }
    elif query_type == "fecha":
        return {
            "labels": results['fecha'].astype(str).tolist()[:15],
            "values": results['cantidad'].tolist()[:15]
        }
    elif query_type == "hora":
        return {
            "labels": [f"{int(h)}:00" for h in results['hora'].tolist()[:24]],
            "values": results['cantidad'].tolist()[:24]
        }
    elif query_type == "tipificacion":
        return {
            "labels": results['Tipificaciones'].tolist()[:8],
            "values": results['cantidad'].tolist()[:8]
        }
    else:
        # Gráfico por defecto con mensajes
        if 'Mensajes' in results.columns:
            return {
                "labels": [f"Ticket {i+1}" for i in range(min(20, len(results)))],
                "values": results['Mensajes'].head(20).fillna(0).tolist()
            }
        elif 'cantidad' in results.columns:
            return {
                "labels": results.iloc[:, 0].astype(str).tolist()[:10],
                "values": results['cantidad'].tolist()[:10]
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
        
        # Consultas inteligentes SIN LÍMITES restrictivos
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
            
        elif any(word in user_query for word in ['departamento', 'departamentos']):
            sql = f"SELECT Departamento, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Departamento IS NOT NULL GROUP BY Departamento ORDER BY cantidad DESC"
            query_type = "departamento"
            
        elif any(word in user_query for word in ['empresa', 'empresas']):
            sql = f"SELECT Empresa, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Empresa IS NOT NULL GROUP BY Empresa ORDER BY cantidad DESC"
            query_type = "empresa"
            
        elif any(word in user_query for word in ['tipificacion', 'tipificaciones']):
            sql = f"SELECT Tipificaciones, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Tipificaciones IS NOT NULL GROUP BY Tipificaciones ORDER BY cantidad DESC LIMIT 20"
            query_type = "tipificacion"
            
        elif any(word in user_query for word in ['hora', 'horas', 'horario']):
            sql = f"SELECT EXTRACT(HOUR FROM PARSE_TIME('%H:%M:%S', Hora_de_inicio)) as hora, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Hora_de_inicio IS NOT NULL GROUP BY hora ORDER BY hora"
            query_type = "hora"
            
        elif any(word in user_query for word in ['prioridad']):
            sql = f"SELECT Prioridad, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Prioridad IS NOT NULL GROUP BY Prioridad ORDER BY cantidad DESC"
            
        elif any(word in user_query for word in ['hoy', 'today']):
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE Fecha_de_inicio = CURRENT_DATE() ORDER BY Hora_de_inicio DESC LIMIT 200"
            
        elif any(word in user_query for word in ['ayer', 'yesterday']):
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ORDER BY Hora_de_inicio DESC LIMIT 200"
            
        elif any(word in user_query for word in ['semana', 'week']):
            sql = f"SELECT Fecha_de_inicio as fecha, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Fecha_de_inicio >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY Fecha_de_inicio ORDER BY Fecha_de_inicio DESC"
            query_type = "fecha"
            
        elif any(word in user_query for word in ['mes', 'month']):
            sql = f"SELECT Fecha_de_inicio as fecha, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Fecha_de_inicio >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY Fecha_de_inicio ORDER BY Fecha_de_inicio DESC"
            query_type = "fecha"
            
        elif any(word in user_query for word in ['mensajes', 'messages']):
            sql = f"SELECT ID, Nick_del_Cliente, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Canal, Estado, Departamento, Identifier FROM `{TABLE_ID}` WHERE Mensajes > 0 ORDER BY Mensajes DESC LIMIT 100"
            
        elif any(word in user_query for word in ['clarita', 'cliente']):
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE LOWER(Nick_del_Cliente) LIKE '%clarita%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT 100"
            
        elif any(word in user_query for word in ['abordaje', 'sla']):
            sql = f"SELECT Abordado_en_SLA, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Abordado_en_SLA IS NOT NULL GROUP BY Abordado_en_SLA"
            
        elif any(word in user_query for word in ['últimos', 'recientes', 'latest']):
            # Detectar cantidad solicitada
            limit = 50
            if any(num in user_query for num in ['100', 'cien']):
                limit = 100
            elif any(num in user_query for num in ['200', 'doscientos']):
                limit = 200
            elif any(num in user_query for num in ['20', 'veinte']):
                limit = 20
            elif any(num in user_query for num in ['5', 'cinco']):
                limit = 5
                
            sql = f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Tipificaciones, Identifier FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT {limit}"
            
        else:
            # Consulta general amplia
            sql = f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT 100"
        
        print(f"Ejecutando SQL: {sql}")
        
        # Ejecutar consulta BigQuery
        results = bq_client.query(sql).to_dataframe()
        
        # Generar gráfico
        chart_data = generate_chart(results, query_type)
        
        # Procesar con Gemini
        model = genai.GenerativeModel('gemini-1.5-flash')
        data_summary = results.head(10).to_string() if len(results) > 0 else "No hay datos"
        
        prompt = f"""
        Usuario pregunta sobre tickets de Adereso: {user_query}
        Datos encontrados: {data_summary}
        Total de registros encontrados: {len(results)}
        
        Responde en español de forma conversacional y profesional. 
        Si hay datos específicos como estados, canales, clientes, fechas, horas, departamentos o empresas, menciónalos.
        Si hay números o estadísticas importantes, resáltalos.
        Si encontraste muchos registros, menciona que hay más datos disponibles.
        Habla como un analista de datos de Adereso que conoce bien el sistema.
        """
        
        response = model.generate_content(prompt)
        
        # Preparar datos para el frontend (máximo 20 tickets para mostrar en cards)
        raw_data = results.head(20).to_dict('records') if len(results) > 0 else []
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "data_count": len(results),
            "raw_data": raw_data
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
