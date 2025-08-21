from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile
import re

app = Flask(__name__)
CORS(app)

# Configuración
PROJECT_ID = "esval-435215"
TABLE_ID = "esval-435215.webhooks.Adereso_WebhookTests"

# Configurar credenciales
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
if creds_json:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name

bq_client = bigquery.Client(project=PROJECT_ID)

@app.route('/')
def home():
    return {"status": "Backend Adereso - SQL Directo (Sin límites Gemini)"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test"], "values": [100]}
    })

def generate_sql_direct(user_query):
    """Genera SQL directamente sin usar Gemini"""
    query = user_query.lower()
    
    # Conteos totales
    if any(word in query for word in ['total', 'cuántos', 'cantidad', 'count']):
        if 'canal' in query:
            return "SELECT Canal, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Canal IS NOT NULL GROUP BY Canal ORDER BY cantidad DESC"
        elif 'estado' in query:
            return "SELECT Estado, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Estado IS NOT NULL GROUP BY Estado ORDER BY cantidad DESC"
        elif 'departamento' in query:
            return "SELECT Departamento, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Departamento IS NOT NULL GROUP BY Departamento ORDER BY cantidad DESC"
        elif 'empresa' in query:
            return "SELECT Empresa, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Empresa IS NOT NULL GROUP BY Empresa ORDER BY cantidad DESC"
        else:
            return "SELECT COUNT(*) as total FROM `esval-435215.webhooks.Adereso_WebhookTests`"
    
    # Búsquedas por cliente
    if 'clarita' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE LOWER(Nick_del_Cliente) LIKE '%clarita%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
    
    # Análisis temporal
    if 'hoy' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio = CURRENT_DATE() ORDER BY Hora_de_inicio DESC"
    
    if 'ayer' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ORDER BY Hora_de_inicio DESC"
    
    if 'semana' in query:
        return "SELECT Fecha_de_inicio as fecha, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY Fecha_de_inicio ORDER BY Fecha_de_inicio DESC"
    
    if 'mes' in query:
        return "SELECT Fecha_de_inicio as fecha, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY Fecha_de_inicio ORDER BY Fecha_de_inicio DESC"
    
    # Análisis por hora
    if any(word in query for word in ['hora', 'horario']):
        return "SELECT EXTRACT(HOUR FROM PARSE_TIME('%H:%M:%S', Hora_de_inicio)) as hora, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Hora_de_inicio IS NOT NULL GROUP BY hora ORDER BY hora"
    
    # Mensajes
    if 'mensaje' in query:
        if 'más' in query or 'mayor' in query:
            return "SELECT ID, Nick_del_Cliente, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Canal, Estado, Departamento, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Mensajes > 0 ORDER BY Mensajes DESC"
        else:
            return "SELECT AVG(Mensajes) as promedio_mensajes, SUM(Mensajes_Enviados) as total_enviados, SUM(Mensajes_Recibidos) as total_recibidos FROM `esval-435215.webhooks.Adereso_WebhookTests`"
    
    # SLA y abordaje
    if 'sla' in query or 'abordaje' in query:
        return "SELECT Abordado_en_SLA, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Abordado_en_SLA IS NOT NULL GROUP BY Abordado_en_SLA"
    
    # Tipificaciones
    if 'tipificacion' in query:
        return "SELECT Tipificaciones, COUNT(*) as cantidad FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Tipificaciones IS NOT NULL GROUP BY Tipificaciones ORDER BY cantidad DESC"
    
    # Últimos registros (con límite solo si se especifica)
    if 'últimos' in query or 'recientes' in query:
        limit = 50
        if '5' in query or 'cinco' in query:
            limit = 5
        elif '10' in query or 'diez' in query:
            limit = 10
        elif '20' in query or 'veinte' in query:
            limit = 20
        elif '100' in query or 'cien' in query:
            limit = 100
        
        return f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Tipificaciones, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT {limit}"
    
    # Consulta por defecto - TODOS los registros
    return "SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"

def generate_chart_from_results(results):
    """Genera gráfico basado en los resultados"""
    if len(results) == 0:
        return None
    
    if 'cantidad' in results.columns:
        return {
            "labels": results.iloc[:, 0].astype(str).tolist()[:15],
            "values": results['cantidad'].tolist()[:15]
        }
    
    if 'total' in results.columns:
        return {
            "labels": ["Total de Tickets"],
            "values": [int(results['total'].iloc[0])]
        }
    
    numeric_cols = results.select_dtypes(include=['int64', 'float64']).columns
    if len(numeric_cols) > 0:
        col = numeric_cols[0]
        return {
            "labels": [f"Registro {i+1}" for i in range(min(20, len(results)))],
            "values": results[col].head(20).fillna(0).tolist()
        }
    
    return {
        "labels": ["Registros Encontrados"],
        "values": [len(results)]
    }

def generate_response_text(user_query, results):
    """Genera respuesta sin usar Gemini"""
    query = user_query.lower()
    total = len(results)
    
    if total == 0:
        return "No se encontraron registros que coincidan con tu consulta."
    
    if 'total' in query or 'cuántos' in query:
        if 'canal' in results.columns:
            top_canal = results.iloc[0]['Canal'] if len(results) > 0 else 'N/A'
            top_count = results.iloc[0]['cantidad'] if len(results) > 0 else 0
            return f"Análisis por canal completado. Se encontraron {total} canales diferentes. El canal con más tickets es '{top_canal}' con {top_count} tickets."
        elif 'Estado' in results.columns:
            top_estado = results.iloc[0]['Estado'] if len(results) > 0 else 'N/A'
            top_count = results.iloc[0]['cantidad'] if len(results) > 0 else 0
            return f"Análisis por estado completado. Se encontraron {total} estados diferentes. El estado más común es '{top_estado}' con {top_count} tickets."
        elif 'total' in results.columns:
            total_tickets = int(results['total'].iloc[0])
            return f"El total de tickets en la base de datos es: {total_tickets:,} registros."
    
    if 'clarita' in query:
        return f"Se encontraron {total} tickets relacionados con Clarita. Los datos incluyen información completa de cada ticket con mensajes, canales y fechas."
    
    if 'hoy' in query:
        return f"Se encontraron {total} tickets creados hoy. Mostrando información completa incluyendo horarios de inicio."
    
    if 'mensaje' in query:
        if 'promedio_mensajes' in results.columns:
            avg_msg = results['promedio_mensajes'].iloc[0]
            total_env = results['total_enviados'].iloc[0]
            total_rec = results['total_recibidos'].iloc[0]
            return f"Estadísticas de mensajes: Promedio por ticket: {avg_msg:.1f}, Total enviados: {total_env:,}, Total recibidos: {total_rec:,}"
        else:
            return f"Se encontraron {total} tickets con mensajes. Ordenados por cantidad de mensajes de mayor a menor."
    
    return f"Consulta ejecutada exitosamente. Se encontraron {total:,} registros en total. Los datos están ordenados por fecha y hora más recientes."

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        
        print(f"Consulta del usuario: {user_query}")
        
        # Generar SQL directamente
        sql = generate_sql_direct(user_query)
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta
        results = bq_client.query(sql).to_dataframe()
        
        print(f"Registros obtenidos: {len(results)}")
        
        # Generar respuesta sin Gemini
        response_text = generate_response_text(user_query, results)
        
        # Generar gráfico
        chart_data = generate_chart_from_results(results)
        
        # Para el frontend, limitar a 50 registros para mostrar
        raw_data = results.head(50).to_dict('records') if len(results) > 0 else []
        
        return jsonify({
            "text": response_text,
            "chart": chart_data,
            "data_count": len(results),
            "raw_data": raw_data,
            "sql_executed": sql,
            "total_records": len(results)
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({
            "text": f"Error ejecutando consulta: {str(e)}",
            "chart": None
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
