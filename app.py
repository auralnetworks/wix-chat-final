from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile
import re

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
    return {"status": "Backend Adereso - Optimizado con Identifiers"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test"], "values": [100]}
    })

def generate_static_sql(user_query):
    """Genera SQL con patrones estáticos (sin usar Gemini)"""
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
    
    # Búsquedas específicas
    if 'chat' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE LOWER(Canal) LIKE '%chat%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
    
    if 'whatsapp' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE LOWER(Canal) LIKE '%whatsapp%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
    
    if 'clarita' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE LOWER(Nick_del_Cliente) LIKE '%clarita%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
    
    if 'hoy' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio = CURRENT_DATE() ORDER BY Hora_de_inicio DESC"
    
    if 'ayer' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ORDER BY Hora_de_inicio DESC"
    
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
        
        return f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT {limit}"
    
    return None

def generate_comparative_sql(user_query):
    """Genera SQL para comparativos sin usar Gemini"""
    query = user_query.lower()
    
    # Comparativo ayer vs hoy
    if any(word in query for word in ['ayer', 'hoy', 'versus', 'vs', 'comparar']):
        if 'whatsapp' in query:
            return """
            SELECT 
                CASE 
                    WHEN Fecha_de_inicio = CURRENT_DATE() THEN 'Hoy'
                    WHEN Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN 'Ayer'
                END as periodo,
                COUNT(*) as cantidad
            FROM `esval-435215.webhooks.Adereso_WebhookTests` 
            WHERE Canal = 'Whatsapp' 
            AND Fecha_de_inicio IN (CURRENT_DATE(), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            GROUP BY periodo
            ORDER BY periodo DESC
            """
        else:
            return """
            SELECT 
                CASE 
                    WHEN Fecha_de_inicio = CURRENT_DATE() THEN 'Hoy'
                    WHEN Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN 'Ayer'
                END as periodo,
                COUNT(*) as cantidad
            FROM `esval-435215.webhooks.Adereso_WebhookTests` 
            WHERE Fecha_de_inicio IN (CURRENT_DATE(), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            GROUP BY periodo
            ORDER BY periodo DESC
            """
    
    if 'hora' in query and 'hoy' in query:
        return """
        SELECT 
            EXTRACT(HOUR FROM PARSE_TIME('%H:%M:%S', Hora_de_inicio)) as hora,
            COUNT(*) as cantidad
        FROM `esval-435215.webhooks.Adereso_WebhookTests` 
        WHERE Fecha_de_inicio = CURRENT_DATE() 
        AND Hora_de_inicio IS NOT NULL
        GROUP BY hora 
        ORDER BY hora
        """
    
    return None

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando Gemini (último recurso)"""
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        sql_prompt = f"""
        Genera SQL para `{TABLE_ID}` basado en: "{user_query}"
        
        REGLAS:
        1. NO uses LIMIT a menos que el usuario pida números específicos
        2. Para conteos usa COUNT(*) sin LIMIT
        3. Incluye siempre Identifier en SELECT cuando sea posible
        4. Solo devuelve la consulta SQL
        
        SQL:
        """
        
        response = model.generate_content(sql_prompt)
        sql = response.text.strip()
        
        # Limpiar respuesta
        sql = re.sub(r'```sql\n?', '', sql)
        sql = re.sub(r'```\n?', '', sql)
        sql = sql.strip()
        
        # Validación de seguridad
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql.upper() for keyword in dangerous_keywords):
            return None
        
        return sql
        
    except Exception as e:
        print(f"Error con Gemini: {e}")
        return None

def generate_chart_from_results(results):
    """Genera gráfico basado en los resultados usando Identifier"""
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
    
    # Para tickets individuales, usar Identifier si está disponible
    if 'Identifier' in results.columns:
        identifiers = results['Identifier'].head(20).fillna('Sin ID').tolist()
        
        # Si hay columna numérica, usarla para los valores
        numeric_cols = results.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 0:
            col = numeric_cols[0]
            return {
                "labels": identifiers,
                "values": results[col].head(20).fillna(0).tolist()
            }
        else:
            # Si no hay columna numérica, usar índice como valor
            return {
                "labels": identifiers,
                "values": list(range(1, len(identifiers) + 1))
            }
    
    # Si hay ID pero no Identifier
    elif 'ID' in results.columns:
        ids = results['ID'].head(20).astype(str).tolist()
        
        numeric_cols = results.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 0:
            col = numeric_cols[0]
            return {
                "labels": ids,
                "values": results[col].head(20).fillna(0).tolist()
            }
        else:
            return {
                "labels": ids,
                "values": list(range(1, len(ids) + 1))
            }
    
    # Fallback para otros casos
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

def generate_smart_response(user_query, results, sql):
    """Genera respuesta inteligente sin usar Gemini"""
    query = user_query.lower()
    total = len(results)
    
    if total == 0:
        return "No se encontraron registros para tu consulta."
    
    # Respuestas para comparativos
    if 'periodo' in results.columns:
        hoy_data = results[results['periodo'] == 'Hoy']
        ayer_data = results[results['periodo'] == 'Ayer']
        
        hoy_count = int(hoy_data['cantidad'].iloc[0]) if len(hoy_data) > 0 else 0
        ayer_count = int(ayer_data['cantidad'].iloc[0]) if len(ayer_data) > 0 else 0
        
        diferencia = hoy_count - ayer_count
        porcentaje = ((diferencia / ayer_count) * 100) if ayer_count > 0 else 0
        
        canal_text = "de WhatsApp " if 'whatsapp' in query else ""
        
        if diferencia > 0:
            return f"📈 **Comparativo Ayer vs Hoy** {canal_text}\n\n**Hoy**: {hoy_count:,} tickets\n**Ayer**: {ayer_count:,} tickets\n\n✅ **Incremento**: +{diferencia:,} tickets ({porcentaje:+.1f}%)\n\nHoy ha sido un día más activo que ayer."
        elif diferencia < 0:
            return f"📉 **Comparativo Ayer vs Hoy** {canal_text}\n\n**Hoy**: {hoy_count:,} tickets\n**Ayer**: {ayer_count:,} tickets\n\n📉 **Disminución**: {diferencia:,} tickets ({porcentaje:.1f}%)\n\nHoy ha sido un día menos activo que ayer."
        else:
            return f"📊 **Comparativo Ayer vs Hoy** {canal_text}\n\n**Hoy**: {hoy_count:,} tickets\n**Ayer**: {ayer_count:,} tickets\n\n➡️ **Sin cambios**: Misma cantidad de tickets ambos días."
    
    # Respuestas para análisis por canal
    if 'Canal' in results.columns:
        top_canal = results.iloc[0]['Canal']
        top_count = int(results.iloc[0]['cantidad'])
        total_canales = len(results)
        
        return f"📱 **Análisis por Canal**\n\nSe encontraron **{total_canales}** canales activos:\n\n🥇 **Canal líder**: {top_canal} con {top_count:,} tickets\n\nDistribución completa mostrada en el gráfico."
    
    # Respuestas para análisis por hora
    if 'hora' in results.columns:
        hora_pico = results.loc[results['cantidad'].idxmax(), 'hora']
        tickets_pico = int(results.loc[results['cantidad'].idxmax(), 'cantidad'])
        
        return f"🕐 **Análisis por Hora - Hoy**\n\n⏰ **Hora pico**: {int(hora_pico)}:00 hrs con {tickets_pico} tickets\n\nDistribución horaria completa en el gráfico. Total analizado: {results['cantidad'].sum():,} tickets de hoy."
    
    # Respuestas específicas por canal
    if 'chat' in query:
        return f"💬 **Tickets de Chat**\n\nSe encontraron **{total:,}** tickets de chat. Los datos incluyen información completa con Identifiers, mensajes enviados/recibidos y fechas."
    
    if 'whatsapp' in query:
        return f"📱 **Tickets de WhatsApp**\n\nSe encontraron **{total:,}** tickets de WhatsApp. Información completa disponible con Identifiers y estadísticas de mensajes."
    
    if 'clarita' in query:
        return f"👤 **Tickets de Clarita**\n\nSe encontraron **{total:,}** tickets relacionados con Clarita. Los datos incluyen información completa de cada ticket con Identifiers únicos."
    
    # Respuesta genérica
    return f"Consulta ejecutada exitosamente. Se encontraron **{total:,}** registros en total. Los gráficos muestran los Identifiers reales de los tickets."

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        
        print(f"Consulta del usuario: {user_query}")
        
        # PASO 1: Intentar con patrones estáticos (sin cuota)
        sql = generate_static_sql(user_query)
        
        # PASO 2: Intentar con comparativos (sin cuota)
        if not sql:
            sql = generate_comparative_sql(user_query)
        
        # PASO 3: Solo usar Gemini como último recurso
        if not sql:
            print("Usando Gemini como último recurso...")
            sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "No pude procesar tu consulta. Intenta con: 'tickets de hoy vs ayer', 'tickets por canal', 'tickets de WhatsApp', 'últimos 10 tickets'",
                "chart": None
            }), 400
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta
        results = bq_client.query(sql).to_dataframe()
        
        print(f"Registros obtenidos: {len(results)}")
        
        # Generar gráfico con Identifiers
        chart_data = generate_chart_from_results(results)
        
        # Generar respuesta SIN usar Gemini (ahorro de cuota)
        response_text = generate_smart_response(user_query, results, sql)
        
        # Para el frontend
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
