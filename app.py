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
    return {"status": "Backend Adereso - Respuestas Inteligentes"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Tickets Abiertos", "Tickets Cerrados"], "values": [1250, 3840]}
    })

def generate_chart(results, query_type):
    """Genera gráficos usando Identifiers cuando sea posible"""
    if len(results) == 0:
        return None
    
    if query_type == "count":
        return {
            "labels": ["Total de Tickets"],
            "values": [int(results.iloc[0, 0])]
        }
    elif query_type == "comparativo":
        return {
            "labels": results['periodo'].tolist(),
            "values": results['cantidad'].tolist()
        }
    elif query_type in ["estado", "canal", "departamento", "empresa", "sentimiento"]:
        return {
            "labels": results.iloc[:, 0].tolist()[:10],
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
    else:
        # Para tickets individuales, usar Identifier
        if 'Identifier' in results.columns:
            identifiers = results['Identifier'].head(20).fillna('Sin ID').tolist()
            if 'Mensajes' in results.columns:
                return {
                    "labels": identifiers,
                    "values": results['Mensajes'].head(20).fillna(0).tolist()
                }
            else:
                return {
                    "labels": identifiers,
                    "values": list(range(1, len(identifiers) + 1))
                }
        elif 'Mensajes' in results.columns:
            return {
                "labels": [f"Ticket {i+1}" for i in range(min(20, len(results)))],
                "values": results['Mensajes'].head(20).fillna(0).tolist()
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
        
        # COMPARATIVO HOY VS AYER (NUEVO)
        if any(word in user_query for word in ['hoy', 'ayer']) and any(word in user_query for word in ['vs', 'versus', 'comparar']):
            sql = f"""
            SELECT 
                CASE 
                    WHEN Fecha_de_inicio = CURRENT_DATE() THEN 'Hoy'
                    WHEN Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN 'Ayer'
                END as periodo,
                COUNT(*) as cantidad
            FROM `{TABLE_ID}` 
            WHERE Fecha_de_inicio IN (CURRENT_DATE(), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            GROUP BY periodo
            ORDER BY periodo DESC
            """
            query_type = "comparativo"
            
        # Consultas específicas mejoradas
        elif any(word in user_query for word in ['total', 'count', 'cuántos', 'cantidad']):
            sql = f"SELECT COUNT(*) as total FROM `{TABLE_ID}`"
            query_type = "count"
            
        elif any(word in user_query for word in ['estado', 'status']):
            sql = f"SELECT Estado, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Estado IS NOT NULL GROUP BY Estado ORDER BY cantidad DESC"
            query_type = "estado"
            
        elif any(word in user_query for word in ['canal', 'canales']):
            sql = f"SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Canal IS NOT NULL GROUP BY Canal ORDER BY cantidad DESC"
            query_type = "canal"
            
        elif any(word in user_query for word in ['departamento', 'departamentos']):
            sql = f"SELECT Departamento, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Departamento IS NOT NULL GROUP BY Departamento ORDER BY cantidad DESC"
            query_type = "departamento"
            
        elif any(word in user_query for word in ['empresa', 'empresas']):
            sql = f"SELECT Empresa, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Empresa IS NOT NULL GROUP BY Empresa ORDER BY cantidad DESC"
            query_type = "empresa"
            
        elif any(word in user_query for word in ['hora', 'horas', 'horario']):
            sql = f"SELECT EXTRACT(HOUR FROM PARSE_TIME('%H:%M:%S', Hora_de_inicio)) as hora, COUNT(*) as cantidad FROM `{TABLE_ID}` WHERE Hora_de_inicio IS NOT NULL GROUP BY hora ORDER BY hora"
            query_type = "hora"
            
        elif 'hoy' in user_query:
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE Fecha_de_inicio = CURRENT_DATE() ORDER BY Hora_de_inicio DESC"
            
        elif 'ayer' in user_query:
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE Fecha_de_inicio = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ORDER BY Hora_de_inicio DESC"
            
        elif 'whatsapp' in user_query:
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
            
        elif 'chat' in user_query:
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%chat%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
            
        elif 'clarita' in user_query:
            sql = f"SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` WHERE LOWER(Nick_del_Cliente) LIKE '%clarita%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
            
        elif any(word in user_query for word in ['últimos', 'recientes']):
            limit = 50
            if any(num in user_query for num in ['5', 'cinco']):
                limit = 5
            elif any(num in user_query for num in ['10', 'diez']):
                limit = 10
            elif any(num in user_query for num in ['20', 'veinte']):
                limit = 20
            elif any(num in user_query for num in ['100', 'cien']):
                limit = 100
                
            sql = f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT {limit}"
            
        else:
            sql = f"SELECT ID, Nick_del_Cliente, Estado, Canal, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC LIMIT 100"
        
        print(f"Ejecutando SQL: {sql}")
        
        # Ejecutar consulta BigQuery
        results = bq_client.query(sql).to_dataframe()
        
        # Generar gráfico
        chart_data = generate_chart(results, query_type)
        
        # RESPUESTAS ESPECÍFICAS SIN GEMINI (para ahorrar cuota)
        response_text = ""
        
        if query_type == "comparativo":
            hoy_data = results[results['periodo'] == 'Hoy']
            ayer_data = results[results['periodo'] == 'Ayer']
            
            hoy_count = int(hoy_data['cantidad'].iloc[0]) if len(hoy_data) > 0 else 0
            ayer_count = int(ayer_data['cantidad'].iloc[0]) if len(ayer_data) > 0 else 0
            
            diferencia = hoy_count - ayer_count
            porcentaje = ((diferencia / ayer_count) * 100) if ayer_count > 0 else 0
            
            if diferencia > 0:
                response_text = f"📈 **Comparativo Hoy vs Ayer**\n\n**Hoy**: {hoy_count:,} tickets\n**Ayer**: {ayer_count:,} tickets\n\n✅ **Incremento**: +{diferencia:,} tickets ({porcentaje:+.1f}%)\n\nHoy ha sido más activo que ayer."
            elif diferencia < 0:
                response_text = f"📉 **Comparativo Hoy vs Ayer**\n\n**Hoy**: {hoy_count:,} tickets\n**Ayer**: {ayer_count:,} tickets\n\n📉 **Disminución**: {diferencia:,} tickets ({porcentaje:.1f}%)\n\nHoy ha sido menos activo que ayer."
            else:
                response_text = f"📊 **Comparativo Hoy vs Ayer**\n\n**Hoy**: {hoy_count:,} tickets\n**Ayer**: {ayer_count:,} tickets\n\n➡️ **Sin cambios**: Misma cantidad ambos días."
        
        elif query_type == "count":
            total = int(results['total'].iloc[0])
            response_text = f"📊 **Total de Tickets en la Base de Datos**\n\nSe encontraron **{total:,}** tickets en total en el sistema Adereso."
        
        elif query_type == "canal":
            top_canal = results.iloc[0]['Canal']
            top_count = int(results.iloc[0]['cantidad'])
            total_canales = len(results)
            response_text = f"📱 **Análisis por Canal**\n\nSe encontraron **{total_canales}** canales:\n\n🥇 **Canal líder**: {top_canal} con {top_count:,} tickets\n\nDistribución completa en el gráfico."
        
        elif query_type == "hora":
            hora_pico = results.loc[results['cantidad'].idxmax(), 'hora']
            tickets_pico = int(results.loc[results['cantidad'].idxmax(), 'cantidad'])
            response_text = f"🕐 **Análisis por Hora**\n\n⏰ **Hora pico**: {int(hora_pico)}:00 hrs con {tickets_pico} tickets\n\nDistribución horaria completa en el gráfico."
        
        else:
            # Para otras consultas, usar Gemini con prompt específico
            try:
                model = genai.GenerativeModel('gemini-1.5-flash')
                data_summary = results.head(5).to_string() if len(results) > 0 else "No hay datos"
                
                prompt = f"""
                Usuario pregunta: "{user_query}"
                Datos encontrados: {data_summary}
                Total: {len(results)} registros
                
                Responde específicamente sobre lo que encontraste. 
                Si es sobre WhatsApp, chat, Clarita, o fechas específicas, menciona esos detalles.
                Si hay muchos registros, dilo.
                Sé específico y conversacional como analista de Adereso.
                """
                
                response = model.generate_content(prompt)
                response_text = response.text
                
            except Exception as e:
                print(f"Error con Gemini: {e}")
                response_text = f"Se encontraron **{len(results):,}** registros para tu consulta."
        
        # Preparar datos para el frontend
        raw_data = results.head(20).to_dict('records') if len(results) > 0 else []
        
        return jsonify({
            "text": response_text,
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
