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
        
        # Generar SQL inteligente con Gemini
        sql = None
        gemini_worked = False
        
        try:
            print(f"🤖 Llamando a Gemini para: {user_query}")
            
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            prompt = f"""
            Eres experto en SQL y BigQuery. Genera SQL para tabla `{TABLE_ID}` basada en: "{user_query}"
            
            CAMPOS DISPONIBLES:
            Identifier, Estado, Canal, Fecha_de_inicio, Mensajes, Sentimiento_Inicial, Escalado, 
            Tipificacion_Bot, Menu_inicial, Tiempo_de_Abordaje__Segundos_, Nick_del_Cliente
            
            EJEMPLOS INTELIGENTES:
            - "whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 15
            - "últimos tickets" → SELECT Identifier, Estado, Canal, Fecha_de_inicio FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC LIMIT 15
            - "tickets por whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 15
            - "hoy" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE DATE(Fecha_de_inicio) = CURRENT_DATE() LIMIT 15
            - "total" → SELECT COUNT(*) as total FROM `{TABLE_ID}`
            - "por canal" → SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Canal ORDER BY cantidad DESC LIMIT 10
            - "escalados" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE Escalado = 'true' LIMIT 15
            
            REGLAS CRÍTICAS:
            1. SIEMPRE usar LIMIT 15 máximo
            2. Para fechas usar DATE(Fecha_de_inicio)
            3. Para conteos usar COUNT(*) as cantidad
            4. Para ordenar usar ORDER BY
            5. Solo SQL válido, sin explicaciones
            
            SQL:
            """
            
            response = model.generate_content(prompt)
            
            if response and response.text:
                sql = response.text.strip()
                print(f"📝 Respuesta cruda: {sql}")
                
                # Limpiar respuesta
                sql = sql.replace('```sql', '').replace('```', '').replace('`', '').strip()
                
                # Validar que sea SQL válido
                if 'SELECT' in sql.upper() and len(sql) > 20:
                    # Forzar LIMIT si no existe
                    if 'LIMIT' not in sql.upper() and 'COUNT(' not in sql.upper():
                        sql += ' LIMIT 15'
                    
                    # Validar seguridad
                    dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER']
                    if not any(word in sql.upper() for word in dangerous):
                        gemini_worked = True
                        print(f"✅ Gemini SQL exitoso: {sql}")
                    else:
                        print(f"❌ SQL peligroso rechazado")
                        sql = None
                else:
                    print(f"❌ SQL inválido: {sql}")
                    sql = None
            else:
                print("❌ Gemini no devolvió respuesta")
                
        except Exception as e:
            print(f"❌ Error Gemini completo: {e}")
            sql = None
        
        # SQL por defecto si Gemini falla
        if not sql:
            print("🔧 Usando SQL por defecto")
            if 'whatsapp' in user_query.lower():
                sql = f"SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 10"
            elif 'últimos' in user_query.lower() or 'recientes' in user_query.lower():
                sql = f"SELECT Identifier, Estado, Canal, Fecha_de_inicio FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC LIMIT 10"
            else:
                sql = f"SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC LIMIT 10"
        
        # Ejecutar consulta
        query_job = bq_client.query(sql)
        results = query_job.result()
        
        # Procesar resultados de forma inteligente
        rows = []
        columns = [field.name for field in results.schema]
        print(f"📊 Columnas encontradas: {columns}")
        
        for row in results:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = str(value) if value is not None else "N/A"
            rows.append(row_dict)
        
        # Generar respuesta inteligente y consistente
        if len(rows) == 0:
            text = "No se encontraron resultados para tu consulta"
            chart = {"labels": ["Sin datos"], "values": [0]}
            tickets = []
        else:
            # Respuestas inteligentes según tipo de consulta
            if 'cantidad' in columns:
                total = sum(int(float(str(row.get('cantidad', 0)))) for row in rows)
                text = f"📈 Análisis completado: {len(rows)} categorías encontradas con {total} registros totales"
                chart = {
                    "labels": [str(row.get(columns[0], 'N/A')) for row in rows[:8]],
                    "values": [int(float(str(row.get('cantidad', 0)))) for row in rows[:8]]
                }
                tickets = []
            elif 'total' in columns:
                total = int(float(str(rows[0].get('total', 0)))) if rows else 0
                text = f"📊 Total de registros en la base de datos: {total:,}"
                chart = {"labels": ["Total de Tickets"], "values": [total]}
                tickets = []
            else:
                # Para consultas de tickets individuales
                if 'whatsapp' in user_query.lower():
                    whatsapp_count = len([r for r in rows if 'whatsapp' in str(r.get('Canal', '')).lower()])
                    text = f"📱 Encontrados {len(rows)} tickets de WhatsApp de un total consultado"
                elif 'últimos' in user_query.lower() or 'recientes' in user_query.lower():
                    text = f"🕰️ Últimos {len(rows)} tickets ordenados por fecha más reciente"
                else:
                    text = f"🎫 Se encontraron {len(rows)} tickets que coinciden con tu consulta"
                
                chart = {"labels": ["Tickets Encontrados"], "values": [len(rows)]}
                
                # Generar tickets con más información
                tickets = []
                for row in rows[:8]:
                    ticket = {
                        "id": row.get('Identifier', 'N/A'),
                        "estado": row.get('Estado', 'N/A'),
                        "canal": row.get('Canal', 'N/A'),
                        "fecha": row.get('Fecha_de_inicio', 'N/A')[:10] if row.get('Fecha_de_inicio') else 'N/A'
                    }
                    tickets.append(ticket)
        
        response_data = {
            "text": text,
            "chart": chart,
            "tickets": tickets
        }
        
        print(f"✅ Respuesta generada: {text}")
        print(f"📊 Gemini funcionó: {gemini_worked}")
        print(f"📈 Tickets: {len(tickets)}, Registros: {len(rows)}")
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ Error completo: {e}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        return jsonify({
            "text": f"❌ Error del sistema: {str(e)[:100]}",
            "chart": {"labels": ["Error"], "values": [0]},
            "tickets": []
        })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
