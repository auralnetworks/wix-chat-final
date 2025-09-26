from flask import Flask, request, jsonify
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import tempfile
from datetime import datetime

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

# TODOS LOS CAMPOS DISPONIBLES (como antes)
ALL_FIELDS = """
ID, Fecha_de_inicio, Hora_de_inicio, Fecha_de_actualizacion, Hora_de_actualizacion, 
Fecha_de_abordaje, Hora_de_abordaje, Fecha_de_termino, Hora_de_termino, Estado, 
Detalles_del_estado, Canal, Cuenta_s, Sentimiento_Inicial, Sentimiento_de_Termino, 
Tiene_mensajes_publicos, Tiene_mensajes_privados, Tiene_ticket_previo, Respondido, 
Nick_del_Cliente, Asignacion_actual, Primera_asignacion, Ultima_asignacion, 
Cantidad_de_asignaciones, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, 
Texto_del_Primer_Mensaje, Texto_del_ultimo_Mensaje, Importante, Abordado, 
Abordado_en_SLA, Tipificado, Escalado, Tiempo_de_Abordaje__Segundos_, 
Segundos_Sin_Asignar, Proactivo, Departamento, Cerrado_Por, Abordado_Por, 
Fecha_de_primer_asignacion_humana, Hora_de_primer_asignacion_humana, 
Fecha_de_asignacion, Hora_de_asignacion, Creado_en_horario_habil, 
Tickets_fusionados, Tiempo_asignado_sin_abordaje__Segundos_, 
Tiempo_de_abordaje_ejecutivo__Segundos_, Abordado_en_SLA_ejecutivo, 
BOT_DERIVATION_Date, BOT_DERIVATION_Time, Primer_departamento, 
Ultimo_departamento, Prioridad, Cliente_Principal, Primera_asignacion_humana, 
Empresa, Grupo, Menu_inicial, Numero_de_servicio, Tipificaciones, 
Tipificaciones_Anidado_1, Tipificacion_Bot, Tipificacion_Menu_Clarita, 
Tipificacion_Sub_Menu_Clarita, Identifier
"""

# CORS manual
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
    return {"status": "Backend Bruno Perfecto - Gemini 1.5 Flash"}

@app.route('/api/test', methods=['POST'])
def test():
    return jsonify({
        "text": "✅ Backend Bruno funcionando perfectamente!",
        "chart": {"labels": ["Test"], "values": [100]},
        "tickets": []
    })

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando Gemini 1.5 Flash con TODOS los campos"""
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql_prompt = f"""
    Eres experto en SQL y BigQuery. Genera una consulta SQL para la tabla `{TABLE_ID}` basada en: "{user_query}"

    CONTEXTO ACTUAL:
    - Fecha/Hora actual: {current_time}
    - Tabla: {TABLE_ID}
    
    TODOS LOS CAMPOS DISPONIBLES:
    {ALL_FIELDS}

    EJEMPLOS DE CONSULTAS INTELIGENTES:
    - "mensajes iniciales" → SELECT Identifier, Texto_del_Primer_Mensaje FROM `{TABLE_ID}` LIMIT 20
    - "mensajes finales" → SELECT Identifier, Texto_del_ultimo_Mensaje FROM `{TABLE_ID}` LIMIT 20
    - "tipificaciones bot" → SELECT Tipificacion_Bot, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Tipificacion_Bot
    - "menú inicial" → SELECT Menu_inicial, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Menu_inicial
    - "sentimientos" → SELECT Sentimiento_Inicial, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Sentimiento_Inicial
    - "tiempos de abordaje" → SELECT Identifier, Tiempo_de_Abordaje__Segundos_ FROM `{TABLE_ID}` WHERE Tiempo_de_Abordaje__Segundos_ IS NOT NULL LIMIT 20
    - "tickets escalados" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE Escalado = 'true' OR Escalado = '1' LIMIT 20
    - "tickets por canal" → SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Canal ORDER BY cantidad DESC
    - "whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 20

    REGLAS IMPORTANTES:
    1. Para "hoy" usa: WHERE DATE(Fecha_de_inicio) = CURRENT_DATE()
    2. Para "ayer" usa: WHERE DATE(Fecha_de_inicio) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    3. Para conteos usa COUNT(*) as cantidad
    4. Para búsquedas de texto usa LOWER() y LIKE '%texto%'
    5. Incluye siempre Identifier cuando sea posible
    6. Para campos booleanos usa = 'true' o = '1'
    7. LIMIT 20 máximo para memoria
    8. Para mensajes usa Texto_del_Primer_Mensaje o Texto_del_ultimo_Mensaje

    IMPORTANTE: Solo devuelve la consulta SQL, sin explicaciones ni markdown.

    SQL:
    """
    
    try:
        # Usar gemini-1.5-flash (nuevo motor)
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        sql = response.text.strip()
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        # Validación de seguridad
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql.upper() for keyword in dangerous_keywords):
            return None
        
        # Forzar LIMIT para memoria
        if 'LIMIT' not in sql.upper() and 'COUNT(' not in sql.upper():
            sql += ' LIMIT 20'
        
        print(f"✅ SQL Gemini 1.5: {sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error generando SQL: {e}")
        return None

def generate_chart_with_identifiers(results):
    """Genera gráfico usando Identifiers cuando sea posible (igual que antes)"""
    if len(results) == 0:
        return None
    
    # Para datos agregados (con 'cantidad')
    if 'cantidad' in results.columns:
        return {
            "labels": results.iloc[:, 0].astype(str).tolist()[:15],
            "values": results['cantidad'].tolist()[:15]
        }
    
    # Para conteos totales
    if 'total' in results.columns:
        return {
            "labels": ["Total de Tickets"],
            "values": [int(results['total'].iloc[0])]
        }
    
    # Para tickets individuales con Identifier
    if 'Identifier' in results.columns:
        identifiers = results['Identifier'].head(20).fillna('Sin ID').tolist()
        
        # Usar Mensajes como valor si está disponible
        if 'Mensajes' in results.columns:
            return {
                "labels": identifiers,
                "values": results['Mensajes'].head(20).fillna(0).tolist()
            }
        # Usar tiempos de abordaje si están disponibles
        elif 'Tiempo_de_Abordaje__Segundos_' in results.columns:
            return {
                "labels": identifiers,
                "values": results['Tiempo_de_Abordaje__Segundos_'].head(20).fillna(0).tolist()
            }
        else:
            return {
                "labels": identifiers,
                "values": list(range(1, len(identifiers) + 1))
            }
    
    # Fallback
    return {
        "labels": ["Registros Encontrados"],
        "values": [len(results)]
    }

def should_show_tickets(user_query, results):
    """Determina si mostrar tarjetas de tickets (igual que antes)"""
    query_lower = user_query.lower()
    
    # Mostrar tickets para consultas de detalle
    show_conditions = [
        'últimos' in query_lower,
        'recientes' in query_lower,
        'mostrar' in query_lower,
        'ver' in query_lower,
        'mensaje' in query_lower and ('inicial' in query_lower or 'final' in query_lower),
        'escalado' in query_lower,
        'fusionado' in query_lower
    ]
    
    # No mostrar para agregaciones
    aggregate_conditions = [
        'total' in query_lower,
        'cuántos' in query_lower,
        'por canal' in query_lower,
        'por estado' in query_lower,
        'count' in query_lower.replace('cuántos', '')
    ]
    
    return any(show_conditions) and not any(aggregate_conditions) and len(results) <= 25

def generate_tickets_data(results, user_query):
    """Genera tarjetas de tickets con todos los campos relevantes (igual que antes)"""
    if not should_show_tickets(user_query, results):
        return []
    
    tickets = []
    for _, row in results.head(15).iterrows():
        ticket = {}
        
        # Campos principales
        if 'ID' in row and pd.notna(row['ID']):
            ticket['id'] = str(row['ID'])
        if 'Identifier' in row and pd.notna(row['Identifier']):
            ticket['identifier'] = str(row['Identifier'])
        if 'Canal' in row and pd.notna(row['Canal']):
            ticket['canal'] = str(row['Canal'])
        if 'Estado' in row and pd.notna(row['Estado']):
            ticket['estado'] = str(row['Estado'])
        if 'Departamento' in row and pd.notna(row['Departamento']):
            ticket['departamento'] = str(row['Departamento'])
        
        # Mensajes
        if 'Mensajes' in row and pd.notna(row['Mensajes']):
            ticket['mensajes'] = str(row['Mensajes'])
        if 'Texto_del_Primer_Mensaje' in row and pd.notna(row['Texto_del_Primer_Mensaje']):
            ticket['primer_mensaje'] = str(row['Texto_del_Primer_Mensaje'])[:100] + "..."
        if 'Texto_del_ultimo_Mensaje' in row and pd.notna(row['Texto_del_ultimo_Mensaje']):
            ticket['ultimo_mensaje'] = str(row['Texto_del_ultimo_Mensaje'])[:100] + "..."
        
        # Tipificaciones
        if 'Tipificaciones' in row and pd.notna(row['Tipificaciones']):
            ticket['tipificaciones'] = str(row['Tipificaciones'])
        if 'Tipificacion_Bot' in row and pd.notna(row['Tipificacion_Bot']):
            ticket['tipificacion_bot'] = str(row['Tipificacion_Bot'])
        if 'Menu_inicial' in row and pd.notna(row['Menu_inicial']):
            ticket['menu_inicial'] = str(row['Menu_inicial'])
        
        # Sentimientos y estados
        if 'Sentimiento_Inicial' in row and pd.notna(row['Sentimiento_Inicial']):
            ticket['sentimiento'] = str(row['Sentimiento_Inicial'])
        if 'Escalado' in row and pd.notna(row['Escalado']):
            ticket['escalado'] = str(row['Escalado'])
        
        if len(ticket) > 1:
            tickets.append(ticket)
    
    return tickets

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[{current_time}] Consulta: {user_query}")
        
        # SIEMPRE generar SQL con Gemini para respuestas dinámicas
        sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "No pude generar una consulta SQL válida. ¿Puedes reformular tu pregunta?",
                "chart": None,
                "tickets": []
            }), 400
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta en tiempo real con límite de memoria
        query_job = bq_client.query(sql)
        results = query_job.result(max_results=25)  # Límite para memoria
        results = results.to_dataframe()
        
        print(f"Registros obtenidos: {len(results)} a las {current_time}")
        
        # Generar gráfico con Identifiers
        chart_data = generate_chart_with_identifiers(results)
        
        # Generar tickets
        tickets_data = generate_tickets_data(results, user_query)
        
        # SIEMPRE usar Gemini para respuestas dinámicas y específicas
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Preparar contexto rico para Gemini
        data_sample = results.head(5).to_string() if len(results) > 0 else "No hay datos"
        
        response_prompt = f"""
        CONSULTA DEL USUARIO: "{user_query}"
        TIMESTAMP ACTUAL: {current_time}
        TOTAL DE REGISTROS: {len(results)}
        
        MUESTRA DE DATOS:
        {data_sample}
        
        INSTRUCCIONES:
        - Responde como Bruno, analista experto de Smart Reports en tiempo real
        - Sé específico con los números y datos encontrados
        - Si son mensajes, tipificaciones, sentimientos, etc., explica qué muestran
        - Si hay patrones interesantes, menciónalos
        - Usa emojis para hacer la respuesta más visual
        - Responde en español de forma conversacional y profesional
        - Máximo 2-3 líneas
        
        RESPUESTA:
        """
        
        response = model.generate_content(response_prompt)
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "tickets": tickets_data,
            "data_count": len(results),
            "timestamp": current_time
        })
        
    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_time}] Error: {str(e)}")
        return jsonify({
            "text": f"Error consultando datos: {str(e)}",
            "chart": None,
            "tickets": [],
            "timestamp": error_time
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
