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

# Campos disponibles en la tabla
AVAILABLE_FIELDS = [
    "ID", "Fecha_de_inicio", "Hora_de_inicio", "Fecha_de_actualizacion", "Hora_de_actualizacion",
    "Fecha_de_abordaje", "Hora_de_abordaje", "Fecha_de_termino", "Hora_de_termino", "Estado",
    "Detalles_del_estado", "Canal", "Cuenta_s", "Sentimiento_Inicial", "Sentimiento_de_Termino",
    "Tiene_mensajes_publicos", "Tiene_mensajes_privados", "Tiene_ticket_previo", "Respondido",
    "Nick_del_Cliente", "Asignacion_actual", "Primera_asignacion", "Ultima_asignacion",
    "Cantidad_de_asignaciones", "Mensajes", "Mensajes_Enviados", "Mensajes_Recibidos",
    "Texto_del_Primer_Mensaje", "Texto_del_ultimo_Mensaje", "Importante", "Abordado",
    "Abordado_en_SLA", "Tipificado", "Escalado", "Tiempo_de_Abordaje__Segundos_",
    "Segundos_Sin_Asignar", "Proactivo", "Departamento", "Cerrado_Por", "Abordado_Por",
    "Fecha_de_primer_asignacion_humana", "Hora_de_primer_asignacion_humana", "Fecha_de_asignacion",
    "Hora_de_asignacion", "Creado_en_horario_habil", "Tickets_fusionados",
    "Tiempo_asignado_sin_abordaje__Segundos_", "Tiempo_de_abordaje_ejecutivo__Segundos_",
    "Abordado_en_SLA_ejecutivo", "BOT_DERIVATION_Date", "BOT_DERIVATION_Time",
    "Primer_departamento", "Ultimo_departamento", "Prioridad", "Cliente_Principal",
    "Primera_asignacion_humana", "Empresa", "Grupo", "Menu_inicial", "Numero_de_servicio",
    "Tipificaciones", "Tipificaciones_Anidado_1", "Tipificacion_Bot", "Tipificacion_Menu_Clarita",
    "Tipificacion_Sub_Menu_Clarita", "Identifier"
]

@app.route('/')
def home():
    return {"status": "Backend Adereso - Queries Dinámicas con IA"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test"], "values": [100]}
    })

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando IA"""
    
    # Prompt para que Gemini genere SQL
    sql_prompt = f"""
    Eres un experto en SQL y BigQuery. Genera una consulta SQL para la tabla `{TABLE_ID}` basada en esta pregunta del usuario: "{user_query}"

    Campos disponibles: {', '.join(AVAILABLE_FIELDS)}

    REGLAS IMPORTANTES:
    1. SIEMPRE usa el nombre completo de la tabla: `{TABLE_ID}`
    2. Si es una consulta de conteo/agrupación, usa GROUP BY y ORDER BY cantidad DESC
    3. Si pide registros específicos, incluye campos importantes: ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier
    4. Para fechas usa formato YYYY-MM-DD
    5. Para búsquedas de texto usa LOWER() y LIKE '%texto%'
    6. Limita resultados con LIMIT (50-200 según el contexto)
    7. Para análisis temporales usa DATE() y EXTRACT()
    8. SOLO devuelve la consulta SQL, sin explicaciones

    Ejemplos:
    - "tickets de hoy" → SELECT ID, Nick_del_Cliente, Canal, Estado FROM `{TABLE_ID}` WHERE Fecha_de_inicio = CURRENT_DATE()
    - "cuántos por canal" → SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Canal ORDER BY cantidad DESC
    - "tickets con más de 10 mensajes" → SELECT ID, Nick_del_Cliente, Mensajes FROM `{TABLE_ID}` WHERE Mensajes > 10 ORDER BY Mensajes DESC

    Consulta SQL:
    """
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        sql = response.text.strip()
        
        # Limpiar la respuesta (remover markdown, etc.)
        sql = re.sub(r'```sql\n?', '', sql)
        sql = re.sub(r'```\n?', '', sql)
        sql = sql.strip()
        
        # Validación básica de seguridad
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql.upper() for keyword in dangerous_keywords):
            return None
            
        return sql
        
    except Exception as e:
        print(f"Error generando SQL: {e}")
        return None

def generate_chart_from_results(results, user_query):
    """Genera gráfico inteligente basado en los resultados"""
    if len(results) == 0:
        return None
    
    # Si hay una columna 'cantidad', es un análisis agregado
    if 'cantidad' in results.columns:
        return {
            "labels": results.iloc[:, 0].astype(str).tolist()[:15],
            "values": results['cantidad'].tolist()[:15]
        }
    
    # Si hay columnas numéricas, usar la primera
    numeric_cols = results.select_dtypes(include=['int64', 'float64']).columns
    if len(numeric_cols) > 0:
        col = numeric_cols[0]
        return {
            "labels": [f"Registro {i+1}" for i in range(min(20, len(results)))],
            "values": results[col].head(20).fillna(0).tolist()
        }
    
    # Fallback: mostrar conteo
    return {
        "labels": ["Registros Encontrados"],
        "values": [len(results)]
    }

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        
        print(f"Consulta del usuario: {user_query}")
        
        # Generar SQL dinámicamente
        sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "No pude generar una consulta SQL válida para tu pregunta. ¿Puedes reformularla?",
                "chart": None
            }), 400
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta
        results = bq_client.query(sql).to_dataframe()
        
        # Generar gráfico
        chart_data = generate_chart_from_results(results, user_query)
        
        # Procesar respuesta con Gemini
        model = genai.GenerativeModel('gemini-1.5-flash')
        data_summary = results.head(10).to_string() if len(results) > 0 else "No hay datos"
        
        response_prompt = f"""
        El usuario preguntó: "{user_query}"
        Se ejecutó esta consulta SQL: {sql}
        Resultados obtenidos: {data_summary}
        Total de registros: {len(results)}
        
        Responde en español de forma conversacional y profesional como analista de Adereso.
        Explica los resultados encontrados de manera clara y útil.
        Si hay datos específicos importantes, menciónalos.
        Si hay muchos registros, indica que hay más datos disponibles.
        """
        
        response = model.generate_content(response_prompt)
        
        # Preparar datos para mostrar (máximo 20)
        raw_data = results.head(20).to_dict('records') if len(results) > 0 else []
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "data_count": len(results),
            "raw_data": raw_data,
            "sql_executed": sql  # Para debug
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
