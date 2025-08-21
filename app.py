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

# Campos disponibles
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
    return {"status": "Backend Adereso - Híbrido (Dinámico + Estático)"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Tickets Abiertos", "Tickets Cerrados"], "values": [1250, 3840]}
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
    
    # Más patrones estáticos...
    if 'clarita' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Fecha_de_inicio, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE LOWER(Nick_del_Cliente) LIKE '%clarita%' ORDER BY Fecha_de_inicio DESC, Hora_de_inicio DESC"
    
    if 'hoy' in query:
        return "SELECT ID, Nick_del_Cliente, Canal, Estado, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Hora_de_inicio, Departamento, Empresa, Identifier FROM `esval-435215.webhooks.Adereso_WebhookTests` WHERE Fecha_de_inicio = CURRENT_DATE() ORDER BY Hora_de_inicio DESC"
    
    return None  # No encontró patrón estático

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando Gemini (solo si es necesario)"""
    sql_prompt = f"""
    Genera SQL para `{TABLE_ID}` basado en: "{user_query}"
    
    Campos: {', '.join(AVAILABLE_FIELDS)}
    
    REGLAS:
    1. NO uses LIMIT a menos que el usuario pida números específicos
    2. Para conteos usa COUNT(*) sin LIMIT
    3. Para búsquedas de texto usa LOWER() y LIKE
    4. Solo devuelve la consulta SQL, sin explicaciones
    
    SQL:
    """
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
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

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        
        print(f"Consulta del usuario: {user_query}")
        
        # PASO 1: Intentar con patrones estáticos (sin usar cuota)
        sql = generate_static_sql(user_query)
        
        # PASO 2: Si no hay patrón estático, usar Gemini
        if not sql:
            print("No hay patrón estático, usando Gemini...")
            sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "No pude procesar tu consulta. ¿Puedes reformularla?",
                "chart": None
            }), 400
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta
        results = bq_client.query(sql).to_dataframe()
        
        print(f"Registros obtenidos: {len(results)}")
        
        # Generar gráfico
        chart_data = generate_chart_from_results(results)
        
        # Generar respuesta (intentar con Gemini, fallback a texto simple)
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            data_sample = results.head(5).to_string() if len(results) > 0 else "No hay datos"
            
            response_prompt = f"""
            Usuario preguntó: "{user_query}"
            Total encontrado: {len(results)} registros
            Muestra: {data_sample}
            
            Responde como analista de Adereso. Menciona el total real encontrado.
            """
            
            response = model.generate_content(response_prompt)
            response_text = response.text
            
        except Exception as e:
            print(f"Error con Gemini para respuesta: {e}")
            # Fallback a respuesta simple
            response_text = f"Consulta ejecutada exitosamente. Se encontraron {len(results):,} registros en total."
        
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
