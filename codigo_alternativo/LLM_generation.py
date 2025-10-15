import os
import pandas as pd
import boto3
import json
from trp import Document
from PyPDF2 import PdfReader, PdfWriter
from pydantic import BaseModel
from typing import List
from openai import OpenAI
import time
import logging
import requests



schema = {
    "type": "object",
    "properties": {
        "All_Requirements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "Requirement": {
                        "type": "string",
                        "description": "The text of the compliance requirement, extracted as close to the original text as possible while maintaining concision"
                    },
                    "Environmental_Component": {
                        "type": "string",
                        "enum": [
                            "Aire", "Agua", "Suelo", "Ruido", "Luz", 
                            "Residuos Líquidos", "Residuos Sólidos", "Residuos Peligrosos",
                            "Sustancias Peligrosas", "Electricidad", "Combustibles",
                            "Flora y Vegetación", "Fauna", "Patrimonio Cultural",
                            "Particularidades Mineras", "Social", "Laboral",
                            "Cambio Climático", "Uso de la Tierra", "Carreteras", "Otros"
                        ],
                        "description": "The primary environmental component this obligation is trying to protect"
                    },
                    "Project_Phase": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "Previo a la construcción",
                                "Construcción",
                                "Operación",
                                "Cierre",
                                "Posterior al cierre"
                            ]
                        },
                        "description": "One or more project phases when this requirement applies"
                    },
                    "Independence": {
                        "type": "string",
                        "enum": [
                            "Obligación Principal",
                            "Obligación Secundaria"
                        ],
                        "description": "Whether this is a primary obligation or a secondary obligation that supports another requirement"
                    },
                    "Result": {
                        "type": "string",
                        "enum": [
                            "Obligación de medios",
                            "Obligación de resultados"
                        ],
                        "description": "Whether the requirement specifies a process to follow (medios) or a specific outcome to achieve (resultados)"
                    }
                },
                "required": [
                    "Requirement",
                    "Environmental_Component",
                    "Project_Phase",
                    "Independence",
                    "Result"
                ]
            }
        }
    },
    "required": ["All_Requirements"]
}

# Example valid outputs:
example1 = {
    "All_Requirements": [
        {
            "Requirement": "Incorporar y mantener sistema de pulverización en los equipos de la planta clasificadora de muros, además de humectar las áreas de trabajo cuando las condiciones ambientales lo requieran.",
            "Environmental_Component": "Aire",
            "Project_Phase": ["Construcción", "Operación"],
            "Independence": "Obligación Principal",
            "Result": "Obligación de medios"
        },
        {
            "Requirement": "Registros de mantenciones y humectaciones.",
            "Environmental_Component": "Aire",
            "Project_Phase": ["Construcción", "Operación"], 
            "Independence": "Obligación Secundaria",
            "Result": "Obligación de medios"
        }
    ]
}

example2 = {
    "All_Requirements": [
        {
            "Requirement": "No se superarán los niveles máximos establecidos por este Decreto.",
            "Environmental_Component": "Ruido",
            "Project_Phase": ["Construcción", "Operación"],
            "Independence": "Obligación Principal",
            "Result": "Obligación de resultados"
        }
    ]
}


prompt = """
Aquí está el texto para tu tarea:

"""


system_message = f"""
Tarea: Se le entregará un texto de un documento de revisión ambiental para un proyecto de inversión. Su objetivo es extraer todos los requisitos de cumplimiento contenidos en el texto. Haga coincidir el texto devuelto del requisito lo más cerca posible del texto de entrada, pero sin perder la concisión. Puede haber cero o más requisitos de cumplimiento.

Para cada requisito de cumplimiento identificado, también identificará las siguientes características:

1. Componente ambiental: Componente del medio ambiente que el requisito de cumplimiento busca proteger. Puede ser uno de los siguientes valores: Aire, Agua, Suelo, Ruido, Luz, Residuos líquidos, Residuos sólidos, Residuos peligrosos, Sustancias peligrosas, Electricidad, Combustibles, Flora y vegetación, Fauna, Patrimonio cultural, Específicos de la minería, Social, Laboral, Cambio climático, Uso del suelo, Carreteras, Otros.

2. Fase Proyecto: Fase del proyecto a la que pertenece el requisito de cumplimiento. Puede ser uno o más de los siguientes valores:
Previo a la construcción, Construcción, Operación, Cierre, Posterior al cierre

3. Independencia: Grado de independencia de la obligación. Si el requisito de cumplimiento está diseñado para asegurar el cumplimiento de otro requisito más amplio, es una "Obligación Secundaria". Si es un requisito de cumplimiento que se mantiene de manera independiente, es una "Obligación Principal". Puede ser uno de los siguientes valores:
Obligación Secundaria u Obligación Principal.

4. Resultado: Resultado requerido para cumplir con la obligación. Si el requisito de cumplimiento impone un estándar de conducta, no un resultado predeterminado, es una "Obligación de medios". Si impone el cumplimiento de un resultado predeterminado, como un umbral de desempeño, es una "Obligación de resultados". Puede ser uno de los siguientes valores:
Obligación de medios u Obligación de resultados


Para cada requisito que identifiques, debes clasificarlo según el siguiente esquema:

{json.dumps(schema, indent=2)}

Ejemplo 1:
Texto de entrada: 
7.1.2. Sistemas de pulverización | de la planta clasificadora de muros.
Tipo de medida | Mitigación.
Componente(s) ambiental(es) objeto de protección | Riesgo para la salud de la población.
Impacto asociado | Calidad del aire.
Objetivo, descripción y justificación | Objetivo: Minimizar las emisiones de material particulado por clasificación y proceso de chancado para material de muros del DR. Descripción: Incorporar y mantener sistema de pulverización en los equipos de la planta clasificadora de muros, además de humectar las áreas de trabajo cuando las condiciones ambientales lo requieran. Justificación: Minimizar las emisiones de material particulado por clasificación y proceso de chancado para material de muros de DR.
Lugar, forma y oportunidad de implementación | Lugar: En la planta clasificadora para material de muros del DR. Forma: Incorporar y mantener sistema de pulverización en los equipos de la planta clasificadora de muros. Oportunidad: Durante las fases de construcción y operación, mientras la planta clasificadora se encuentre operativa.
Indicador de cumplimiento | - Se realizarán mantenciones de los sistemas de control de polvo en los equipos de harnero y chancado móvil de la Planta, de acuerdo con los requisitos establecidos por el Proveedor. - Se aplicará humectación en las áreas de trabajo de la Planta, previa inspección de las condiciones ambientales, con frecuencia diaria. Se deberá mantener los registros de estas mantenciones y humectaciones en la faena.
Referencia al ICE para mayores detalles | Ver numeral 8.2 del ICE del proyecto.
| Se deberán mantener los registros de estas mantenciones y humectaciones en la faena.
Referencia al ICE para mayores detalles | Ver numeral 8.2 del ICE del proyecto.
Salida:
{json.dumps(example1, indent=2)}

Ejemplo 2:
Texto de entrada: 
10.3. COMPONENTE/MATERIA: | Ruido.
NORMAL | Decreto Supremo N° 38/2012 del Ministerio del Medio Ambiente. Establece norma de emisión de ruidos generados por fuentes que indica, elaborada a partir de la revisión del Decreto Supremo N°146/97, MINSEGPRES.
Fase del Proyecto a la que aplica o en la que se dará cumplimiento | Todas las fases.
Forma de cumplimiento | No se superarán los niveles máximos establecidos por este Decreto.
Indicador que acredita su cumplimiento | Resultados de monitoreos o seguimientos que acreditan el cumplimiento de la norma.
Forma de control y seguimiento | No se superarán los niveles máximos establecidos por este Decreto.
Salida: 
{json.dumps(example2, indent=2)}


Devuelva siempre un JSON válido que coincida con este esquema. Si no se encuentran requisitos, devuelva "None" y ningún otro texto.
"""


system_message_CE = f"""
Tarea: Se le entregará una tabla de un documento de revisión ambiental para un proyecto de inversión. Su objetivo es resumir la acción o medida principal prescrita en la tabla. Generalmente será una combinación de información en el título de la tabla y la fila titulada "Acciones o medidas a implementar". 
Devuelve sólo la respuesta sin ningún otro texto.

Ejemplo 1:
Texto de entrada:
10.2.1. Emergencias
Fase del proyecto a la que aplica | Operacion
Parte, obra o acción asociada | De acuerdo a lo indicado en la punto 10.1 Plan de Prevencion de Contingencias, numeral 10.1.1 Contingencias de esta Resolución.
Acciones o medidas a implementar | Las indicadas en el punto 10.1 Plan de Prevencion de Contingencias numeral 10.1.1 Contingencias de esta Resolución.
Forma de control y seguimiento | Las indicadas en el punto 10.1 Plan de Prevencion de Contingencias numeral 10.1.1 Contingencias de esta Resolución.
Salida:
Las indicadas en el punto 10.1 Plan de Prevencion de Contingencias numeral 10.1.1 Contingencias de esta Resolución.

Si no se encuentran acciones o medidas prescritas, devuelva "None" y ningún otro texto.
"""


system_message_PAS = """
Tarea: Se le entregará una tabla de un documento de revisión ambiental para un proyecto de inversión. Su objetivo es devolver

1. El título de la tabla sin el número de tabla.
2. Un resumen de los elementos de la fila con respecto a las condiciones o exigencias específicas para su otorgamiento. Si no hay condiciones o exigencias, devuelva "None".

Ejemplo 1:
Texto de entrada:
Tabla 9.1.6. Permiso para la 294 del Decreto con Fuerza de del artículo 155 del reglamento | construcción de ciertas obras hidráulicas, establecido en el artículo Ley N° 1.122, de 1981, del Ministerio de Justicia, Código de Aguas, del SEIA.
Fase del proyecto a la cual corresponde | Construcción y operación.
Parte, obra o acción a la que aplica | Para la modificación del depósito de relaves. Para mayor detalle, ver numeral 3.2 y Anexo N° 3-B, ambos de la Adenda complementaria del EIA y numeral 3.2 de la Adenda excepcional del EIA.
Condiciones o exigencias específicas para su otorgamiento | No existen condiciones o exigencias asociadas a este permiso.
Pronunciamiento del órgano competente | La Comisión de Evaluación, se pronunció conforme respecto de los requisitos entregados por el titular y otorgó favorablemente el permiso ambiental sectorial contenido en el artículo 155 del RSEIA.
Salida:
{
"Title": "Permiso para la 294 del Decreto con Fuerza de del artículo 155 del reglamento construcción de ciertas obras hidráulicas, establecido en el artículo Ley N° 1.122, de 1981, del Ministerio de Justicia, Código de Aguas, del SEIA.",
"Conditions": "None"
}


Ejemplo 2:
Texto de entrada:
Tabla 9.1.4. Permiso para la planta de tratamiento de basuras lugar destinado a la acumulación, basuras y desperdicios de cualquier | construcción, reparación, modificación y ampliación de cualquier y desperdicios de cualquier clase o para la instalación de todo selección, industrialización, comercio o disposición final de clase, del artículo 140 del reglamento del SEIA.
Fase del proyecto a la cual corresponde | Todas las fases.
Parte, obra o acción a la que aplica | El proyecto no aumentará el número de trabajadores aprobado en la RCA N°165/2018. Por tal motivo, la ejecución del proyecto no generará nuevos o mayores cantidades de residuos sólidos no peligrosos, los que serán dispuestos en las instalaciones evaluadas y aprobadas en los Proyectos originales. Sin embargo, para ampliar las alternativas de manejo de los residuos sólidos no peligrosos y para dar flexibilidad al sistema, se habilitará un sitio de disposición final de residuos sólidos no peligrosos (RESCON). Para mayor detalle, ver numeral 3.1 y Anexo N° 3-A, ambos de la Adenda excepcional del EIA.
Condiciones o exigencias específicas para su otorgamiento | Las observaciones de la Secretaría Regional Ministerial de Salud de la Región de Antofagasta deberán ser incorporadas por el titular y presentadas en los antecedentes del PAS 140 de forma sectorial y en forma previa al inicio de la operación disposición final de residuos sólidos no peligrosos (RESCON).
Pronunciamiento del órgano competente | La Comisión de Evaluación, se pronunció conforme respecto de los requisitos entregados por el titular y otorgó favorablemente el permiso ambiental sectorial contenido en el artículo 140 del RSEIA.
Salida:
{
"Title": "Permiso para la 294 del Decreto con Fuerza de del artículo 155 del reglamento construcción de ciertas obras hidráulicas, establecido en el artículo Ley N° 1.122, de 1981, del Ministerio de Justicia, Código de Aguas, del SEIA.",
"Conditions": "Las observaciones de la Secretaría Regional Ministerial de Salud de la Región de Antofagasta deben ser incluidas por el responsable y presentadas en los antecedentes del PAS 140, de manera sectorial, antes de comenzar la operación de disposición final de residuos sólidos no peligrosos (RESCON)."
}


Solo devuelve JSON válido sin ningún otro texto. Si no se encuentran requisitos, devuelva "None"
"""


class ComplianceRequirement(BaseModel):
    Requirement: str
    Environmental_Component: str
    Project_Phase: List[str]
    Independence: str
    Result: str

class Response(BaseModel):
    All_Requirements: List[ComplianceRequirement]

def get_LLM_response_general(prompt, system_message, client):
    time.sleep(1)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": system_message
            },
            {
            "role": "user",
            "content": prompt
            }
        ],
        temperature=0,
        max_tokens=10000
    )
    
    json_string = response.choices[0].message.content

    # Handle extra characters GPT tends to add
    if json_string.startswith('```json'):
        json_string = json_string[7:]
    elif json_string.startswith('```'):
        json_string = json_string[3:]
    if json_string.endswith('```'):
        json_string = json_string[:-3]

    if json_string.lower() == 'none':
        return []

    # Parse and validate the response using Pydantic
    try:
        validated_response = Response.model_validate_json(json_string)
        return validated_response
    except Exception as e:
        print(f"Error parsing LLM response: {e}")
        print(f"Raw response: {json_string}")
        return []
    
def get_LLM_response_PAS(prompt, system_message_PAS, client):
    time.sleep(1)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": system_message_PAS
            },
            {
            "role": "user",
            "content": prompt
            }
        ],
        temperature=0,
        max_tokens=10000
    )
    
    json_string = response.choices[0].message.content
    try:
        json_obj = json.loads(json_string)
        return json_obj['Title'], json_obj['Conditions']
    except:
        print("PAS prompt response error:")
        print(json_string)
        return None, None

def get_LLM_response_CE(prompt, system_message_CE, client):
    # Add delay to avoid rate limiting
    time.sleep(1)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": system_message_CE
            },
            {
            "role": "user",
            "content": prompt
            }
        ],
        temperature=0,
        max_tokens=10000
    )

    resp = response.choices[0].message.content

    if resp.lower() == 'none':
        return []
    
    return resp



def get_obligation_row(text_block, section, logger, RCA_name):
    print(section)
    os.environ['OPENAI_API_KEY'] = 'key'
    
    # Use the OpenAI client with this session
    from openai import OpenAI
    client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

    new_rows = pd.DataFrame(columns=[
        'Obligation', 'Numeral', 'Section', 'EnvironmentalComponent',
        'ProjectPhase', 'Independence', 'Result', 'Conditions'
    ])

    text = text_block['Text']
    if len(text.split()) < 5:
        return pd.DataFrame()
        
    full_prompt = "Aquí está el texto para tu tarea:\n\n" + text

    if section == "Permisos Ambientales Sectoriales":
        tries = 0
        while tries < 1:
            try:
                ob, cond = get_LLM_response_PAS(full_prompt, system_message_PAS, client)
                break
            except Exception as e:
                tries += 1
                if tries == 1:
                    logger.error(f"{RCA_name} CHUNK FAILED - {str(e)}")
                    ob, cond = None, None
        if ob:
            new_row = pd.DataFrame({
                'Obligation': [ob],
                'Numeral': [text_block['numeral']],
                'Section': [section],
                'EnvironmentalComponent': ["NA"],
                'ProjectPhase': ["NA"],
                'Independence': ["NA"],
                'Result': ["NA"],
                'Conditions': [cond]
            })
            new_rows = pd.concat([new_rows, new_row], ignore_index=True)

    elif section == "Contingencias y Emergencias":
        tries = 0
        while tries < 2:
            try:
                resp = get_LLM_response_CE(full_prompt, system_message_CE, client)
                break
            except Exception as e:
                tries += 1
                if tries == 1:
                    logger.error(f"{RCA_name} CHUNK FAILED - {str(e)}")
                    resp = []  
        if resp != []:
            new_row = pd.DataFrame({
                'Obligation': [resp],
                'Numeral': [text_block['numeral']],
                'Section': [section],
                'EnvironmentalComponent': ["NA"],
                'ProjectPhase': ["NA"],
                'Independence': ["NA"],
                'Result': ["NA"],
                'Conditions': ["NA"]
            })
            new_rows = pd.concat([new_rows, new_row], ignore_index=True)

        
    else:
        response = get_LLM_response_general(full_prompt, system_message, client)

        tries = 0
        while tries < 2:
            try:
                response = get_LLM_response_general(full_prompt, system_message, client)
                break
            except Exception as e:
                tries += 1
                if tries == 2:
                    logger.error(f"{RCA_name} CHUNK FAILED - {str(e)}")
                    response = []
        if response != []:
            for item in response.All_Requirements:
                new_row = pd.DataFrame({
                    'Obligation': [item.Requirement],
                    'Numeral': [text_block['numeral']],
                    'Section': [section],
                    'EnvironmentalComponent': [item.Environmental_Component],
                    'ProjectPhase': [item.Project_Phase],
                    'Independence': [item.Independence],
                    'Result': [item.Result],
                    'Conditions': ["NA"]
                })
                new_rows = pd.concat([new_rows, new_row], ignore_index=True)

    
    try:
        return new_rows
    except Exception as e:
        logger.error(f"{RCA_name} test4 CHUNK FAILED - {str(e)}")
        print(section)
        return pd.DataFrame()
        