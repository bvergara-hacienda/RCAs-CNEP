import os
import pandas as pd
import numpy as np
import boto3
import json
from trp.t_pipeline import pipeline_merge_tables
import trp.trp2 as t2
from textractcaller.t_call import call_textract, Textract_Features
from textractprettyprinter.t_pretty_print import (
    Textract_Pretty_Print, 
    get_string, 
    get_tables_string, 
    Pretty_Print_Table_Format, 
    convert_table_to_list
)
from trp.trp2 import TDocument, TDocumentSchema
from trp.t_tables import MergeOptions, HeaderFooterType
import io
import re
from trp import Document
#from fuzzywuzzy import fuzz
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from pydantic import BaseModel
from typing import List
from openai import OpenAI
import time
import logging
from thefuzz import fuzz


section_headers = {
    "vistos": ["VISTOS", "VISTOS:"],
    "considerando": ["CONSIDERANDO","CONSIDERANDO:", 
        "Que, en lo relativo a los efectos, características y circunstancias señalados en el artículo 11 de la Ley Nº 19.300",
        "Que, en lo relativo a los efectos, características y circunstancias señalados en el artículo 11 de la Ley Nº19.300",
        'Que en lo relativo a los efectos características y circunstancias señalados en los literales "a","b", "c", "d", "e" y "f" del artículo 11 de la Ley 19.300',
        "Que, en lo relativo a los efectos, características y circunstancias señalados ene l artículo 11 de la Ley 19.300",
    ],
    "Medidas de Mitigacion": [
        "Que, del proceso de evaluación de impacto ambiental del Proyecto puede concluirse que las siguientes medidas de mitigación", 
        "Que, del proceso de evaluación de impacto ambiental del Proyecto puede concluirse que la siguiente medida de mitigación",
        "Que, las medidas de mitigación, reparación y/o compensación asociadas a los efectos, características y circunstancias del artículo 11 de la Ley N° 19.300",
        "Plan de medidas asociado al Medio Humano",
    ],
    "Permisos Ambientales Sectoriales": [
        "Que resultan aplicables al Proyecto los siguientes permisos ambientales sectoriales, asociados a las correspondientes partes, obras o acciones que se señalan a continuación",
        "Que resultan aplicables al Proyecto el siguiente permiso ambiental sectorial",  
        "el proyecto ha obtenido los permisos de carácter ambiental de los siguientes artículos"
        "Que al proyecto le resultan aplicables los permisos ambientales sectoriales de los artículos",
        "Que, al proyecto le es aplicable los siguientes permisos ambientales sectoriales señalados en el Título VII del Reglamento del SEIA",
        "Que al proyecto no le resultan aplicables permisos ambientales sectoriales, asociados a las correspondientes partes, obras o acciones.",
        "Permisos Ambientales Sectoriales de Contenido Únicamente Ambiental",
        "Permisos ambientales sectoriales mixtos", 
        "requiere de los permisos ambientales sectoriales contemplados en los artículos",
        "PERMISOS Y PRONUNCIAMIENTO AMBIENTALES SECTORIALES",
        "PERMISOS AMBIENTALES SECTORIALES",
        "Permiso Ambientales Sectoriales (PAS)",
        "Que tenidos a la vista los antecedentes del proyecto y sus características, se concluye que el proyecto requiere de los permisos ambientales sectoriales que se enlistan en los artículos",
        "Que dada las caracteríscas del proyecto no le son aplicables permisos ambientales sectoriales",
        "El Proyecto no requiere de ningún permiso, ni pronunciamiento ambiental sectorial de acuerdo al Título VII del Párrafo",
    ],
    "Forma de Cumplimiento": [ 
        "Que, de acuerdo a los antecedentes que constan en el expediente de evaluación, la forma de cumplimiento de la normativa de carácter ambiental",
        "Que, de acuerdo con los antecedentes que constan en el expediente de evaluación, la forma de cumplimiento de la normativa de carácter ambiental",
        "Que, de acuerdo con antecedentes que constan en el expediente de evaluación, la forma de cumplimiento de la normativa de carácter ambiental",
        "Que, de acuerdo a los antecedentes que constan en el expediente de evaluación, la forma de cumplimiento de la normava de carácter ambiental",           
        "la forma de cumplimiento de la normativa de carácter ambiental aplicable al Proyecto es la siguiente",
        "la forma de cumplimiento de la normativa de carácter ambiental aplicable al Proyecto",
        "Normas relacionadas con las partes, obras, actividades o acciones, emisiones, residuos y sustancias peligrosas del proyecto",
        "Normas relacionadas al emplazamiento del proyecto",
        # "Componente/materia:", # maybe delete
        "NORMATIVA DE CARÁCTER AMBIENTAL APLICABLE",
        "Que, en relación con el cumplimiento de la normativa ambiental aplicable al proyecto",
        "Normativa Ambiental Sectorial",
    ],
    "Condiciones y Exigencias": [
        'Que, para ejecutar el Proyecto deben cumplirse las siguientes condiciones o exigencias, en concordancia con el artículo 25 de la Ley N° 19.300',
        'Que, durante el procedimiento de evaluación de la DIA se estableció que el Titular del proyecto deberá cumplir con lo siguiente',
        'Que, en uso de sus facultades, la Comisión de Evaluación, ha resuelto imponer al titular, como condiciones o exigencias',
        "Que, para ejecutar el Proyecto se requieren condiciones o exigencias especiales, en concordancia con el artículo 25 de la Ley N° 19.300", 
        "Las condiciones o exigencias para ejecutar el proyecto son las siguientes", 
        "Que, durante el procedimiento de evaluación de la DIA se establecieron condiciones o exigencias adicionales al Proyecto",
        'Durante el proceso de evaluación fueron solicitadas las siguientes exigencias para el desarrollo del Proyecto',
        'Durante el proceso de evaluación fueron solicitadas las siguientes condiciones para el desarrollo del Proyecto',
        "Que, las condiciones o exigencias, en concordancia con el artículo 25 de la Ley N° 19.300, corresponden a",
        'para ejecutar el Proyecto deben cumplirse las siguientes condiciones o exigencias',
        'estableció las siguientes condiciones o exigencias para la aprobación del Proyecto',
        'se establecieron las siguientes condiciones o exigencias al Proyecto',
        ' CONDICIONES Y EXIGENCIAS',
        "pueda ejecutarse, necesariamente deberá cumplir con todas las normas vigentes que le sean aplicables",
    ],
    "Compromisos Voluntarios": [
        "Que, durante el procedimiento de evaluación de la DIA el Titular del Proyecto propuso los siguientes compromisos ambientales voluntarios",
        "Que, durante el procedimiento de evaluación de la DIA el Titular se ha obligado voluntariamente a los siguientes compromisos ambientales",
        "Que, durante el procedimiento de evaluación del EIA el Titular se ha obligado voluntariamente a los siguientes compromisos ambientales",
        "Que, durante el procedimiento de evaluación del EIA el Titular del Proyecto propuso los siguientes compromisos ambientales voluntarios",
        'Que, durante el procedimiento de evaluación de la DIA el Titular del Proyecto propuso el siguiente compromiso ambiental voluntario:',
        "Que, durante el procedimiento de evaluación de la DIA se establecieron los siguientes compromisos ambientales voluntarios", 
        "Que, durante el procedimiento de evaluación del EIA se establecieron los siguientes compromisos ambientales voluntarios", 
        "Que, durante el procedimiento de evaluación el Titular del Proyecto, en cuanto al establecimiento de compromisos ambientales voluntarios, indicó lo siguiente:",
        "el titular se ha comprometido voluntariamente a lo siguiente",
        "Que, el proyecto considera los siguientes compromisos ambientales voluntarios:", 
        "Que, durante el procedimiento de evaluación de la DIA el Titular del Proyecto propuso los siguientes compromisos ambientales voluntarios",
        "Que, para ejecutar el Proyecto deben cumplirse los siguientes compromisos voluntarios, condiciones o exigencias:", 
        "Que, el proyecto contará con los siguientes compromisos voluntarios",
        "El Proyecto contará con el siguiente compromiso voluntario", 
        "9. Compromisos Ambientales Voluntarios",
        "el titular adquirió el siguiente compromiso ambiental voluntario:",
        "COMPROMISOS AMBIENTALES VOLUNTARIOS", "COMPROMISOS VOLUNTARIOS", "Compromiso ambiental voluntario"
    ],  
    "Contingencias y Emergencias": [ 
        "Que, las medidas relevantes del plan de contingencia se detallan acciones a seguir",
        "Que, las medidas relevantes del Plan de Prevención de Contingencias",
        "Que, las medidas o acciones relevantes del plan de prevención de contingencias",
        'Que, el proyecto contará con un "Plan de Prevencion de Contingencias"', 
        "Que, las medidas relevantes del Plan de Prevención de Emergencias",
        ". PLAN DE PREVENCIÓN DE CONTINGENCIAS",
        'Que, respecto del Plan de Prevención de Contingencias',
        "Planes Consolidados de Contingencia y Emergencia", 
        ". PLAN DE EMERGENCIAS",
        "Riesgo o contingencia", 
        "Que, toda vez que ocurra una contingencia, en alguna de las estructuras, partes o sistemas",
        "Que, en caso de emergencias producidas por materiales o sustancia peligrosas, o que puedan afectar, pudiendo ser o no alguna de las establecidas en los planes de contingencia"
    ],
    "Plan de Seguimiento":[
        "Que, el plan de seguimiento de las variables ambientales relevantes que dieron origen al EIA es el siguiente:",
        "Que, el plan de seguimiento de las variables ambientales relevantes que fueron objeto de evaluación ambiental es el siguiente:",
    ],
    "considerandos_otros": [ 
        "Evaluación técnica de las observaciones ciudadanas:",
        "Que, no se solicitó la apertura de proceso de participación ciudadana, conforme a lo dispuesto en el artículo",
        "Que, durante el proceso de participación ciudadana, desarrollado conforme a lo dispuesto en el artículo",
        "Que, durante el proceso de evaluación no hubo proceso de participación ciudadana (PCA), conforme a lo dispuesto en el artículo 30 bis de la Ley N° 19.300.",
        "Que, respecto a la apertura de proceso de participación ciudadana, conforme a lo dispuesto en el artículo",
        "en la presente evaluación no se realizaron reuniones con grupos humanos pertenecientes a pueblos indígenas del artículo 86 del Reglamento del SEIA",
        "durante el proceso de participación ciudadana, desarrollado conforme a lo dispuesto en el artículo", 
        "el Titular deberá remitir a la Superintendencia del Medio Ambiente la información respecto de las condiciones, compromisos o medidas",
        "el Titular deberá informar a la Superintendencia del Medio Ambiente la realización de la gestión, acto o faena mínima que da cuenta del inicio de la ejecución de obras",
        "con el objeto de dar adecuado seguimiento a la ejecución del Proyecto, el Titular deberá informar a la Superintendencia",
        "Síntesis del proceso de participación", 
        "Que, con el objeto de dar adecuado seguimiento a la ejecución del proyecto, el Titular deberá informar a la Dirección Regional del Servicio de Evaluación Ambiental",
        "Que, el Titular deberá remitir a la Superintendencia del Medio Ambiente la información respecto de las condiciones, compromisos o medidas",
    ],
    "resuelvo": ["RESUELVO", "RESUELVO:", "RESUELVE:"]
}



relevant_sections = [
    "Medidas de Mitigacion",
    "Permisos Ambientales Sectoriales",
    "Forma de Cumplimiento",
    "Condiciones y Exigencias",
    "Compromisos Voluntarios",
    "Contingencias y Emergencias",
    "Plan de Seguimiento"
]



def get_all_paragraphs_tables(textract_json):

    def textract_to_dataframe(table_blocks, blocks_map):

        def get_text(cell, blocks_map):
            text = ''
            if 'Relationships' in cell:
                for rel in cell['Relationships']:
                    if rel['Type'] == 'CHILD':
                        for child_id in rel['Ids']:
                            word = blocks_map.get(child_id, {})
                            if word.get('BlockType') == 'WORD':
                                text += word.get('Text', '') + ' '
            return text.strip()
        
        # Initialize a list to store cell information
        cells = []
        
        for table in table_blocks:
            for relationship in table.get('Relationships', []):
                if relationship['Type'] == 'CHILD':
                    for cell_id in relationship['Ids']:
                        cell = blocks_map[cell_id]
                        if cell['BlockType'] == 'CELL':
                            cell_info = {
                                'RowIndex': cell['RowIndex'],
                                'ColumnIndex': cell['ColumnIndex'],
                                'RowSpan': cell.get('RowSpan', 1),
                                'ColumnSpan': cell.get('ColumnSpan', 1),
                                'Text': get_text(cell, blocks_map)
                            }
                            cells.append(cell_info)
            
            # Determine the size of the table
            max_row = max(cell['RowIndex'] + cell['RowSpan'] - 1 for cell in cells)
            max_col = max(cell['ColumnIndex'] + cell['ColumnSpan'] - 1 for cell in cells)
            
            # Initialize an empty DataFrame
            df = pd.DataFrame('', index=range(1, max_row+1), columns=range(1, max_col+1))
            
            # Populate the DataFrame
            for cell in cells:
                for i in range(cell['RowIndex'], cell['RowIndex'] + cell['RowSpan']):
                    for j in range(cell['ColumnIndex'], cell['ColumnIndex'] + cell['ColumnSpan']):
                        if i == cell['RowIndex'] and j == cell['ColumnIndex']:
                            df.at[i, j] = cell['Text']
                        else:
                            df.at[i, j] = None  # Indicate merged area
        
        return df

    blocks_map = {block['Id']: block for block in textract_json['Blocks']}

    table_blocks_map = {}
    for block in textract_json['Blocks']:
        if block['BlockType'] == 'LAYOUT_TABLE':
            table_blocks_map[block['Id']] = block
            if 'Relationships' in block:
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            table_blocks_map[child_id] = blocks_map[child_id]

    footer_blocks_map = {}
    for block in textract_json['Blocks']:
        if block['BlockType'] == 'LAYOUT_FOOTER':
            footer_blocks_map[block['Id']] = block
            if 'Relationships' in block:
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            footer_blocks_map[child_id] = blocks_map[child_id]
    
    layout_blocks_map = {}
    for block in textract_json['Blocks']:
        if block['BlockType'] == 'LAYOUT_TEXT':
            layout_blocks_map[block['Id']] = block
            if 'Relationships' in block:
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            layout_blocks_map[child_id] = blocks_map[child_id]

    multi_line_blocks = [b for b in textract_json['Blocks'] if b['BlockType'] == 'LAYOUT_TEXT'
                         or b['BlockType'] == 'LAYOUT_FOOTER'
                         and b['Id'] not in table_blocks_map]
    for block in multi_line_blocks:
        # Get text from layout block by combining its child lines
        text = ""
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        child_block = blocks_map[child_id]
                        if child_block['BlockType'] == 'LINE':
                            text += child_block['Text'] + " "
        block['Text'] = text.strip()


    single_line_blocks = [b for b in textract_json['Blocks'] if b['BlockType'] == 'LINE' 
                        and b['Id'] not in layout_blocks_map 
                        and b['Id'] not in table_blocks_map]


    all_paragraphs = single_line_blocks + multi_line_blocks
    for b in all_paragraphs:
        b['numeral'] = 'freetext'
    all_paragraphs.sort(key=lambda x: (x['Page'], x['Geometry']['BoundingBox']['Top']))

    all_tables = [block for block in textract_json['Blocks'] if block['BlockType'] == 'TABLE']
    all_tables.sort(key=lambda x: (x['Page'], x['Geometry']['BoundingBox']['Top']))

    for block in all_tables:
        df = textract_to_dataframe([block], blocks_map)
        block['dataframe'] = df

    return all_paragraphs, all_tables


def find_section_markers_from_textract(all_paragraphs, section_headers, relevant_sections):

    markers = {}

    for section, headers in section_headers.items():
        matches = []
        
        for block in all_paragraphs: 
            for header in headers:
                if len(block['Text']) < len(header) * 0.75:
                    continue
                ratio = fuzz.partial_ratio(header, block['Text'])
        
                if ratio >= 85:
                    matches.append({
                        'section': section,
                        'start_page': block['Page'],
                        'start_sentence': block['Text'],
                        'matched_header': header,
                        'start_geometry': block['Geometry'],
                        'ratio': ratio
                    })

        if matches == []:
            continue
        matches.sort(key=lambda x: x['ratio'], reverse=True)
        markers[section] = matches

    chosen_markers = []
    for section, matches in markers.items():
        chosen_markers.append(matches[0])
    chosen_markers.sort(key=lambda x: (x['start_page'], x['start_geometry']['BoundingBox']['Top']))

    # Check if Condiciones y Exigencias is second to last or last section
    while chosen_markers[-1]['section'] == "Condiciones y Exigencias" or chosen_markers[-2]['section'] == "Condiciones y Exigencias":
        if len(markers['Condiciones y Exigencias']) <= 1:
            del markers['Condiciones y Exigencias']
            chosen_markers = []
            for section, matches in markers.items():
                chosen_markers.append(matches[0])
            chosen_markers.sort(key=lambda x: (x['start_page'], x['start_geometry']['BoundingBox']['Top']))
            break
        if len(markers['Condiciones y Exigencias']) > 1:
            markers['Condiciones y Exigencias'].pop(0)
            chosen_markers = []
            for section, matches in markers.items():
                chosen_markers.append(matches[0])
            chosen_markers.sort(key=lambda x: (x['start_page'], x['start_geometry']['BoundingBox']['Top']))

    # Check if Compromisos Voluntarios is first, second, or third section (after vistos, considerandos)
    while chosen_markers[0]['section'] == 'Compromisos Voluntarios' or chosen_markers[1]['section'] == 'Compromisos Voluntarios' or chosen_markers[2]['section'] == 'Compromisos Voluntarios':
        if len(markers['Compromisos Voluntarios']) <= 1:
            del markers['Compromisos Voluntarios']
            chosen_markers = []
            for section, matches in markers.items():
                chosen_markers.append(matches[0])
            chosen_markers.sort(key=lambda x: (x['start_page'], x['start_geometry']['BoundingBox']['Top']))
            break
        if len(markers['Compromisos Voluntarios']) > 1: 
            markers['Compromisos Voluntarios'].pop(0)
            chosen_markers = []
            for section, matches in markers.items():
                chosen_markers.append(matches[0])
            chosen_markers.sort(key=lambda x: (x['start_page'], x['start_geometry']['BoundingBox']['Top']))

        
    # Add end points for each section
    for i, marker in enumerate(chosen_markers):
        if i < len(chosen_markers) - 1:
            marker['end_sentence'] = chosen_markers[i+1]['start_sentence']
            marker['end_page'] = chosen_markers[i+1]['start_page']
            marker['end_geometry'] = chosen_markers[i+1]['start_geometry']
        else:
            marker['end_sentence'] = 'No end sentence found'
            marker['end_page'] = 10000
            marker['end_geometry'] = 10000

    relevant_section_markers = []
    for section in relevant_sections:
        for m in chosen_markers:
            if m['section'] == section:
                relevant_section_markers.append(m)
    
    return relevant_section_markers, chosen_markers





def get_section_paragraphs_tables(all_paragraphs, all_tables, marker):

    section_paragraph_blocks = []
    section_table_blocks = []

    if marker['section'] == "Permisos Ambientales Sectoriales":
        all_paragraphs = []
    else:
        for block in all_paragraphs:

            if block['Page'] > marker['start_page'] and block['Page'] < marker['end_page']:
                block['section'] = marker['section']
                section_paragraph_blocks.append(block)
            
            if block['Page'] == marker['start_page']:
                if block['Geometry']['BoundingBox']['Top'] >= marker['start_geometry']['BoundingBox']['Top']:
                    block['section'] = marker['section']
                    section_paragraph_blocks.append(block)
                        
            if block['Page'] == marker['end_page']:
                if block['Geometry']['BoundingBox']['Top'] < marker['end_geometry']['BoundingBox']['Top']:
                    block['section'] = marker['section']
                    section_paragraph_blocks.append(block)


    for block in all_tables:

        if block['Page'] > marker['start_page'] and block['Page'] < marker['end_page']:
            block['section'] = marker['section']
            section_table_blocks.append(block)
        
        if block['Page'] == marker['start_page']:
            if block['Geometry']['BoundingBox']['Top'] >= marker['start_geometry']['BoundingBox']['Top']:
                block['section'] = marker['section']
                section_table_blocks.append(block)
                    
        if block['Page'] == marker['end_page']:
            if block['Geometry']['BoundingBox']['Top'] < marker['end_geometry']['BoundingBox']['Top']:
                block['section'] = marker['section']
                section_table_blocks.append(block)

    return section_paragraph_blocks, section_table_blocks



def process_tables(all_tables):

    def get_table_number(df):
        if df.empty or df.iloc[0, 0] is None:
            return None
        first_cell = str(df.iloc[0, 0]).strip()

        table_number_pattern = r'(?:Tabla|Table)?\s*(\d+(?:\.\d+)*)\.'
        
        match = re.search(table_number_pattern, first_cell)
        if match:
            return match.group(1)
        else:
            return None

    def combine_tables_splitrow(df0, df1):
        for col in df0.columns:
            df0.iloc[-1, df0.columns.get_loc(col)] += ' ' + df1.iloc[0, df1.columns.get_loc(col)]
        combined_df = pd.concat([df0, df1.iloc[1:]], ignore_index=True)
        return combined_df

    def combine_tables_newrow(df0, df1):
        combined_df = pd.concat([df0, df1], ignore_index=True)
        return combined_df

    def table_to_text(df):

        text_output = []
        
        for row_idx in range(len(df)):
            row_content = [str(df.iloc[row_idx, col_idx]).strip() 
                        for col_idx in range(len(df.columns))]
            text_output.append(" | ".join(row_content))
        
        return "\n".join(text_output)

    # Add first_table field indicating if table is first on its page
    for i in range(len(all_tables)):
        if i == 0:
            all_tables[i]['first_table'] = True
        else:
            all_tables[i]['first_table'] = all_tables[i]['Page'] != all_tables[i-1]['Page']
        all_tables[i]['delete'] = False
        all_tables[i]['numeral'] = get_table_number(all_tables[i]['dataframe'])

    for i in range(1, len(all_tables)):
        # identify tables spanning multiple pages where row is split across pages
        if all_tables[i]['first_table'] and all_tables[i]['dataframe'].iloc[0,0] == "":
            df0 = all_tables[i-1]['dataframe']
            df1 = all_tables[i]['dataframe']
            try:
                combined_df = combine_tables_splitrow(df0, df1)
                all_tables[i-1]['dataframe'] = combined_df
                all_tables[i]['delete'] = True
            except:
                pass

        # identify tables spanning multiple pages where new row begins on next page
        if all_tables[i]['first_table'] and all_tables[i]['numeral'] is None:
            df0 = all_tables[i-1]['dataframe']
            df1 = all_tables[i]['dataframe']
            try:
                combined_df = combine_tables_newrow(df0, df1)
                all_tables[i-1]['dataframe'] = combined_df
                all_tables[i]['delete'] = True
            except:
                pass

    # delete tables that have been linked with previous
    all_tables = [block for block in all_tables if not block['delete']]

    # relabel table numbers
    for i in range(len(all_tables)):
        all_tables[i]['numeral'] = get_table_number(all_tables[i]['dataframe'])
        if all_tables[i]['numeral'] is None:
            all_tables[i]['numeral'] = 'Table numeral not identified'

    # turn all tables into strings
    for i in range(len(all_tables)):
        all_tables[i]['Text'] = table_to_text(all_tables[i]['dataframe'])

    return all_tables


