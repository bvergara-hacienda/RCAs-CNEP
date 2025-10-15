import os
import pandas as pd
import numpy as np
import boto3
import json

# Modify trp imports
try:
    from trp.t_pipeline import pipeline_merge_tables
except ImportError:
    pipeline_merge_tables = None

try:
    import trp.trp2 as t2
except ImportError:
    t2 = None

from textractcaller.t_call import call_textract, Textract_Features
from textractprettyprinter.t_pretty_print import (
    Textract_Pretty_Print, 
    get_string, 
    get_tables_string, 
    Pretty_Print_Table_Format, 
    convert_table_to_list
)

# Modify these imports to be more robust
try:
    from trp.trp2 import TDocument, TDocumentSchema
except ImportError:
    TDocument = None
    TDocumentSchema = None

try:
    from trp.t_tables import MergeOptions, HeaderFooterType
except ImportError:
    MergeOptions = None
    HeaderFooterType = None

try:
    from trp import Document
except ImportError:
    Document = None

import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from pydantic import BaseModel
from typing import List
from openai import OpenAI
import time
import logging
from thefuzz import fuzz

os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'  

# Local imports
from identify_sections import (
    get_all_paragraphs_tables,
    find_section_markers_from_textract,
    get_section_paragraphs_tables,
    process_tables,
    section_headers,
    relevant_sections
)
from LLM_generation import (
    get_LLM_response_PAS,
    get_LLM_response_CE,
    get_LLM_response_general,
    system_message_PAS,
    system_message_CE,
    system_message,
    get_obligation_row
)
from textract_querying import (
    get_textract_json,
    upload_pdf_to_s3_uri
)


def run_pipeline(RCA_name, pdf_folder, out_folder, logger):

    pdf_fp = pdf_folder + RCA_name + ".pdf"
    textract_json = get_textract_json(pdf_fp)

    all_paragraphs, all_tables = get_all_paragraphs_tables(textract_json)
    all_tables = process_tables(all_tables)
    relevant_markers, markers = find_section_markers_from_textract(all_paragraphs, section_headers, relevant_sections)

    out_df = pd.DataFrame(columns=[
        'Obligation', 'Numeral', 'Section', 'EnvironmentalComponent',
        'ProjectPhase', 'Independence', 'Result', 'Conditions'
    ])

    for m in relevant_markers:
        section_paragraphs, section_tables = get_section_paragraphs_tables(all_paragraphs, all_tables, m)
        all_texts = section_paragraphs + section_tables

        for block in all_texts:
            new_row = get_obligation_row(block, m['section'], logger, RCA_name)
            if not new_row.empty:
                out_df = pd.concat([out_df, new_row], ignore_index=True)

    out_df.to_excel(out_folder + f"{RCA_name}_output.xlsx", index=False)
    print(f"Saved {RCA_name}")


def process_single_file(i, pdf_folder, out_folder, logger):
    start_time = time.time()
    RCA_name = f"B{i}"
    
    try:
        run_pipeline(RCA_name, pdf_folder, out_folder, logger)
        processing_time = time.time() - start_time
        logger.info(f"{i},{RCA_name},{processing_time:.2f}")
    except Exception as e:
        logger.error(f"{i},{RCA_name},FAILED - {str(e)}")

def main():

    pdf_folder = r"C:/Users/bverg/OneDrive - Ministerio de Hacienda/B Vergara/IA Uso Interno/RCA/RCAs/mineria/"
    out_folder = r"C:/Users/bverg/OneDrive - Ministerio de Hacienda/B Vergara/IA Uso Interno/RCA/sam/"
    # Set up logging
    log_file = "test_batch_log_10_2.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will print to console as well
        ]
    )
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("openai").setLevel(logging.CRITICAL)
    logger = logging.getLogger(__name__)
    logger.info("Iteration,Filename,Processing_Time_Seconds")


    
    pdflist = ['218', '219','220']
        
    for i in pdflist:
        start_time = time.time()
        RCA_name = f"B{i}"

        RCA_pdf_path = f"path/{RCA_name}.pdf"

        if os.path.exists(RCA_pdf_path):

            try:
                run_pipeline(RCA_name, pdf_folder, out_folder, logger)
                processing_time = time.time() - start_time
                logger.info(f"{i},{RCA_name},{processing_time:.2f}")
                
            except Exception as e:
                logger.error(f"{i},{RCA_name},FAILED - {str(e)}")

        #run_pipeline(RCA_name, pdf_folder, out_folder, logger)
        #processing_time = time.time() - start_time
        #logger.info(f"{i},{RCA_name},{processing_time:.2f}")

    """
    from concurrent.futures import ProcessPoolExecutor
    from functools import partial

    # Use up to 4 processes at a time
    with ProcessPoolExecutor(max_workers=5) as executor:
        # Create a partial function with the fixed arguments
        process_func = partial(process_single_file, 
                             pdf_folder=pdf_folder,
                             out_folder=out_folder, 
                             logger=logger)
        
        # Map the function across the range of indices
        list(executor.map(process_func, range(2, 5)))
    """

if __name__ == "__main__":
    main()



