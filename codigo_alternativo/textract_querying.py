import os
import boto3
import json
from trp.t_pipeline import pipeline_merge_tables
import trp.trp2 as t2
from textractcaller.t_call import call_textract, Textract_Features

def upload_pdf_to_s3_uri(file_path, s3_uri):
    """
    Uploads a PDF file to an S3 location specified by URI
    
    Args:
        file_path (str): Local path to the PDF file
        s3_uri (str): Full S3 URI (e.g., 's3://bucket-name/folder/file.pdf')
    
    Returns:
        bool: True if upload successful, False otherwise
    """
    # Parse S3 URI
    s3_path = s3_uri.replace('s3://', '')
    bucket_name = s3_path.split('/')[0]
    s3_key = '/'.join(s3_path.split('/')[1:])
    

    s3_client = boto3.client('s3',
        aws_access_key_id= 'key',
        aws_secret_access_key= 'key',
        region_name='us-east-2'
    )
    
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Successfully uploaded {file_path} to {s3_uri}")
        return True
        
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

#def get_textract_json(pdf_fp):

#    s3_uri = f"s3://XXX/current_pdf.pdf"
#    upload_pdf_to_s3_uri(pdf_fp, s3_uri)

#    session = boto3.Session(
#        aws_access_key_id=  'key',
#        aws_secret_access_key=  'key',
#        region_name='us-east-2'
#    )
#    textract_client = session.client('textract')
    
#    textract_json = call_textract(
#        input_document=s3_uri,
#        features=[Textract_Features.FORMS, Textract_Features.TABLES, Textract_Features.LAYOUT],
#        boto3_textract_client=textract_client
#    )
#
#    return textract_json


# Create a global counter outside your function
textract_call_counter = {'total': 0, 'pages': 0}

def get_textract_json(pdf_fp):
    # Increment the counter instead of calling the API
    global textract_call_counter
    textract_call_counter['total'] += 1
    
    # Count pages in the PDF to estimate potential costs
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(pdf_fp)
        page_count = len(reader.pages)
        textract_call_counter['pages'] += page_count
        print(f"API call avoided for {pdf_fp}: {page_count} pages")
        print(f"Total avoided API calls: {textract_call_counter['total']}, Total pages: {textract_call_counter['pages']}")
    except Exception as e:
        print(f"Error counting pages: {e}")
    
    # Return dummy data structure that mimics Textract output
    # This is a very simplified version - you might need to enhance it
    # based on what your downstream functions expect
    return {
        "DocumentMetadata": {
            "Pages": page_count
        },
        "Blocks": [
            # Minimal block structure that downstream code might need
            {"BlockType": "PAGE", "Id": "1", "Page": 1}
        ]
    }