from openai import OpenAI
from dotenv import load_dotenv
import pandas as pd
import os
import json
from data_ingester import ingest_survey_data


load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')

function_call_schema = json.load(open('function_call_schema.json'))

client = OpenAI(api_key=api_key)

def get_codes():
    df = ingest_survey_data()

    for column in df.columns:
        col_codes = get_column_codes(column)
        print(col_codes)

def get_column_codes(column):
    codes = []
    for data in column:
        codes.append(get_code(data))
    return codes

def get_code(text):
    response = client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[
            {
                'role': 'user',
                'content': text
            }
        ],
        tools = [
            {
                "type": "function",
                "function": 
                    function_call_schema
                
            }
        ],
        tool_choice={"type": "function", "function": {"name": "add_qualitative_data_codes"}}
    )

    return response.choices[0].message.tool_calls[0].function.arguments

get_codes()