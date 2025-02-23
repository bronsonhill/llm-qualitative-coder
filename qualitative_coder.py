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
    
    new_columns = pd.DataFrame(index=df.index)
    
    for column in df.columns:
        codes_per_column = get_column_codes(df[column])
        
        for code_name, binary_values in codes_per_column.items():
            new_column_name = f"{code_name}_{column}" 
            new_columns[new_column_name] = binary_values
    
    df_with_codes = pd.concat([df, new_columns], axis=1)
    return df_with_codes

def get_column_codes(column):
    codes_dict = {}
    
    for data in column:
        row_codes = get_code(data)
        
        for code in row_codes:
            if code not in codes_dict:
                codes_dict[code] = []
            codes_dict[code].append(1)
    
    for code in codes_dict.keys():
        while len(codes_dict[code]) < len(column):
            codes_dict[code].append(0)
    
    return codes_dict

def get_code(text):
    response = client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[
            {
                'role': 'user',
                'content': text
            }
        ],
        tools=[
            {
                "type": "function",
                "function": function_call_schema
            }
        ],
        tool_choice={"type": "function", "function": {"name": "add_qualitative_data_codes"}}
    )

    # Extract and return the list of codes
    return response.choices[0].message.tool_calls[0].function.arguments["codes"]

# Example usage
df_with_codes = get_codes()
df_with_codes.to_csv("survey_data/survey_data_with_codes.csv", index=False)
print(df_with_codes)
