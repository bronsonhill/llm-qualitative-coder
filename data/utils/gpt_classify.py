import pandas as pd
import openai
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data.models.Thesis import Thesis
from data.db import engine
from dotenv import load_dotenv
import os
from itertools import islice

MODEL_NAME = "gpt-4o"
SUBSET_SIZE = 20

def classify_theses():
    load_dotenv()
    openai.api_key = os.getenv('OPENAI_API_KEY')
    theses = get_thesis_records(SUBSET_SIZE)

    results = {}
    for thesis in theses:
        print(f"Classifying thesis: {thesis.link}")
        categories = classify_thesis(thesis.text)
        results[thesis.link] = categories
    return results

def get_thesis_records(count=1000):
    # Database connection setup
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Query the database to get the thesis records
        thesis_records = session.query(Thesis).limit(count).all()
        return thesis_records
    except Exception as e:
        print(f"Error fetching thesis records: {e}")
        return []
    finally:
        session.close()

def chunk_dict(data, chunk_size=33):
    """Split dictionary into smaller chunks."""
    it = iter(data.items())
    for i in range(0, len(data), chunk_size):
        yield dict(islice(it, chunk_size))

def get_classifier_function_schema(categories_dict):
    """Creates schema for a subset of categories."""
    function_schema = {
        "name": "extract_investment_categories_from_thesis",
        "description": "Given a thesis, identify whether each investment approach in the list applies. Return true or false, plus a justification.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "categories": {
                    "type": "object",
                    "description": "A dictionary of categories with booleans representing their presence in the input text",
                    "properties": categories_dict,
                    "additionalProperties": False,
                    "required": list(categories_dict.keys())
                }
            },
            "required": ["categories"],
            "additionalProperties": False
        }
    }
    return function_schema

def get_all_categories(category_book_path='data/utils/category_book.csv'):
    """Get all categories from the category book."""
    category_book = pd.read_csv(category_book_path)
    categories = {}
    for index, row in category_book.iterrows():
        categories[row['Label']] = {
            "type": "object",
            "properties": {
                "value": {
                    "type": "boolean",
                    "description": f"Indicates if {row['Label']} approach is present in the thesis, described as: {row['Description']}"
                },
                "justification": {
                    "type": "string",
                    "description": f"A brief explanation of why the {row['Label']} category was assigned true or false, using quotes or references to the thesis text."
                }
            },
            "required": ["value", "justification"],
            "additionalProperties": False
        }
    return categories

def classify_thesis(thesis_text):
    all_categories = get_all_categories()
    all_results = {"categories": {}}

    for chunk in chunk_dict(all_categories, 30):
        print(f"Classifying chunk: {chunk.keys()}")
        messages = [
            {"role": "system", "content": "You are a helpful investment thesis categorisation assistant."},
            {"role": "user", "content": f"Provide categories for the following investment thesis: {thesis_text}"}
        ]

        response = openai.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            tools=[
                {
                    "type": "function",
                    "function": get_classifier_function_schema(chunk)
                }
            ],
            tool_choice={"type": "function", "function": {"name": "extract_investment_categories_from_thesis"}}
        )

        try:
            chunk_result = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
            all_results["categories"].update(chunk_result["categories"])
        except Exception as e:
            print(f"Error processing chunk: {e}")

    return all_results["categories"]

# Example usage
if __name__ == "__main__":
    res = classify_theses()
    with open('classification_results.json', 'w') as f:
        json.dump(res, f, indent=4)