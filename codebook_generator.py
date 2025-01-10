import json


def generate_function_call_schema(codes):
    properties = {code: {"type": "boolean", "description": f"Indicates if {code} is present"} for code in codes}
    schema = {
        "name": "add_qualitative_data_codes",
        "description": "The function takes all the qualitative data codes (true or false) and adds them to a file for code analysis.",
        "strict": True,
        "parameters": {
            "type": "object",
            "required": ["codes"],
            "properties": {
                "codes": {
                    "type": "object",
                    "description": "A dictionary of themes/codes with booleans representing their presence in the input text",
                    "properties": properties,
                    "additionalProperties": False,
                    "required": codes
                }
            },
            "additionalProperties": False
        }
    }
    return schema
    

