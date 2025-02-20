import streamlit as st
import pandas as pd
import openai
import json
from io import StringIO
from typing import List

CODING_SYSTEM_PROMPT = "You are a helpful assistant that extracts short qualitative codes from text."


def get_codes_for_cell(cell_value: str, openai_api_key: str) -> List[str]:
    """
    Sends cell_value to OpenAI with a 'function call' that returns a list of qualitative codes.
    Returns a list of code strings extracted from the content.
    """
    openai.api_key = openai_api_key
    
    # Example function schema for extracting codes from text
    functions = {
            "name": "extract_codes_from_text",
            "description": "Extract qualitative codes and their definitions from a piece of text.",
            "parameters": {
                "type": "object",
                "properties": {
                    "codes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {"type": "string"},
                                "definition": {"type": "string"}
                            },
                            "required": ["label", "definition"]
                        },
                        "description": "A list of code objects with label and definition."
                    }
                },
                "required": ["codes"]
            }
        }
    
    
    # We craft a basic message to ask GPT for codes
    messages = [
        {
            "role": "system",
            "content": CODING_SYSTEM_PROMPT
        },
        {
            "role": "user",
            "content": f"Extract codes from this text:\n\n{cell_value}\n\n"
                       f"Return each code as an object with 'label' and 'definition'. "
                       f"Short definitions are fine."
        }
    ]
    
    # We explicitly request the function call
    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=[
            {
                "type": "function",
                "function": functions
            }
        ],
        tool_choice={"type": "function", "function": {"name": "extract_codes_from_text"}}
    )
    
    # The assistant should return a JSON payload that matches our function schema
    try:
        function_args = response.choices[0].message.tool_calls[0].function.arguments
        parsed = json.loads(function_args)  # e.g. { "codes": [ { "label": "X", "definition": "Y" }, ...] }
        code_objects = parsed.get("codes", [])
        
    except Exception:
        # If anything unexpected happens, just return an empty list
        st.warning("Failed to extract codes from the text. You may need to restart the process.")
        return []
    
    # Convert objects into a list of labels + dict of definitions
    codes_list = []
    code_definitions = {}
    for obj in code_objects:
        label = obj.get("label", "").strip()
        definition = obj.get("definition", "").strip()
        if label: 
            codes_list.append(label)
            code_definitions[label] = definition

    return codes_list, code_definitions

def code_entire_dataframe(df: pd.DataFrame, openai_api_key: str):
    """
    Iterates over each cell, extracts codes+definitions from OpenAI,
    adds columns for each code, and accumulates the definitions in a codebook.

    Returns:
        coded_df (pd.DataFrame): DataFrame with additional columns <colName>_<codeLabel>.
        codebook_df (pd.DataFrame): DataFrame with columns ['Code', 'Definition'].
    """
    coded_df = df.copy()
    code_definitions_global = {}  # code_label -> definition

    # We'll store new columns in a dict-of-lists to avoid DataFrame fragmentation
    new_cols_dict = {}

    for row_idx in range(len(coded_df)):
        for col_name in coded_df.columns:
            cell_value = str(coded_df.iat[row_idx, coded_df.columns.get_loc(col_name)])
            codes_list, local_definitions = get_codes_for_cell(cell_value, openai_api_key)

            # Update global codebook
            for code_label, definition in local_definitions.items():
                if code_label not in code_definitions_global:
                    code_definitions_global[code_label] = definition

            # Prepare columns for codes
            for code_label in codes_list:
                new_col = f"{col_name}_{code_label}"
                if new_col not in new_cols_dict:
                    new_cols_dict[new_col] = [0]*len(coded_df)
                new_cols_dict[new_col][row_idx] = 1

    # Now that we have all new columns in memory, add them at once
    if new_cols_dict:
        new_cols_df = pd.DataFrame(new_cols_dict)
        coded_df = pd.concat([coded_df, new_cols_df], axis=1)

    # Build the codebook DataFrame
    codebook_records = [{"Code": lbl, "Definition": defn} 
                        for lbl, defn in code_definitions_global.items()]
    codebook_df = pd.DataFrame(codebook_records)

    return coded_df, codebook_df


def main():
    st.title("Qualitative Coder with Codebook")

    # 1. API key
    api_key = st.text_input("Enter your OpenAI API Key", type="password")
    if not api_key:
        st.warning("Please enter an API key to proceed.")
        return

    # 2. Upload CSV
    uploaded_file = st.file_uploader("Upload CSV data")
    if not uploaded_file:
        st.info("Please upload a CSV file.")
        return

    # Read and display the data as a DataFrame
    df = pd.read_csv(uploaded_file).head(3) # for testing
    st.write("### Uploaded Data")
    st.dataframe(df, use_container_width=True)

    # 3. Code the data upon button click
    if st.button("Code Data"):
        with st.spinner("Coding data. Please wait..."):
            coded_df, codebook_df = code_entire_dataframe(df, api_key)

        st.success("Data coded successfully!")
        st.write("### Coded DataFrame")
        st.dataframe(coded_df, use_container_width=True)

        # Display the codebook
        st.write("### Codebook")
        st.dataframe(codebook_df, use_container_width=True)

        # 4. Download buttons
        # For the coded DataFrame
        csv_buffer_coded = StringIO()
        coded_df.to_csv(csv_buffer_coded, index=False)
        st.download_button(
            label="Download Coded CSV",
            data=csv_buffer_coded.getvalue(),
            file_name="coded_data.csv",
            mime="text/csv"
        )

        # For the codebook
        csv_buffer_codebook = StringIO()
        codebook_df.to_csv(csv_buffer_codebook, index=False)
        st.download_button(
            label="Download Codebook CSV",
            data=csv_buffer_codebook.getvalue(),
            file_name="codebook.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()