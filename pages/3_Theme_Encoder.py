import streamlit as st
import pandas as pd
import openai
import json
from io import StringIO

# ---------- 1. Define the helper function for calling GPT with a function schema ----------

def get_themes_for_text(
    text: str, 
    themebook: pd.DataFrame, 
    openai_api_key: str, 
    model_name: str = "gpt-4o-mini"
):
    """
    Sends the input text + theme definitions to OpenAI, requests a structured JSON 
    with label, value, and justification for each theme.
    """
    openai.api_key = openai_api_key
    
    # Define the function schema
    function_schema = {
        "name": "extract_themes_from_text",
        "description": "Given a text, identify whether each theme in the theme book applies. Return 0 or 1, plus a justification.",
        "parameters": {
            "type": "object",
            "properties": {
                "themes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {
                                "type": "string", 
                                "description": "The theme label from the theme book."
                            },
                            "value": {
                                "type": "integer", 
                                "description": "1 if theme is present, 0 if not."
                            },
                            "justification": {
                                "type": "string", 
                                "description": "A brief explanation of why the theme was assigned 0 or 1."
                            }
                        },
                        "required": ["label", "value", "justification"]
                    }
                }
            },
            "required": ["themes"]
        }
    }

    # Build a user-friendly message enumerating the themes and definitions
    # so GPT knows what to look for
    theme_lines = []
    for _, row in themebook.iterrows():
        theme_label = str(row["Theme"])
        theme_def = str(row["Definition"])
        theme_lines.append(f"- {theme_label}: {theme_def}")
    themes_str = "\n".join(theme_lines)

    user_message = f"""
Text to analyze:
{text}

You have a theme book containing:
{themes_str}

Return JSON in this structure:
{{
  "themes": [
    {{
      "label": "<ThemeLabel>",
      "value": 0 or 1,
      "justification": "Short reason"
    }},
    ...
  ]
}}
Make sure to include each theme from the theme book exactly once.
"""

    # Prepare the messages for GPT
    messages = [
        {"role": "system", "content": "You are a helpful theme identification assistant."},
        {"role": "user", "content": user_message}
    ]

    # Call ChatCompletion with a function call request
    response = openai.chat.completions.create(
        model=model_name,
        messages=messages,
        tools=[
            {
                "type": "function",
                "function": function_schema
            }
        ],
        tool_choice={"type": "function", "function": {"name": "extract_themes_from_text"}}
    )

    # If GPT returns the JSON via a function call, parse it
    
    function_args = response.choices[0].message.tool_calls[0].function.arguments
    try:
        parsed = json.loads(function_args)
        # Should have a "themes" list
        return parsed.get("themes", [])
    except:
        return []


# ---------- 2. Define a helper function to apply the theme-coding across your entire dataset ----------

def theme_code_entire_dataframe(
    df: pd.DataFrame, 
    themebook: pd.DataFrame, 
    openai_api_key: str
) -> pd.DataFrame:
    """
    For each text cell in df, calls get_themes_for_text(...) to retrieve 0/1 + justification,
    then populates new columns for each theme and its justification.
    """

    coded_df = df.copy()

    # Get the unique themes from the themebook
    theme_labels = themebook["Theme"].tolist()

    # Pre-create columns for each theme + justification
    # so we have columns to store the 0/1 values and short text
    for theme in theme_labels:
        # If these columns already exist in df, you might rename them to avoid clashes
        coded_df[theme] = 0
        coded_df[f"{theme}_justification"] = ""

    # For each row/col in the original data, request theme presence from GPT
    for row_idx in range(len(coded_df)):
        for col_name in df.columns:
            # We only code original columns. (Exclude newly added theme columns.)
            if col_name in theme_labels or col_name.endswith("_justification"):
                continue

            cell_value = str(coded_df.iat[row_idx, coded_df.columns.get_loc(col_name)])
            if not cell_value.strip():
                # Skip empty cells
                continue

            # Call GPT to get the theme presence
            themes_result = get_themes_for_text(cell_value, themebook, openai_api_key)
            # Each item is: { "label": "...", "value": 0 or 1, "justification": "..." }
            for t_obj in themes_result:
                label = t_obj.get("label", "").strip()
                val = t_obj.get("value", 0)
                just = t_obj.get("justification", "").strip()

                # Store these values if label is recognized
                if label in theme_labels:
                    coded_df.at[row_idx, label] = val
                    coded_df.at[row_idx, f"{label}_justification"] = just

    return coded_df


# ---------- 3. Define the main Streamlit app ----------

def main():
    st.title("Theme-Based Coder")

    # Step 1. Ask for API Key
    api_key = st.text_input("Enter your OpenAI API Key", type="password")
    if not api_key:
        st.warning("Please enter an API key to proceed.")
        return

    # Step 2. Upload Theme Book CSV
    theme_file = st.file_uploader("Upload your Theme Book CSV (with columns: Theme, Definition)")
    if theme_file is not None:
        df_themebook = pd.read_csv(theme_file)
        st.write("### Theme Book Preview")
        st.dataframe(df_themebook, use_container_width=True)

        # Step 3. Upload Data CSV
        data_file = st.file_uploader("Upload the CSV data you want to theme-code")
        if data_file is not None:
            df_data = pd.read_csv(data_file)
            st.write("### Uploaded Data (Preview)")
            st.dataframe(df_data, use_container_width=True)

            # Step 4. Code the Data
            if st.button("Code Data"):
                with st.spinner("Coding data..."):
                    coded_df = theme_code_entire_dataframe(df_data, df_themebook, api_key)
                st.success("Data coded successfully!")
                st.write("### Coded DataFrame")
                st.dataframe(coded_df, use_container_width=True)

                # Let user download the coded data
                csv_buffer_coded = StringIO()
                coded_df.to_csv(csv_buffer_coded, index=False)
                st.download_button(
                    label="Download Coded CSV",
                    data=csv_buffer_coded.getvalue(),
                    file_name="coded_data.csv",
                    mime="text/csv"
                )

    else:
        st.info("Please upload a Theme Book CSV to begin.")


if __name__ == "__main__":
    main()
