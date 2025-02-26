import streamlit as st
import pandas as pd
import json
from io import StringIO
import os

# ---------- 1. Define functions to prepare jobs for batch processing ----------

def prepare_theme_job(
    text: str, 
    themebook: pd.DataFrame, 
    model_name: str = "gpt-4o-mini",
    task_id: str = "task-0"
):
    """
    Creates a chat completion job for theme identification without executing it.
    Returns a JSON representation of the job in the required format for batch processing.
    """
    # Define the function schema (same as original)
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
    theme_lines = []
    for _, row in themebook.iterrows():
        theme_label = str(row["theme"])
        theme_def = str(row["definition"])
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

    # Create the job request format in the required format
    job_request = {
        "custom_id": task_id,
        "method": "POST",
        "url": "/chat/completions",
        "body": {
            "model": model_name,
            "messages": messages,
            "tools": [
                {
                    "type": "function",
                    "function": function_schema
                }
            ],
            "tool_choice": {"type": "function", "function": {"name": "extract_themes_from_text"}}
        }
    }
    
    return job_request


def prepare_jobs_for_dataframe(
    df: pd.DataFrame, 
    themebook: pd.DataFrame, 
    model_name: str = "gpt-4o-mini"
) -> list:
    """
    For each text cell in df, prepares a theme coding job for batch processing.
    Returns a list of jobs with the required format for batch processing.
    """
    jobs = []
    
    # Get the unique themes from the themebook
    theme_labels = themebook["theme"].tolist()
    
    # For each row/col in the original data, prepare a theme coding job
    job_count = 0
    for row_idx in range(len(df)):
        for col_name in df.columns:
            # We only code original columns. (Exclude columns that match theme names or justifications)
            if col_name in theme_labels or col_name.endswith("_justification"):
                continue

            cell_value = str(df.iat[row_idx, df.columns.get_loc(col_name)])
            if not cell_value.strip():
                # Skip empty cells
                continue

            # Create task ID with the job number, row and column information
            task_id = f"row{row_idx}-col{col_name}"
            job_count += 1
            
            # Prepare the job for this cell
            job = prepare_theme_job(cell_value, themebook, model_name, task_id)
            
            # Store metadata in the custom_id field
            job["metadata"] = {
                "row_idx": row_idx,
                "col_name": col_name
            }
            
            jobs.append(job)
    
    return jobs


# ---------- 3. Define the main Streamlit app ----------

def main():
    st.title("Theme Encoder Batch Job Creator")
    st.write("""
    This tool prepares LLM jobs for theme encoding that can be run in batch mode.
    It will generate a JSONL file with all the jobs but won't execute them.
    """)

    # Step 1. Model Selection
    st.write("Note: The model name you select will be included in the batch jobs, but you can edit it later.")
    model_name = st.selectbox(
        "Select LLM model for batch processing",
        ["REPLACE-WITH-MODEL-DEPLOYMENT-NAME", "gpt-4o-mini", "gpt-4o", "gpt-4", "claude-3-haiku-20240307"]
    )

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
            st.dataframe(df_data.head(), use_container_width=True)
            
            # Display stats about the job
            num_rows = len(df_data)
            num_cols = len(df_data.columns)
            total_cells = num_rows * num_cols
            
            st.write(f"Your dataset has {num_rows} rows and {num_cols} columns, for a total of {total_cells} cells.")
            st.write("Note: Only non-empty text cells will be processed.")

            # Step 4. Prepare the Batch Jobs
            if st.button("Generate Batch Jobs"):
                with st.spinner("Preparing batch jobs..."):
                    jobs = prepare_jobs_for_dataframe(df_data, df_themebook, model_name)
                
                st.success(f"Successfully prepared {len(jobs)} theme coding jobs!")
                
                # Create the JSONL content
                jsonl_content = StringIO()
                for job in jobs:
                    jsonl_content.write(json.dumps(job) + "\n")
                
                # Let user download the JSONL file
                st.download_button(
                    label="Download Batch Jobs (JSONL)",
                    data=jsonl_content.getvalue(),
                    file_name="theme_coding_jobs.jsonl",
                    mime="application/jsonl"
                )
                
                # Also save locally if desired
                if st.checkbox("Save to local file"):
                    local_path = "theme_coding_jobs.jsonl"
                    with open(local_path, "w") as f:
                        for job in jobs:
                            f.write(json.dumps(job) + "\n")
                    st.success(f"Saved batch jobs to {os.path.abspath(local_path)}")
                
                # Display job summary
                st.write("### Job Summary")
                st.write(f"Total jobs prepared: {len(jobs)}")
                if len(jobs) > 0:
                    sample_job = jobs[0]
                    st.write("Sample job format:")
                    st.json(sample_job)
                    
                    st.write("### Example of the batch job format:")
                    st.code('''
{
  "custom_id": "task-0-row0-colQuestionText",
  "method": "POST",
  "url": "/chat/completions",
  "body": {
    "model": "YOUR-MODEL-DEPLOYMENT-NAME",
    "messages": [...],
    "tools": [...],
    "tool_choice": {...}
  },
  "metadata": {
    "row_idx": 0,
    "col_name": "QuestionText"
  }
}
                    ''')
                    
                # Instructions for processing
                st.write("### Next Steps")
                st.write("""
                1. Download the JSONL file containing all jobs
                2. Process these jobs using your preferred batch processing system
                3. Each job has a custom_id and metadata to help map results back to your original data
                4. Use the task_id in the custom_id field to identify which responses belong to which cells
                """)
                
                st.write("### Processing Results")
                st.write("""
                When you get the results back, you'll need to:
                1. Parse the function calls from each response
                2. Extract the themes data
                3. Use the metadata to map the themes back to the correct row and column in your dataset
                """)
    else:
        st.info("Please upload a Theme Book CSV to begin.")


if __name__ == "__main__":
    main()