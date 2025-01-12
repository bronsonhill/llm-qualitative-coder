import streamlit as st
import pandas as pd
from openai import OpenAI
import json

def app():
    """
    Main function for the Streamlit app. Renders the page title, 
    API key container, file uploader, theme set container, and chat sections.
    """
    st.title('Qualitative Coding')

    # Display API Key container
    api_key_container = st.container()
    with api_key_container:
        display_api_key_container()

    data = None
    upload_container = st.container()
    if 'api_key' in st.session_state:
        with upload_container:
            data = display_upload_container()
            st.dataframe(data, use_container_width=True)

    theme_set_container = st.container()
    st.session_state["theme_set"] = None
    if data is not None:
        with theme_set_container:
            st.session_state["chat_container"] = st.container()
            st.session_state["chat_container"].write('## Chat about the theme set')
            st.session_state["input_container"] = st.container()

            display_theme_set_container(data)

def display_api_key_container():
    """
    Renders a text input for the user to enter an OpenAI API key.
    If a key is already set, displays a caption instead.
    """
    st.write('## API Key')
    if 'api_key' not in st.session_state:
        api_key = st.text_input('Enter your OpenAI API Key', type='password')
        if api_key:
            st.session_state['api_key'] = api_key
    else:
        st.caption('Your key is set')

def display_upload_container():
    """
    Renders a file uploader for the user to upload a CSV file.
    Returns:
        pd.DataFrame or None: Uploaded DataFrame if available, otherwise None.
    """
    uploaded_file = st.file_uploader('Upload a new dataset')
    if uploaded_file is not None:
        return pd.read_csv(uploaded_file)
    return None

def display_theme_set_container(survey_data):
    """
    Renders the theme set generation section, including a chat input and table editor.
    Args:
        survey_data (pd.DataFrame): The survey data to be used for generating or editing a theme set.
    """
    st.write('## Generate a theme set')
    st.write('and/or direct edit below')

    chat_input = ''
    with st.session_state["input_container"]:
        chat_input = st.chat_input('Ask for modifications to the theme set')
        if chat_input:
            codes = handle_user_input(chat_input, survey_data)

    with st.session_state["chat_container"]:
        display_chat_messages()

    display_theme_set_table(survey_data)

def display_chat_messages():
    """
    Renders the chat messages from session state. 
    Only prints the first character of user messages after the first message as per original code.
    """
    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    i = 0
    for message in st.session_state["messages"]:
        if message["role"] == "user":
            if i != 0:
                # Original code only prints first character of subsequent user messages
                st.chat_message("user").write(message["content"].strip()[0])
        elif message["role"] == "assistant":
            st.chat_message("assistant").write(message["content"])
        i += 1

def display_theme_set_table(survey_data):
    """
    Renders and edits the theme set (codes) in a Streamlit data editor.
    If the theme set doesn't exist yet, it generates an initial one.
    Args:
        survey_data (pd.DataFrame): The survey data for generating the initial theme set.
    """
    theme_set_df = st.session_state['theme_set']

    if theme_set_df is None:
        with st.status('Generating initial theme set'):
            theme_set_df = generate_initial_theme_set(survey_data)
            st.session_state['theme_set'] = theme_set_df

    st.session_state['theme_set'] = st.data_editor(
        data=theme_set_df,
        use_container_width=True,
        num_rows="dynamic"
    )

def generate_initial_theme_set(survey_data):
    """
    Generates the initial theme set by prompting the OpenAI API with a default input.
    Args:
        survey_data (pd.DataFrame): The survey data to be passed to the model.
    Returns:
        pd.DataFrame: Generated DataFrame of codes (the theme set).
    """
    initial_user_input = 'Generate an initial theme set'
    codes, assistant_message = generate_theme_set(initial_user_input, survey_data)
    if assistant_message:
        st.session_state["messages"].append({"role": "assistant", "content": assistant_message})
    st.write(codes)
    return generate_theme_set_df(codes)

def handle_user_input(chat_input, survey_data):
    """
    Handles user input for editing the theme set. Calls the OpenAI API to get new codes,
    updates the theme set, and displays any message from the assistant.
    Args:
        chat_input (str): The user's message regarding the theme set.
        survey_data (pd.DataFrame): The survey data for context.
    Returns:
        pd.DataFrame: The updated codes from the model.
    """
    with st.session_state['chat_container']:
        st.chat_message("user").markdown(chat_input)

    st.session_state["messages"].append({"role": "user", "content": chat_input})

    with st.status("Editing theme set"):
        codes, assistant_message = generate_theme_set(chat_input, survey_data)
        st.session_state['theme_set'] = generate_theme_set_df(codes)

    if assistant_message:
        with st.session_state["chat_container"]:
            st.chat_message("assistant").markdown(assistant_message)
        st.session_state["messages"].append({"role": "assistant", "content": assistant_message})

    return codes

def generate_code_df_row(code, description=''):
    """
    Helper function for creating a row in the DataFrame representing a single code.
    Args:
        code (str): The code (theme).
        description (str, optional): A brief description of the code.
    Returns:
        dict: A dictionary with keys 'Codes', 'Description', and 'Use'.
    """
    return {'Codes': code, 'Description': description, 'Use': True}

def generate_theme_set_df(codes):
    """
    Converts a list of code strings into a DataFrame with columns ['Codes', 'Description', 'Use'].
    Args:
        codes (list): List of code/theme strings.
    Returns:
        pd.DataFrame: A DataFrame containing the codes, an empty description, and a boolean for 'Use'.
    """
    data = [generate_code_df_row(code) for code in codes]
    return pd.DataFrame(
        data=data,
        columns=['Codes', 'Description', 'Use']
    )

def generate_theme_set(user_input, survey_data):
    """
    Sends a prompt to OpenAI to generate or modify the theme set codes. 
    Parses the JSON response for codes and an optional message from the assistant.
    Args:
        user_input (str): The prompt or instruction from the user.
        survey_data (pd.DataFrame): The data used for context in generating themes.
    Returns:
        (list, str): A tuple where the first element is a list of codes 
                     and the second is a message from the assistant (if any).
    """
    prompt = merge_context(user_input, survey_data)
    theme_set_response = get_theme_set_response(prompt)
    theme_set_data = json.loads(theme_set_response.choices[0].message.tool_calls[0].function.arguments)
    codes = theme_set_data["codes"]
    message = theme_set_data["message"]
    return codes, message

def merge_context(user_input, survey_data):
    """
    Merges the user input with either the survey data or the existing theme set for context.
    Args:
        user_input (str): The user prompt to be merged with context.
        survey_data (pd.DataFrame): The entire survey data set.
    Returns:
        str: A string combining the user input and relevant context for the model.
    """
    if st.session_state['theme_set'] is None:
        return f"{user_input}\n\nThis is the data to be coded:\n\n{survey_data.to_csv()}"
    else:
        theme_set_csv = st.session_state['theme_set'].to_csv(index=False)
        return f"{user_input}\nThis is the current theme_set:\n{theme_set_csv}"

def get_theme_set_response(prompt):
    """
    Calls the OpenAI API to generate a theme set based on the given prompt.
    Expects to find a JSON schema for the function call in 'analyse_themes_from_data.json'.
    Args:
        prompt (str): The user prompt or merged context to send to OpenAI.
    Returns:
        openai.openai_object.OpenAIObject: The raw response from the OpenAI API.
    """
    client = OpenAI(api_key=st.session_state['api_key'])
    add_message(prompt, 'user')
    messages = get_messages()

    # Load the function call schema for the GPT model
    function_call_schema = json.load(open('analyse_themes_from_data.json'))
    return client.chat.completions.create(
        model='gpt-4o',
        messages=messages,
        tools=[
            {
                "type": "function",
                "function": function_call_schema
            }
        ],
        tool_choice={"type": "function", "function": {"name": "analyse_themes_from_data"}}
    )

def add_message(content, role):
    """
    Adds a new message to the session_state for conversation context.
    Args:
        content (str): The message content.
        role (str): The role of the message ('user' or 'assistant').
    """
    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    st.session_state["messages"].append({"role": role, "content": content})

def get_messages():
    """
    Retrieves the conversation messages from session_state.
    Returns:
        list: A list of message dictionaries { 'role': str, 'content': str }.
    """
    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    return st.session_state["messages"]

def get_trimmed_messages():
    """
    Returns only the first line of each user message from the conversation history.
    Returns:
        list: A list of truncated user messages.
    """
    trimmed_messages = []
    for message in get_messages():
        if message["role"] == "user":
            trimmed_messages.append(message["content"].split("\n")[0])
    return trimmed_messages

# Run the app
app()
