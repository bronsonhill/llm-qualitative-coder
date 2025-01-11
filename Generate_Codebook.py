import streamlit as st
import pandas as pd
from openai import OpenAI
from io import StringIO
import json
from data_ingester import ingest_survey_data

def app():
    st.title('Qualitative Coding')

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
    st.write('## API Key')
    if 'api_key' not in st.session_state:
        api_key = st.text_input('Enter your OpenAI API Key', type='password')
        if api_key:
            st.session_state['api_key'] = api_key
    else:
        st.caption('Your key is set')

def display_upload_container():
    f = st.file_uploader('Upload a new dataset')
    if f is not None:
        return pd.read_csv(f)
    else :
        return None

def generate_code_df_row(code, description = ''):
    return {'Codes': code, 'Description': description, 'Use': True}

def display_theme_set_container(survey_data):

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
        
        if "messages" not in st.session_state:
            st.session_state["messages"] = []
        i = 0
        for message in st.session_state["messages"]:
            if message["role"] == "user":
                if i != 0:
                    st.chat_message("user").write(message["content"].strip()[0])
            elif message["role"] == "assistant":
                st.chat_message("assistant").write(message["content"])
            i += 1

def display_theme_set_table(survey_data):
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
    return

def generate_initial_theme_set(survey_data):
    initial_user_input = 'Generate an initial theme set'
    codes, message = generate_theme_set(initial_user_input, survey_data)
    if message:
        st.session_state["messages"].append({"role": "assistant", "content": message})

    st.write(codes)
    
    return generate_theme_set_df(codes)

def generate_theme_set(user_input, data):

    content = merge_context(user_input, data)
    # st.write(content)
    theme_set_response = get_theme_set_response(content)
    theme_set_arguments = json.loads(theme_set_response.choices[0].message.tool_calls[0].function.arguments)

    codes = pd.DataFrame()
    codes = theme_set_arguments["codes"]
    
    message = theme_set_arguments["message"]

    return codes, message

def generate_theme_set_df(codes):
    data = [generate_code_df_row(code) for code in codes]
    return pd.DataFrame(
        data=data,
        columns=['Codes', 'Description', 'Use']
    )

def get_theme_set_response(content):
    client = OpenAI(api_key=st.session_state['api_key'])
    add_message(content, 'user')
    messages = get_messages()
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

def get_message_response():
    client = OpenAI(api_key=st.session_state['api_key'])
    messages = get_messages()
    if st.session_state['theme_set'] is not None:
        system_message = {"role": "system", "content": "Explain the codes you have previously generated that are attached."}
        messages.append(system_message)
    return client.chat.completions.create(
        model='gpt-4o',
        messages=messages

    )

def merge_context(user_input, data):
    if st.session_state['theme_set'] is None:
        return f"{user_input}\n\nThis is the data to be coded:\n\n{data.to_csv()}"
    else:
        theme_set = st.session_state['theme_set'].to_csv(index=False)
        return f"{user_input}\nThis is the current theme_set:\n{theme_set}"

def add_message(content, role):
    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    st.session_state["messages"].append({"role": role, "content": content})
    return

def get_messages():
    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    return st.session_state["messages"]

def get_trimmed_messages():
    trimmed_messages = []
    for message in get_messages():
        if message["role"] == "user":
            trimmed_messages.append(message["content"].split("\n")[0])
    
    return trimmed_messages

def handle_user_input(chat_input, survey_data):

    with st.session_state['chat_container']:
        st.chat_message("user").markdown(chat_input)
    st.session_state["messages"].append({"role": "user", "content": chat_input})
    
    with st.status("Editing theme set"): 
        codes, message = generate_theme_set(chat_input, survey_data)
        st.session_state['theme_set'] = generate_theme_set_df(codes)

    if message:
        with st.session_state["chat_container"]:
            st.chat_message("assistant").markdown(message)
        st.session_state["messages"].append({"role": "assistant", "content": message})
    return codes


app()