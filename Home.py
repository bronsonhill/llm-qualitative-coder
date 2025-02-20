import streamlit as st


def app():
    st.title('Home')
    st.write('## Instructions')
    st.write('### Automated method: code and generate thematic from responses')
    st.caption('Generate Codebook')
    st.write('1. Upload the survey data')
    st.write('2. Click on the "Code Data" button')
    st.write('3. Download the coded data and codebook')
    st.write('4. Edit codebook as needed')
    st.caption('Generate Themes')
    st.write('5. Click on the "Generate Themes" button')
    st.write('6. Discuss and edit the themes with the assistant as required')
    st.write('7. Apply the themes to the data')
    st.write('8. Download the data with themes applied')

    st.write('### Direct method: generate themes directly from responses')


    st.write('### Hybrid method: code, embed and cluster')


app()