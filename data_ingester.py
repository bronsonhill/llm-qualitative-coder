import pandas as pd

SURVEY_COUNT = 3
COLUMN_CODES_SURVEY_1 = ["2275844"]
COLUMN_CODES_SURVEY_2 = ["2298314"]
COLUMN_CODES_SURVEY_3 = ["2298340"]
COLUMNS_OF_INTEREST = ["2275844", "2298314", "2298340"]

def ingest_survey_data():
    survey_data = retrieve_concatenated_survey_data()
    
    survey_data = survey_data.loc[:, survey_data.columns[survey_data.columns.str.contains('|'.join(COLUMNS_OF_INTEREST))]]
    
    return survey_data

def retrieve_concatenated_survey_data():
    survey_data = pd.DataFrame()
    for i in range(1, SURVEY_COUNT + 1):
        data = ingest_survey_data_by_survey_code(i)
        survey_data = pd.concat([survey_data, data], axis=1)

    return survey_data

def ingest_survey_data_by_survey_code(survey_code):
    # Assuming survey files exist and are properly structured
    data = pd.read_csv(f"survey_data/survey_{survey_code}_cleaned.csv")
    return data
