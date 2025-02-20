import streamlit as st
import pandas as pd
from io import StringIO

def compare_gold_vs_test(generated_df: pd.DataFrame, test_df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Take the first column of the gold_df (the 'text' column).
    2. Include all justification columns (ending with '_justification') from gold_df.
    3. Identify shared theme columns (0/1) in both gold_df and test_df,
       and create a new col <theme>_compare = gold - test.
    4. Merge these into a new DataFrame: [text_col, justification_cols, compare_cols].
    5. Sort all columns except the text column alphabetically, so text remains first.
    6. Return the final DataFrame.
    """

    # The first column of gold_df (the 'text')
    text_col_name = generated_df.columns[0]

    # Justification columns
    justification_cols = [c for c in generated_df.columns if c.endswith("_justification")]

    # Identify theme columns in gold_df that also exist in test_df
    # (we exclude text_col and justification columns).
    possible_themes = []
    for col in generated_df.columns:
        if col == text_col_name:
            continue
        if col.endswith("_justification"):
            continue
        if col in test_df.columns:
            possible_themes.append(col)

    # Build a new DataFrame starting with the text column
    compared_df = pd.DataFrame()
    compared_df[text_col_name] = generated_df[text_col_name]

    # Add justification columns
    for jc in justification_cols:
        compared_df[jc] = generated_df[jc]

    # For each identified theme, compute (gold - test) in a new "<theme>_compare" column
    for theme_col in possible_themes:
        compare_col = f"{theme_col}_compare"
        compared_df[compare_col] = generated_df[theme_col] - test_df[theme_col]

    # Now we sort everything except the first (text) column
    # We keep text as the first column, then sort all remaining columns alphabetically.
    sorted_cols = [text_col_name] + sorted([c for c in compared_df.columns if c != text_col_name])
    compared_df = compared_df[sorted_cols]

    return compared_df

def main():
    st.title("Compare Theme Outputs (Generated vs. Test)")

    st.markdown(
        """
        **Instructions**:
        1. Upload the **generated** CSV (from the previous page) which includes 0/1 columns and
           `_justification` columns.
        2. Upload the **test** CSV, which has the same 0/1 columns but no justifications.
        3. We'll subtract `test`from `generated` for each theme column:
           - `0` ⇒ both match
           - `1` ⇒ generated=1, test=0
           - `-1` ⇒ generated=0, test=1
        4. The final DataFrame includes the original generated columns (so you keep the justification)
           and new `_compare` columns for each theme.
        """
    )

    gold_file = st.file_uploader("Upload Generated CSV (with justification columns)", type="csv")
    if gold_file is None:
        st.info("Please upload the generated CSV first.")
        return

    test_file = st.file_uploader("Upload Test CSV (same shape, same theme columns)", type="csv")
    if test_file is None:
        st.info("Please upload the Test CSV.")
        return

    # Read them as DataFrames
    gold_df = pd.read_csv(gold_file)
    test_df = pd.read_csv(test_file)
    # Show the user a preview
    st.write("### Generated CSV (Preview)")
    st.dataframe(gold_df, use_container_width=True)

    st.write("### Test CSV (Preview)")
    st.dataframe(test_df, use_container_width=True)

    # Compare button
    if st.button("Compare Columns"):
        with st.spinner("Comparing theme columns..."):
            compared_df = compare_gold_vs_test(gold_df, test_df)

        st.success("Comparison complete!")
        st.write("### Comparison Results")
        st.dataframe(compared_df, use_container_width=True)

        # Download button
        csv_buffer = StringIO()
        compared_df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="Download Compared CSV",
            data=csv_buffer.getvalue(),
            file_name="compared_data.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()
