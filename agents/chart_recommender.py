import pandas as pd
import altair as alt


def recommend_chart_type(df: pd.DataFrame) -> str:
    """
    Recommend a chart type based on the structure of the DataFrame.
    """
    time_columns = [col for col in df.columns if "date" in col.lower() or "time" in col.lower()]
    numeric_columns = df.select_dtypes(include='number').columns.tolist()
    category_columns = df.select_dtypes(include='object').columns.tolist()

    if time_columns and numeric_columns:
        return "line"
    elif category_columns and numeric_columns:
        return "bar"
    elif len(numeric_columns) >= 2:
        return "scatter"
    else:
        return "table"


def plot_chart(df: pd.DataFrame, chart_type: str) -> alt.Chart:
    """
    Generate an Altair chart based on the recommended chart type.
    """
    if chart_type == "line":
        time_col = [col for col in df.columns if "date" in col.lower() or "time" in col.lower()][0]
        value_col = df.select_dtypes(include='number').columns[0]
        return alt.Chart(df).mark_line().encode(
            x=alt.X(time_col, title="Date"),
            y=alt.Y(value_col, title=value_col.replace("_", " ").title()),
            tooltip=[time_col, value_col]
        ).properties(title="📈 Line Chart")

    elif chart_type == "bar":
        cat_col = df.select_dtypes(include='object').columns[0]
        value_col = df.select_dtypes(include='number').columns[0]
        return alt.Chart(df).mark_bar().encode(
            x=alt.X(cat_col, sort='-y', title=cat_col.replace("_", " ").title()),
            y=alt.Y(value_col, title=value_col.replace("_", " ").title()),
            tooltip=[cat_col, value_col]
        ).properties(title="📊 Bar Chart")

    elif chart_type == "scatter":
        num_cols = df.select_dtypes(include='number').columns[:2]
        return alt.Chart(df).mark_circle(size=60).encode(
            x=num_cols[0],
            y=num_cols[1],
            tooltip=list(num_cols)
        ).properties(title="🔘 Scatter Plot")

    else:
        raise ValueError("Chart type not supported or not applicable.")
