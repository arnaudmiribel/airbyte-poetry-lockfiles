import streamlit as st
from ghapi.all import GhApi
from pathlib import Path
import pandas as pd

# Constants
QUERY = "repo:airbytehq/airbyte airbyte-integrations/connectors in:path poetry.lock"

@st.cache_resource
def get_api():
    return GhApi(
        token=ACCESS_TOKEN,
    )

@st.cache_data(ttl=24 * 60 * 60)
def get_data(_api: GhApi, query: str):
    results = _api.search.code(q=query)
    return results

@st.cache_data
def get_connectors() -> [pd.DataFrame, pd.DataFrame]:
    dataframes = pd.read_html(Path("connectors.html").read_text())
    sources_df, destinations_df = dataframes
    return sources_df, destinations_df

def format_path(path: str) -> str:
    return (
        path
        .removeprefix("airbyte-integrations/connectors/destination-")
        .removesuffix("/poetry.lock")
    )

api = get_api()

data = get_data(
    _api=api,
    query=QUERY,
)

sources_df, destinations_df = get_connectors()

st.title("🐙")
st.header("Poetry Lock Files in Airbyte Connectors")


if data:
    st.subheader("Summary")
    total_count = data.total_count
    st.metric("Num. connectors using poetry.lock", f"{total_count}  🎉")

    st.subheader("Migrated Connectors")
    for item in dict(data)["items"]:
        st.link_button(format_path(item.path), item.html_url, use_container_width=True)

    st.subheader("All Connectors")
    st.write("Sources")
    st.dataframe(sources_df)
    st.write("Destinations")
    st.dataframe(destinations_df)

    st.divider()
    with st.expander("Debug"):
        st.caption("Query")
        st.code(QUERY)
        st.caption("Output")
        st.json(data)
else:
    st.error("No data to display")
