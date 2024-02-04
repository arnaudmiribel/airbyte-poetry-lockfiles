import streamlit as st
from ghapi.all import GhApi

st.set_page_config(page_title="Poetry Migration", page_icon="🐙")

QUERY = "repo:airbytehq/airbyte airbyte-integrations/connectors in:path poetry.lock"

@st.cache_resource
def get_api():
    return GhApi(
        token=st.secrets.ACCESS_TOKEN,
    )

@st.cache_data(ttl=24 * 60 * 60)
def get_data(_api: GhApi, query: str):
    results = _api.search.code(q=query)
    return results

api = get_api()

st.title("🐙")
st.header("Poetry Lock Files in Airbyte Connectors")

data = get_data(
    _api=api,
    query=QUERY,
)

def format_path(path: str) -> str:
    return (
        path
        .removeprefix("airbyte-integrations/connectors/destination-")
        .removesuffix("/poetry.lock")
    )

if data:
    st.subheader("Summary")
    total_count = data.total_count
    st.metric("Num. connectors using poetry.lock", f"{total_count}  🎉")

    st.subheader("Connectors")
    for item in dict(data)["items"]:
        st.link_button(format_path(item.path), item.html_url, use_container_width=True)

    st.divider()
  
    with st.expander("Raw data"):
        st.caption("Query")
        st.code(QUERY)
        st.caption("Output")
        st.json(data)
else:
    st.error("No data to display")
