import streamlit as st
from ghapi.all import GhApi
from pathlib import Path
import pandas as pd

QUERY_POETRY = (
    "repo:airbytehq/airbyte airbyte-integrations/connectors in:path poetry.lock"
)
QUERY_REQUIREMENTS = (
    "repo:airbytehq/airbyte airbyte-integrations/connectors in:path requirements.txt"
)


@st.cache_resource
def get_api():
    return GhApi(
        token=st.secrets.ACCESS_TOKEN,
    )


@st.cache_data(ttl=24 * 60 * 60)
def get_data(_api: GhApi, query: str):
    results = _api.search.code(q=query)
    return results


@st.cache_data
def get_connectors() -> [pd.DataFrame, pd.DataFrame]:
    dataframes = pd.read_html(Path("connectors.html").read_text())
    sources_df, destinations_df = dataframes

    # Pretty print 'Support Level'
    sources_df["Support Level"] = sources_df["Support Level"].str.title()
    destinations_df["Support Level"] = destinations_df["Support Level"].str.title()

    # Restrict to useful columns
    sources_df = sources_df[["Connector Name", "Support Level", "OSS", "Cloud"]]
    destinations_df = destinations_df[
        ["Connector Name", "Support Level", "OSS", "Cloud"]
    ]

    # Turn emojis into boolean (checkbox) column
    for column in ("OSS", "Cloud"):
        sources_df[column] = sources_df[column].apply(lambda x: x == "✅")
        destinations_df[column] = destinations_df[column].apply(lambda x: x == "✅")

    return sources_df, destinations_df


def format_path(path: str) -> str:
    """ Extract connector name from path. """
    return path.removeprefix("airbyte-integrations/connectors/").split("/")[0]


api = get_api()

poetry_data = get_data(
    _api=api,
    query=QUERY_POETRY,
)

requirements_data = get_data(
    _api=api,
    query=QUERY_REQUIREMENTS,
)

sources_df, destinations_df = get_connectors()

st.title("🐙")
st.header("Poetry Lock Files in Airbyte Connectors")

""" This app helps monitoring our progress as we migrate
all Airbyte connectors to Poetry. To do so, we lookup the
amount of `poetry.lock` files under the `airbyte-integrations/connectors`
directory of the main `airbytehq/airbyte` repository.

Reach out to @alafanechere for feedback or help!
"""

if poetry_data:
    st.subheader("Summary")

    left, middle, right = st.columns((2, 2, 1))
    left.metric("Num. connectors using poetry.lock", f"{poetry_data.total_count}  🎉")
    middle.metric(
        "Num. connectors using requirements.txt", f"{requirements_data.total_count}"
    )
    right.metric(
        "Percent migrated",
        f"{poetry_data.total_count / (poetry_data.total_count + requirements_data.total_count):.0%}",
    )

    st.subheader("Migrated Connectors")
    for item in dict(poetry_data)["items"]:
        if item.path.startswith(
            (
                "airbyte-integrations/connectors/source-",
                "airbyte-integrations/connectors/destination-",
            )
        ):
            st.link_button(
                format_path(item.path), item.html_url, use_container_width=True
            )

    st.subheader("All Connectors")
    st.write("Sources")
    st.dataframe(sources_df, use_container_width=True, hide_index=True)
    st.write("Destinations")
    st.dataframe(destinations_df, use_container_width=True, hide_index=True)

else:
    st.error("No data to display")
