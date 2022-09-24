import streamlit as st
import pandas as pd
import os
import yaml
import sqlalchemy
from sqlalchemy import text
import matplotlib.pyplot as plt

st.title("2022 Heisman predictions")

dirname = os.path.dirname(__file__)
secrets_filename = os.path.join(dirname, "../config.yml")


def get_yaml(path):
    # from https://stackoverflow.com/a/63170705
    with open(path) as settings:
        yaml_data = yaml.load(settings, Loader=yaml.Loader)

    return yaml_data


def connect_to_db(secrets):
    database = secrets["db"]["database"]
    user = secrets["db"]["user"]
    password = secrets["db"]["password"]
    host = secrets["db"]["host"]
    port = secrets["db"]["port"]

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = sqlalchemy.create_engine(url)

    return engine


@st.cache
def load_data():
    secrets = get_yaml(secrets_filename)
    engine = connect_to_db(secrets)

    sql = """
        select *
        from player p 
        inner join team t
            on p.team_abbreviation = t.team_id 
            and p.season = t.season;
    """
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql))

    df = pd.DataFrame(query.fetchall())

    df = df.T.drop_duplicates().T
    df = df.drop("height", axis=1)
    df = df.drop("usage_overall", axis=1)
    df = df.convert_dtypes()

    return df


data_load_state = st.text("Loading data...")
data = load_data()
data_load_state.text("")

st.subheader("Leaderboard")

top_15_data = data.sort_values(by=["votes"], ascending=False).head(15)

st.pyplot(plt.plot(top_15_data["votes"]))
st.dataframe(top_15_data.style.hide_index().format(precision=2))
