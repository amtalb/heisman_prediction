import streamlit as st
import pandas as pd
import os
import yaml
import sqlalchemy
from sqlalchemy import text
import matplotlib.pyplot as plt
import requests
import re

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

    url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
    engine = sqlalchemy.create_engine(url)

    return engine


@st.cache
def load_data():
    response = requests.get("http://127.0.0.1:8000/predictions/")
    df = pd.DataFrame.from_dict(response.json())

    return df


data_load_state = st.text("Loading data...")
data = load_data()
data_load_state.text("")

st.subheader("Leaderboard")

top_15_data = data.sort_values(by=["projected_votes"], ascending=False).head(15)
top_15_data["player_id"] = [
    re.sub(r"[0-9]", "", str(x)).replace("-", " ").title()
    for x in top_15_data["player_id"]
]
top_15_data = top_15_data.reset_index()
top_15_data.index += 1
top_15_data = top_15_data.drop(["index", "season"], axis=1)
top_15_data.rename(
    columns={
        "player_id": "Player",
        "team_id": "Team",
        "projected_votes": "Projected Votes",
    },
    inplace=True,
)
st.dataframe(top_15_data.style.format(precision=0))
