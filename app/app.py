import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import os
import yaml
import sqlalchemy
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
    response = requests.get("https://heisman-prediction-api-amtalb.vercel.app/")
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
top_15_data["team_id"] = [str(x).title() for x in top_15_data["team_id"]]
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


from matplotlib.patches import FancyBboxPatch

bar_fig = plt.figure(figsize=(8, 7))
bar_ax = bar_fig.add_subplot(111)

bar_ax = top_15_data.set_index("Player").plot.barh(
    alpha=0.8,
    ax=bar_ax,
    legend=False,
    width=0.9,
    figsize=(6, 6),
)
bar_ax.invert_yaxis()

new_patches = []
for cl, patch in zip(
    [
        "#03071E",
        "#370617",
        "#6A040F",
        "#9D0208",
        "#D00000",
        "#DC2F02",
        "#E85D04",
        "#F48C06",
        "#FAA307",
        "#FFBA08",
    ],
    reversed(bar_ax.patches),
):
    bb = patch.get_bbox()
    p_bbox = FancyBboxPatch(
        (bb.xmin, bb.ymin),
        abs(bb.width),
        abs(bb.height),
        boxstyle="round,rounding_size=30",
        ec="none",
        fc=cl,
        mutation_aspect=0.01,
    )
    patch.remove()
    new_patches.append(p_bbox)
for patch in new_patches:
    bar_ax.add_patch(patch)

bar_ax.spines["top"].set_visible(False)
bar_ax.spines["right"].set_visible(False)
bar_ax.spines["left"].set_visible(False)
bar_ax.spines["bottom"].set_visible(False)

bar_ax.xaxis.label.set_color("#DDDDDD")

# Third, add a horizontal grid (but keep the vertical grid hidden).
# Color the lines a light gray as well.
bar_ax.set_axisbelow(True)
bar_ax.xaxis.grid(False)
bar_ax.yaxis.grid(False)

bar_ax.set_facecolor("#0e1117")
bar_fig.set_facecolor("#0e1117")
bar_fig.set_alpha(0.1)
plt.yticks([])
plt.xticks([])
plt.ylabel(None)
plt.xlabel(None)

for i, v in enumerate(top_15_data["Projected Votes"]):
    bar_ax.text(
        v + 10,
        i,
        str(int(v)),
        color="#DDDDDD",
        fontweight="bold",
        fontsize=10,
        fontfamily="fantasy",
        ha="left",
        va="center",
    )

for i, v in enumerate(top_15_data["Projected Votes"]):
    bar_ax.text(
        10,
        i,
        top_15_data.iloc[i, :]["Player"] + "- " + top_15_data.iloc[i, :]["Team"],
        color="#222222",
        fontweight="bold",
        fontsize=10,
        fontfamily="fantasy",
        ha="left",
        va="center",
    )

bar_fig.tight_layout()
bar_fig
