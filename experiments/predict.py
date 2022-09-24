import pickle
import yaml
import psycopg2
from pathlib import Path

# from https://stackoverflow.com/a/63170705
full_file_path = Path(__file__).parent.joinpath("../config.yml")
with open(full_file_path) as settings:
    config = yaml.load(settings, Loader=yaml.Loader)


# connection establishment
conn = psycopg2.connect(
    database=config["db"]["database"],
    user=config["db"]["user"],
    password=config["db"]["password"],
    host=config["db"]["host"],
    port=config["db"]["port"],
)
cursor = conn.cursor()

# read recent data
sql = """SELECT *
         FROM player
         WHERE season = 2022;
        """
cursor.execute(sql)

# read in model pickles
sql = """
select clf_model_object, reg_model_object
from model
where model_id like 'a0670834-fd68-4058-8363-dc152b1fe282';;
"""

# unpickle model
with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# make predictions

# write them to db
sql = """INSERT INTO TABLE prediction(
                player_id ,
                team_id ,
                season ,
                projected_votes ,
            );"""
cursor.execute(sql)


conn.commit()
conn.close()
