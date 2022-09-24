from fastapi import FastAPI
import psycopg2
from pathlib import Path
import yaml
import sqlalchemy

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/predictions/{week_num}")
def read_item(week_num):
    # from https://stackoverflow.com/a/63170705
    full_file_path = Path(__file__).parent.joinpath("../config.yml")
    with open(full_file_path) as settings:
        secrets = yaml.load(settings, Loader=yaml.Loader)

    database = secrets["db"]["database"]
    user = secrets["db"]["user"]
    password = secrets["db"]["password"]
    host = secrets["db"]["host"]
    port = secrets["db"]["port"]

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = sqlalchemy.create_engine(url)

    sql = f"""
    select * 
    from prediction
    where week = {week_num}
    """

    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(sql)

    return {query.fetchall()}
