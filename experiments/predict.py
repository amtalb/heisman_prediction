import pickle
from turtle import position
import yaml
import sqlalchemy
from pathlib import Path
import numpy as np
import pandas as pd


def connect_to_db():
    # from https://stackoverflow.com/a/63170705
    full_file_path = Path(__file__).parent.joinpath("../config.yml")
    with open(full_file_path) as settings:
        secrets = yaml.load(settings, Loader=yaml.Loader)

    database = secrets["db"]["database"]
    user = secrets["db"]["user"]
    password = secrets["db"]["password"]
    host = secrets["db"]["host"]

    url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
    engine = sqlalchemy.create_engine(url)

    return engine


def get_player_data(engine):
    # read recent data
    sql = """select *
            from player p 
            inner join team t
                on p.team_abbreviation = t.team_id 
                and p.season = t.season
            where p.season = 2022;
            """
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(sql)

    df = pd.DataFrame(query.fetchall())
    return df


def get_model_data(engine):
    # read in model pickles
    # sql = """
    # select clf_model_object, reg_model_object
    # from model
    # where model_id like 'a0670834-fd68-4058-8363-dc152b1fe282';
    # """  # TODO pick best model with SQL query
    # with engine.connect().execution_options(autocommit=True) as conn:
    #     query = conn.execute(sql)

    # model_data = query.fetchall()

    # # unpickle models
    # clf_model = pickle.loads(model_data[0])
    # reg_model = pickle.loads(model_data[1])

    # from https://stackoverflow.com/a/63170705
    full_file_path = Path(__file__).parent.joinpath("clf_model.pkl")
    with open(full_file_path, "rb") as f:
        clf_model = pickle.load(f)
    full_file_path = Path(__file__).parent.joinpath("reg_model.pkl")
    with open(full_file_path, "rb") as f:
        reg_model = pickle.load(f)

    return clf_model, reg_model


def predict(player_data, clf, reg):
    # preprocessing
    df = player_data.convert_dtypes()
    df = pd.get_dummies(
        df, columns=["year", "position", "team_abbreviation", "conference"]
    )
    df = df.drop(["player_id", "season", "votes", "team_id"], axis=1)

    # add in missing columns
    for col in clf.feature_names_in_:
        if col not in df.columns:
            df[col] = 0

    # reorder columns
    df = df[clf.feature_names_in_.tolist()]

    # make predictions
    clf_pred = clf.predict_proba(df)

    # grab the 10 players with the highest predicted "probability"
    idxs = np.argpartition(clf_pred[:, 1], -10)[-10:]
    got_votes_pred = np.zeros(len(clf_pred))
    got_votes_pred[idxs] = 1

    df = df[np.where(got_votes_pred == 1, True, False)]

    # make predictions
    votes_pred = reg.predict(df)

    return_df = player_data.iloc[idxs]
    return_df["projected_votes"] = votes_pred
    return_df = return_df[["player_id", "team_id", "season", "projected_votes"]]
    return_df = return_df.T.drop_duplicates().T

    return return_df


def write_predictions(engine, predictions):
    # write them to db
    predictions.to_sql("prediction", engine, if_exists="replace", index=False)


if __name__ == "__main__":
    engine = connect_to_db()
    player_data = get_player_data(engine)
    clf_model, reg_model = get_model_data(engine)
    predictions = predict(player_data, clf_model, reg_model)
    write_predictions(engine, predictions)
