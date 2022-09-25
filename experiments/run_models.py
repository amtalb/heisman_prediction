import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import yaml
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Lasso
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import mean_squared_error
import math
import warnings
import itertools
import logging
import pickle
import os
import uuid


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


def retrieve_data(engine):
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
    df = df.convert_dtypes()

    return df


def data_prep(df):
    df = pd.get_dummies(
        df, columns=["year", "position", "team_abbreviation", "conference"]
    )
    df["votes"] = df["votes"].fillna(0)
    df["got_votes"] = np.where(df["votes"] > 0, 1, 0)

    return df


def run_model(df, clf_model, reg_model):
    precs = []
    recalls = []
    fscores = []
    rmses = []
    metrics_d = {}

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        for test_year in range(2006, 2022):
            # separate out training/test for time series data
            train = df[df["season"] < test_year]
            test = df[df["season"] == test_year]

            # X/y split
            # X is all features except the target and player_id, season
            # target variable is whether or not the player got in the top 10 votes
            X_train = train.drop(["player_id", "season", "got_votes", "votes"], axis=1)
            y_train = train[["got_votes"]]
            X_test = test.drop(["player_id", "season", "got_votes", "votes"], axis=1)
            y_test = test[["got_votes"]]

            # train the classification model
            clf = clf_model.fit(X_train, y_train.values.ravel())

            # grab the 10 players with the highest predicted "probability"
            idxs = np.argpartition(clf.predict_proba(X_test)[:, 1], -10)[-10:]
            y_pred = np.zeros(len(y_test))
            y_pred[idxs] = 1

            # print out classification log
            prec, recall, fscore, _ = precision_recall_fscore_support(
                y_test, y_pred, average="macro"
            )
            precs.append(prec)
            recalls.append(recall)
            fscores.append(fscore)
            logging.info(
                f"Year: {test_year}, Precision: {prec}, Recall: {recall}, F-score: {fscore}"
            )

            # put in dict for logging to db
            metrics_d["precision_" + str(test_year)] = prec
            metrics_d["recall_" + str(test_year)] = recall
            metrics_d["fscore_" + str(test_year)] = fscore

            # X/y split for regression
            # here the target variable is the number of votes received
            test = test[np.where(y_pred == 1, True, False)]
            X_train = train[train["votes"] > 0].drop(
                ["player_id", "season", "got_votes", "votes"], axis=1
            )
            y_train = train[train["votes"] > 0][["votes"]]
            X_test = test.drop(["player_id", "season", "got_votes", "votes"], axis=1)
            y_test = test[["votes"]]

            # train the regression model
            reg = reg_model.fit(X_train, y_train.values.ravel())

            # print out regression log
            y_pred = reg.predict(X_test)
            rmse = math.sqrt(mean_squared_error(y_test, y_pred))
            rmses.append(rmse)
            logging.info(f"Year: {test_year}, RMSE: {rmse}")

            # put in dict for logging to db
            metrics_d["rmse_" + str(test_year)] = rmse

        precision_avg = sum(precs) / len(precs)
        recall_avg = sum(recalls) / len(recalls)
        fscore_avg = sum(fscores) / len(fscores)
        rmse_avg = sum(rmses) / len(rmses)

        logging.info(
            f"""Avg Precision: {precision_avg}, Avg Recall: {recall_avg}, Avg F-score: {fscore_avg}, Avg RMSE: {rmse_avg}"""
        )

        metrics_d["precision_avg"] = precision_avg
        metrics_d["recall_avg"] = recall_avg
        metrics_d["fscore_avg"] = fscore_avg
        metrics_d["rmse_avg"] = rmse_avg

        return clf, reg, metrics_d


def model_grid(df, model_config, engine):
    def product_dict(**kwargs):
        keys = kwargs.keys()
        vals = kwargs.values()
        for instance in itertools.product(*vals):
            yield dict(zip(keys, instance))

    # iterate over all classification models
    for clf in model_config["classification_models"]:
        clf_model_name = list(clf.keys())[0]
        param_dict = list(clf.values())[0]

        # iterate over all possible hyperparam combos and instatiate a model object
        for clf_model_param_combo in list(product_dict(**param_dict)):
            if clf_model_name == "LogisticRegression":
                clf_obj = LogisticRegression(**clf_model_param_combo)
            elif clf_model_name == "RandomForestClassifier":
                clf_obj = RandomForestClassifier(**clf_model_param_combo)
            elif clf_model_name == "DecisionTreeClassifer":
                clf_obj = DecisionTreeClassifier(**clf_model_param_combo)

            # iterate over all regression models
            for reg in model_config["regression_models"]:
                reg_model_name = list(reg.keys())[0]
                param_dict = list(reg.values())[0]

                # iterate over all possible hyperparam combos and instatiate a model object
                for reg_model_param_combo in list(product_dict(**param_dict)):
                    if reg_model_name == "LinearRegression":
                        reg_obj = LinearRegression(**reg_model_param_combo)
                    elif reg_model_name == "Ridge":
                        reg_obj = Ridge(**reg_model_param_combo)
                    elif reg_model_name == "RandomForestRegressor":
                        reg_obj = RandomForestRegressor(**reg_model_param_combo)
                    elif reg_model_name == "Lasso":
                        reg_obj = Lasso(**reg_model_param_combo)
                    elif reg_model_name == "PoissonRegressor":
                        reg_obj = PoissonRegressor(**reg_model_param_combo)

                    logging.info("NEW RUN")
                    logging.info(clf_model_name, str(clf_model_param_combo))
                    logging.info(reg_model_name, str(reg_model_param_combo))

                    # run the model
                    clf_fit, reg_fit, metric_d = run_model(df, clf_obj, reg_obj)

                    # save model artifact as pickle
                    clf_bin_str = pickle.dumps(clf_fit)
                    reg_bin_str = pickle.dumps(reg_fit)

                    # write model run to database
                    sql = """
                    insert into model (
                        model_id,
                        clf_model_object,
                        reg_model_object,
                        clf,
                        reg,
                        clf_params,
                        reg_params,
                        precision_avg,
                        recall_avg,
                        fscore_avg,
                        rmse_avg,
                        precision_2006,
                        recall_2006,
                        fscore_2006,
                        rmse_2006,
                        precision_2007,
                        recall_2007,
                        fscore_2007,
                        rmse_2007,
                        precision_2008,
                        recall_2008,
                        fscore_2008,
                        rmse_2008,
                        precision_2009,
                        recall_2009,
                        fscore_2009,
                        rmse_2009,
                        precision_2010,
                        recall_2010,
                        fscore_2010,
                        rmse_2010,
                        precision_2011,
                        recall_2011,
                        fscore_2011,
                        rmse_2011,
                        precision_2012,
                        recall_2012,
                        fscore_2012,
                        rmse_2012,
                        precision_2013,
                        recall_2013,
                        fscore_2013,
                        rmse_2013,
                        precision_2014,
                        recall_2014,
                        fscore_2014,
                        rmse_2014,
                        precision_2015,
                        recall_2015,
                        fscore_2015,
                        rmse_2015,
                        precision_2016,
                        recall_2016,
                        fscore_2016,
                        rmse_2016,
                        precision_2017,
                        recall_2017,
                        fscore_2017,
                        rmse_2017,
                        precision_2018,
                        recall_2018,
                        fscore_2018,
                        rmse_2018,
                        precision_2019,
                        recall_2019,
                        fscore_2019,
                        rmse_2019,
                        precision_2020,
                        recall_2020,
                        fscore_2020,
                        rmse_2020,
                        precision_2021,
                        recall_2021,
                        fscore_2021,
                        rmse_2021
                    ) values (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    """
                    with engine.connect().execution_options(autocommit=True) as conn:
                        query = conn.execute(
                            sql,
                            (
                                str(uuid.uuid4()),
                                clf_bin_str,
                                reg_bin_str,
                                clf_model_name,
                                reg_model_name,
                                str(clf_model_param_combo),
                                str(reg_model_param_combo),
                                metric_d["precision_avg"],
                                metric_d["recall_avg"],
                                metric_d["fscore_avg"],
                                metric_d["rmse_avg"],
                                metric_d["precision_2006"],
                                metric_d["recall_2006"],
                                metric_d["fscore_2006"],
                                metric_d["rmse_2006"],
                                metric_d["precision_2007"],
                                metric_d["recall_2007"],
                                metric_d["fscore_2007"],
                                metric_d["rmse_2007"],
                                metric_d["precision_2008"],
                                metric_d["recall_2008"],
                                metric_d["fscore_2008"],
                                metric_d["rmse_2008"],
                                metric_d["precision_2009"],
                                metric_d["recall_2009"],
                                metric_d["fscore_2009"],
                                metric_d["rmse_2009"],
                                metric_d["precision_2010"],
                                metric_d["recall_2010"],
                                metric_d["fscore_2010"],
                                metric_d["rmse_2010"],
                                metric_d["precision_2011"],
                                metric_d["recall_2011"],
                                metric_d["fscore_2011"],
                                metric_d["rmse_2011"],
                                metric_d["precision_2012"],
                                metric_d["recall_2012"],
                                metric_d["fscore_2012"],
                                metric_d["rmse_2012"],
                                metric_d["precision_2013"],
                                metric_d["recall_2013"],
                                metric_d["fscore_2013"],
                                metric_d["rmse_2013"],
                                metric_d["precision_2014"],
                                metric_d["recall_2014"],
                                metric_d["fscore_2014"],
                                metric_d["rmse_2014"],
                                metric_d["precision_2015"],
                                metric_d["recall_2015"],
                                metric_d["fscore_2015"],
                                metric_d["rmse_2015"],
                                metric_d["precision_2016"],
                                metric_d["recall_2016"],
                                metric_d["fscore_2016"],
                                metric_d["rmse_2016"],
                                metric_d["precision_2017"],
                                metric_d["recall_2017"],
                                metric_d["fscore_2017"],
                                metric_d["rmse_2017"],
                                metric_d["precision_2018"],
                                metric_d["recall_2018"],
                                metric_d["fscore_2018"],
                                metric_d["rmse_2018"],
                                metric_d["precision_2019"],
                                metric_d["recall_2019"],
                                metric_d["fscore_2019"],
                                metric_d["rmse_2019"],
                                metric_d["precision_2020"],
                                metric_d["recall_2020"],
                                metric_d["fscore_2020"],
                                metric_d["rmse_2020"],
                                metric_d["precision_2021"],
                                metric_d["recall_2021"],
                                metric_d["fscore_2021"],
                                metric_d["rmse_2021"],
                            ),
                        )

                    logging.info("\n")


def main():
    dirname = os.path.dirname(__file__)
    secrets_filename = os.path.join(dirname, "../config.yml")
    model_config_filename = os.path.join(dirname, "model_config.yml")

    # set up logging
    logging.basicConfig(filename="model_run.log", encoding="utf-8", level=logging.INFO)

    secrets = get_yaml(secrets_filename)
    engine = connect_to_db(secrets)
    df = retrieve_data(engine)
    df = data_prep(df)
    model_config = get_yaml(model_config_filename)
    model_grid(df, model_config, engine)


if __name__ == "__main__":
    main()
