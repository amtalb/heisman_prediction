import psycopg2
import yaml
from pathlib import Path


# from https://stackoverflow.com/a/63170705
def get_settings():
    full_file_path = Path(__file__).parent.joinpath("../config.yml")
    with open(full_file_path) as settings:
        settings_data = yaml.load(settings, Loader=yaml.Loader)
    return settings_data


def create_team_table(cursor):
    sql = """CREATE TABLE team(
                team_id varchar(50) NOT NULL,
                season float NOT NULL,
                conference varchar(20) NOT NULL,
                win_percentage float NOT NULL,
                points_per_game float NOT NULL,
                points_against_per_game float NOT NULL,
                strength_of_schedule float NOT NULL,
                simple_rating_system float NOT NULL,
                PRIMARY KEY (team_id,season)
            );"""
    cursor.execute(sql)


def create_player_table(cursor):
    sql = """CREATE TABLE player(
            player_id varchar(50) NOT NULL,
            season float NOT NULL,
            team_abbreviation varchar(30) NOT NULL,
            position char(4) NOT NULL,
            year varchar(4) NOT NULL,
            games float NOT NULL,
            completed_passes float NOT NULL,
            pass_attempts float NOT NULL,
            passing_completion float NOT NULL,
            passing_yards float NOT NULL,
            passing_touchdowns float NOT NULL,
            interceptions_thrown float NOT NULL,
            passing_yards_per_attempt float NOT NULL,
            adjusted_yards_per_attempt float NOT NULL,
            quarterback_rating float NOT NULL,
            rush_attempts float NOT NULL,
            rush_yards float NOT NULL,
            rush_yards_per_attempt float NOT NULL,
            rush_touchdowns float NOT NULL,
            receptions float NOT NULL,
            receiving_yards float NOT NULL,
            receiving_yards_per_reception float NOT NULL,
            receiving_touchdowns float NOT NULL,
            plays_from_scrimmage float NOT NULL,
            yards_from_scrimmage float NOT NULL,
            yards_from_scrimmage_per_play float NOT NULL,
            rushing_and_receiving_touchdowns float NOT NULL,
            solo_tackles float NOT NULL,
            assists_on_tackles float NOT NULL,
            total_tackles float NOT NULL,
            tackles_for_loss float NOT NULL,
            sacks float NOT NULL,
            interceptions float NOT NULL,
            yards_returned_from_interceptions float NOT NULL,
            yards_returned_per_interception float NOT NULL,
            interceptions_returned_for_touchdown float NOT NULL,
            passes_defended float NOT NULL,
            fumbles_recovered float NOT NULL,
            yards_recovered_from_fumble float NOT NULL,
            fumbles_recovered_for_touchdown float NOT NULL,
            fumbles_forced float NOT NULL,
            punt_return_touchdowns float NOT NULL,
            kickoff_return_touchdowns float NOT NULL,
            other_touchdowns float NOT NULL,
            total_touchdowns float NOT NULL,
            extra_points_made float NOT NULL,
            field_goals_made float NOT NULL,
            extra_points_attempted float NOT NULL,
            extra_point_percentage float NOT NULL,
            field_goals_attempted float NOT NULL,
            field_goal_percentage float NOT NULL,
            two_point_conversions float NOT NULL,
            safeties float NOT NULL,
            points float NOT NULL,
            votes int NOT NULL,
            PRIMARY KEY (player_id,season),
            CONSTRAINT fk_team
                    FOREIGN KEY(team_abbreviation, season) 
                        REFERENCES team(team_id, season)
          );"""
    cursor.execute(sql)


def create_prediction_table(cursor):
    sql = """CREATE TABLE prediction(
                player_id varchar(50) NOT NULL,
                team_id varchar(50) NOT NULL,
                season float NOT NULL,
                projected_votes float NOT NULL,
                PRIMARY KEY (player_id,season),
                CONSTRAINT fk_player
                    FOREIGN KEY(player_id, season) 
                        REFERENCES player(player_id, season),
                CONSTRAINT fk_team
                    FOREIGN KEY(team_id, season) 
                        REFERENCES team(team_id, season)
            );"""
    cursor.execute(sql)


def create_model_table(cursor):
    sql = """CREATE TABLE model(
                model_id varchar(36) NOT NULL,
                clf_model_object bytea NOT NULL,
                reg_model_object bytea NOT NULL,
                clf varchar(50) NOT NULL,
                reg varchar(50) NOT NULL,
                clf_params varchar(200) NOT NULL,
                reg_params varchar(200) NOT NULL,
                precision_avg float NOT NULL,
                recall_avg float NOT NULL,
                fscore_avg float NOT NULL,
                rmse_avg float NOT NULL,
                precision_2006 float NOT NULL,
                recall_2006 float NOT NULL,
                fscore_2006 float NOT NULL,
                rmse_2006 float NOT NULL,
                precision_2007 float NOT NULL,
                recall_2007 float NOT NULL,
                fscore_2007 float NOT NULL,
                rmse_2007 float NOT NULL,
                precision_2008 float NOT NULL,
                recall_2008 float NOT NULL,
                fscore_2008 float NOT NULL,
                rmse_2008 float NOT NULL,
                precision_2009 float NOT NULL,
                recall_2009 float NOT NULL,
                fscore_2009 float NOT NULL,
                rmse_2009 float NOT NULL,
                precision_2010 float NOT NULL,
                recall_2010 float NOT NULL,
                fscore_2010 float NOT NULL,
                rmse_2010 float NOT NULL,
                precision_2011 float NOT NULL,
                recall_2011 float NOT NULL,
                fscore_2011 float NOT NULL,
                rmse_2011 float NOT NULL,
                precision_2012 float NOT NULL,
                recall_2012 float NOT NULL,
                fscore_2012 float NOT NULL,
                rmse_2012 float NOT NULL,
                precision_2013 float NOT NULL,
                recall_2013 float NOT NULL,
                fscore_2013 float NOT NULL,
                rmse_2013 float NOT NULL,
                precision_2014 float NOT NULL,
                recall_2014 float NOT NULL,
                fscore_2014 float NOT NULL,
                rmse_2014 float NOT NULL,
                precision_2015 float NOT NULL,
                recall_2015 float NOT NULL,
                fscore_2015 float NOT NULL,
                rmse_2015 float NOT NULL,
                precision_2016 float NOT NULL,
                recall_2016 float NOT NULL,
                fscore_2016 float NOT NULL,
                rmse_2016 float NOT NULL,
                precision_2017 float NOT NULL,
                recall_2017 float NOT NULL,
                fscore_2017 float NOT NULL,
                rmse_2017 float NOT NULL,
                precision_2018 float NOT NULL,
                recall_2018 float NOT NULL,
                fscore_2018 float NOT NULL,
                rmse_2018 float NOT NULL,
                precision_2019 float NOT NULL,
                recall_2019 float NOT NULL,
                fscore_2019 float NOT NULL,
                rmse_2019 float NOT NULL,
                precision_2020 float NOT NULL,
                recall_2020 float NOT NULL,
                fscore_2020 float NOT NULL,
                rmse_2020 float NOT NULL,
                precision_2021 float NOT NULL,
                recall_2021 float NOT NULL,
                fscore_2021 float NOT NULL,
                rmse_2021 float NOT NULL,
                PRIMARY KEY (model_id)
            );"""
    cursor.execute(sql)


def main(cursor):
    create_player_table(cursor)
    create_team_table(cursor)
    create_prediction_table(cursor)
    create_model_table(cursor)


if __name__ == "__main__":

    config = get_settings()

    # connection establishment
    conn = psycopg2.connect(
        database=config["db"]["database"],
        user=config["db"]["user"],
        password=config["db"]["password"],
        host=config["db"]["host"],
    )
    conn.autocommit = True
    cursor = conn.cursor()

    main(cursor)

    conn.commit()
    conn.close()
