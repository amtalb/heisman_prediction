import psycopg2

import yaml
from pathlib import Path


# from https://stackoverflow.com/a/63170705
def get_settings():
    full_file_path = Path(__file__).parent.joinpath("../config.yml")
    with open(full_file_path) as settings:
        settings_data = yaml.load(settings, Loader=yaml.Loader)
    return settings_data


config = get_settings()

# connection establishment
conn = psycopg2.connect(
    database=config["db"]["database"],
    user=config["db"]["user"],
    password=config["db"]["password"],
    host=config["db"]["host"],
    port=config["db"]["port"],
)

cursor = conn.cursor()

sql = """COPY player(
                player_id,
                season,
                name,
                team_abbreviation,
                position,
                height,
                weight,
                year,
                games,
                completed_passes,
                pass_attempts,
                passing_completion,
                passing_yards,
                passing_touchdowns,
                interceptions_thrown,
                passing_yards_per_attempt,
                adjusted_yards_per_attempt,
                quarterback_rating,
                rush_attempts,
                rush_yards,
                rush_yards_per_attempt,
                rush_touchdowns,
                receptions,
                receiving_yards,
                receiving_yards_per_reception,
                receiving_touchdowns,
                plays_from_scrimmage,
                yards_from_scrimmage,
                yards_from_scrimmage_per_play,
                rushing_and_receiving_touchdowns,
                solo_tackles,
                assists_on_tackles,
                total_tackles,
                tackles_for_loss,
                sacks,
                interceptions,
                yards_returned_from_interceptions,
                yards_returned_per_interception,
                interceptions_returned_for_touchdown,
                passes_defended,
                fumbles_recovered,
                yards_recovered_from_fumble,
                fumbles_recovered_for_touchdown,
                fumbles_forced,
                punt_return_touchdowns,
                kickoff_return_touchdowns,
                other_touchdowns,
                total_touchdowns,
                extra_points_made,
                field_goals_made,
                extra_points_attempted,
                extra_point_percentage,
                field_goals_attempted,
                field_goal_percentage,
                two_point_conversions,
                safeties,
                points
              )
        FROM '/home/amtalb/Documents/heisman/data/clean_data.csv'
        DELIMITER ','
        CSV HEADER;"""

cursor.execute(sql)

conn.commit()
conn.close()
