import sys
import psycopg2
from psycopg2.extras import execute_values
from sportsipy.ncaaf.teams import Team
from sportsipy.ncaaf.conferences import Conference
import yaml
from pathlib import Path


# from https://stackoverflow.com/a/63170705
def get_settings():
    full_file_path = Path(__file__).parent.joinpath("../config.yml")
    with open(full_file_path) as settings:
        settings_data = yaml.load(settings, Loader=yaml.Loader)
    return settings_data


def get_sports_ref_data(cursor, years=range(2000, 2022)):
    power_5 = ["big-12", "acc", "big-ten", "sec", "pac-12", "big-east", "independents"]
    teams = []

    for year in years:
        # conferences in the power 5
        for conf in power_5:
            if conf == "pac-12" and year < 2011:
                conf = "pac-10"
            if conf == "big-east" and year > 2012:
                continue

            # some nonsense for independents schools
            # BYU only relevant ind. team in 2020 (Notre Dame temporarily joined the ACC)
            # Notre Dame & BYU independents from 2011 on (minus 2020)
            # Only Notre Dame prior to 2011
            if conf == "independents" and year == 2020:
                conf_teams = ["brigham-young"]
            elif conf == "independents" and year > 2010:
                conf_teams = ["notre-dame", "brigham-young"]
            elif conf == "independents":
                conf_teams = ["notre-dame"]

            # not independent
            else:
                conference = Conference(conf, str(year))
                conf_teams = conference.teams.keys()
            # go through each team in the conference
            for t in conf_teams:
                team = Team(t.upper(), year=str(year))
                # team name preprocessing
                team_abbrev = team.abbreviation
                if team_abbrev == "LOUISIANA-STATE":
                    team_abbrev = "LSU"
                elif team_abbrev == "MISSISSIPPI":
                    team_abbrev = "OLE-MISS"
                elif team_abbrev == "SOUTHERN-CALIFORNIA":
                    team_abbrev = "USC"
                elif team_abbrev == "PITTSBURGH":
                    team_abbrev = "PITT"
                elif team_abbrev == "TEXAS-CHRISTIAN":
                    team_abbrev = "TCU"
                elif team_abbrev == "BRIGHAM-YOUNG":
                    team_abbrev = "BYU"
                try:
                    d = {
                        "team_id": team_abbrev,
                        "season": str(year),
                        "conference": team.conference,
                        "win_percentage": team.win_percentage,
                        "points_per_game": team.points_per_game,
                        "points_against_per_game": team.points_against_per_game,
                        "strength_of_schedule": team.strength_of_schedule,
                        "simple_rating_system": team.simple_rating_system,
                    }
                    # replace nulls with zeros
                    no_null_d = {k: v or 0 for (k, v) in d.items()}
                    teams.append(no_null_d)
                except:
                    pass

            # write to db
            columns = teams[0].keys()
            query = """INSERT INTO team ({}) VALUES %s
                        ON CONFLICT (team_id, season) DO UPDATE
                        SET
                            team_id = EXCLUDED.team_id,
                            season = EXCLUDED.season,
                            conference = EXCLUDED.conference,
                            win_percentage = EXCLUDED.win_percentage,
                            points_per_game = EXCLUDED.points_per_game,
                            points_against_per_game = EXCLUDED.points_against_per_game,
                            strength_of_schedule = EXCLUDED.strength_of_schedule,
                            simple_rating_system = EXCLUDED.simple_rating_system;
                        """.format(
                ",".join(columns)
            )
            values = [[value for value in team.values()] for team in teams]
            execute_values(cursor, query, values)
            conn.commit()

            print(f"Wrote to db conference {conf} in year {year}")

            teams = []


def main(cursor, years=None):
    if years:
        get_sports_ref_data(cursor, years)
    else:
        get_sports_ref_data(cursor)


if __name__ == "__main__":
    config = get_settings()

    # connection establishment
    conn = psycopg2.connect(
        database=config["db"]["database"],
        user=config["db"]["user"],
        password=config["db"]["password"],
        host=config["db"]["host"],
        port=config["db"]["port"],
    )
    conn.autocommit = True
    cursor = conn.cursor()

    if len(sys.argv) > 1:
        years = [int(sys.argv[1])]
        main(cursor, years)
    else:
        main(cursor)

    # close db
    conn.commit()
    conn.close()
