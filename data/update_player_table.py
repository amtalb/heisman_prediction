import sys
import psycopg2
from psycopg2.extras import execute_values
from sportsipy.ncaaf.teams import Team
from sportsipy.ncaaf.conferences import Conference
import cfbd
from cfbd.rest import ApiException
import requests
from bs4 import BeautifulSoup
import yaml
from pathlib import Path


# from https://stackoverflow.com/a/63170705
def get_settings():
    full_file_path = Path(__file__).parent.joinpath("../config.yml")
    with open(full_file_path) as settings:
        settings_data = yaml.load(settings, Loader=yaml.Loader)
    return settings_data


def get_heisman_votes_data(cursor, years=range(2009, 2022)):
    for year in years:
        page = requests.get(
            f"https://www.sports-reference.com/cfb/awards/heisman-{year}.html"
        )
        soup = BeautifulSoup(page.content, "html.parser")
        table = soup.find("table", id="heisman")
        rows = table.tbody.findAll("tr")
        for row in rows:
            player_id = row.findAll("a")[0]["href"].split("/")[-1].split(".")[0]
            votes = int(row.findAll("td")[-2].text)

            sql = f"""UPDATE player
                    SET votes = {votes}
                    WHERE player_id LIKE '{player_id}' AND season = '{year}';"""
            cursor.execute(sql)


def get_sports_ref_data(cursor, years=range(2000, 2022)):
    power_5 = ["big-12", "acc", "big-ten", "sec", "pac-12", "big-east", "independents"]
    players = []

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
                conf_teams = ["Brigham-Young"]
            elif conf == "independents" and year > 2010:
                conf_teams = ["Notre-Dame", "Brigham-Young"]
            elif conf == "independents":
                conf_teams = ["Notre-Dame"]

            else:
                conference = Conference(conf, str(year))
                conf_teams = conference.teams.keys()
            # go through each team in the conference
            for key in conf_teams:
                team = Team(key.upper(), year=str(year))
                roster = team.roster
                # go through each player on the roster
                for player in roster.players:
                    # create dictionary of player stats and append it to the list
                    try:
                        if player(str(year)).season == "Career":
                            continue
                        # team name preprocessing
                        team_abbrev = player(str(year)).team_abbreviation
                        team_abbrev = team_abbrev.replace(" ", "-")
                        team_abbrev = (
                            team_abbrev.replace("(", "")
                            .replace(")", "")
                            .replace("&", "")
                        )
                        team_abbrev = team_abbrev.upper()
                        d = {
                            "player_id": player(str(year)).player_id,
                            "season": player(str(year)).season,
                            "team_abbreviation": team_abbrev,
                            "position": player(str(year)).position,
                            "height": player(str(year)).height,
                            "weight": player(str(year)).weight,
                            "year": player(str(year)).year,
                            "games": player(str(year)).games,
                            "completed_passes": player(str(year)).completed_passes,
                            "pass_attempts": player(str(year)).pass_attempts,
                            "passing_completion": player(str(year)).passing_completion,
                            "passing_yards": player(str(year)).passing_yards,
                            "passing_touchdowns": player(str(year)).passing_touchdowns,
                            "interceptions_thrown": player(
                                str(year)
                            ).interceptions_thrown,
                            "passing_yards_per_attempt": player(
                                str(year)
                            ).passing_yards_per_attempt,
                            "adjusted_yards_per_attempt": player(
                                str(year)
                            ).adjusted_yards_per_attempt,
                            "quarterback_rating": player(str(year)).quarterback_rating,
                            "rush_attempts": player(str(year)).rush_attempts,
                            "rush_yards": player(str(year)).rush_yards,
                            "rush_yards_per_attempt": player(
                                str(year)
                            ).rush_yards_per_attempt,
                            "rush_touchdowns": player(str(year)).rush_touchdowns,
                            "receptions": player(str(year)).receptions,
                            "receiving_yards": player(str(year)).receiving_yards,
                            "receiving_yards_per_reception": player(
                                str(year)
                            ).receiving_yards_per_reception,
                            "receiving_touchdowns": player(
                                str(year)
                            ).receiving_touchdowns,
                            "plays_from_scrimmage": player(
                                str(year)
                            ).plays_from_scrimmage,
                            "yards_from_scrimmage": player(
                                str(year)
                            ).yards_from_scrimmage,
                            "yards_from_scrimmage_per_play": player(
                                str(year)
                            ).yards_from_scrimmage_per_play,
                            "rushing_and_receiving_touchdowns": player(
                                str(year)
                            ).rushing_and_receiving_touchdowns,
                            "solo_tackles": player(str(year)).solo_tackles,
                            "assists_on_tackles": player(str(year)).assists_on_tackles,
                            "total_tackles": player(str(year)).total_tackles,
                            "tackles_for_loss": player(str(year)).tackles_for_loss,
                            "sacks": player(str(year)).sacks,
                            "interceptions": player(str(year)).interceptions,
                            "yards_returned_from_interceptions": player(
                                str(year)
                            ).yards_returned_from_interceptions,
                            "yards_returned_per_interception": player(
                                str(year)
                            ).yards_returned_per_interception,
                            "interceptions_returned_for_touchdown": player(
                                str(year)
                            ).interceptions_returned_for_touchdown,
                            "passes_defended": player(str(year)).passes_defended,
                            "fumbles_recovered": player(str(year)).fumbles_recovered,
                            "yards_recovered_from_fumble": player(
                                str(year)
                            ).yards_recovered_from_fumble,
                            "fumbles_recovered_for_touchdown": player(
                                str(year)
                            ).fumbles_recovered_for_touchdown,
                            "fumbles_forced": player(str(year)).fumbles_forced,
                            "punt_return_touchdowns": player(
                                str(year)
                            ).punt_return_touchdowns,
                            "kickoff_return_touchdowns": player(
                                str(year)
                            ).kickoff_return_touchdowns,
                            "other_touchdowns": player(str(year)).other_touchdowns,
                            "total_touchdowns": player(str(year)).total_touchdowns,
                            "extra_points_made": player(str(year)).extra_points_made,
                            "field_goals_made": player(str(year)).field_goals_made,
                            "extra_points_attempted": player(
                                str(year)
                            ).extra_points_attempted,
                            "extra_point_percentage": player(
                                str(year)
                            ).extra_point_percentage,
                            "field_goals_attempted": player(
                                str(year)
                            ).field_goals_attempted,
                            "field_goal_percentage": player(
                                str(year)
                            ).field_goal_percentage,
                            "two_point_conversions": player(
                                str(year)
                            ).two_point_conversions,
                            "safeties": player(str(year)).safeties,
                            "points": player(str(year)).points,
                        }
                        # replace nulls with zeros
                        no_null_d = {k: v or 0 for (k, v) in d.items()}
                        players.append(no_null_d)
                    except:
                        pass

                # remove duplicates
                dedup_players = [dict(t) for t in {tuple(d.items()) for d in players}]
                # write to db
                columns = players[0].keys()
                query = """INSERT INTO player ({}) VALUES %s 
                            ON CONFLICT (player_id, season) DO UPDATE 
                            SET 
                                player_id = EXCLUDED.player_id,
                                season = EXCLUDED.season,
                                team_abbreviation = EXCLUDED.team_abbreviation,
                                position = EXCLUDED.position,
                                height = EXCLUDED.height,
                                weight = EXCLUDED.weight,
                                year = EXCLUDED.year,
                                games = EXCLUDED.games,
                                completed_passes = EXCLUDED.completed_passes,
                                pass_attempts = EXCLUDED.pass_attempts,
                                passing_completion = EXCLUDED.passing_completion,
                                passing_yards = EXCLUDED.passing_yards,
                                passing_touchdowns = EXCLUDED.passing_touchdowns,
                                interceptions_thrown = EXCLUDED.interceptions_thrown,
                                passing_yards_per_attempt = EXCLUDED.passing_yards_per_attempt,
                                adjusted_yards_per_attempt = EXCLUDED.adjusted_yards_per_attempt,
                                quarterback_rating = EXCLUDED.quarterback_rating,
                                rush_attempts = EXCLUDED.rush_attempts,
                                rush_yards = EXCLUDED.rush_yards,
                                rush_yards_per_attempt = EXCLUDED.rush_yards_per_attempt,
                                rush_touchdowns = EXCLUDED.rush_touchdowns,
                                receptions = EXCLUDED.receptions,
                                receiving_yards = EXCLUDED.receiving_yards,
                                receiving_yards_per_reception = EXCLUDED.receiving_yards_per_reception,
                                receiving_touchdowns = EXCLUDED.receiving_touchdowns,
                                plays_from_scrimmage = EXCLUDED.plays_from_scrimmage,
                                yards_from_scrimmage = EXCLUDED.yards_from_scrimmage,
                                yards_from_scrimmage_per_play = EXCLUDED.yards_from_scrimmage_per_play,
                                rushing_and_receiving_touchdowns = EXCLUDED.rushing_and_receiving_touchdowns,
                                solo_tackles = EXCLUDED.solo_tackles,
                                assists_on_tackles = EXCLUDED.assists_on_tackles,
                                total_tackles = EXCLUDED.total_tackles,
                                tackles_for_loss = EXCLUDED.tackles_for_loss,
                                sacks = EXCLUDED.sacks,
                                interceptions = EXCLUDED.interceptions,
                                yards_returned_from_interceptions = EXCLUDED.yards_returned_from_interceptions,
                                yards_returned_per_interception = EXCLUDED.yards_returned_per_interception,
                                interceptions_returned_for_touchdown = EXCLUDED.interceptions_returned_for_touchdown,
                                passes_defended = EXCLUDED.passes_defended,
                                fumbles_recovered = EXCLUDED.fumbles_recovered,
                                yards_recovered_from_fumble = EXCLUDED.yards_recovered_from_fumble,
                                fumbles_recovered_for_touchdown = EXCLUDED.fumbles_recovered_for_touchdown,
                                fumbles_forced = EXCLUDED.fumbles_forced,
                                punt_return_touchdowns = EXCLUDED.punt_return_touchdowns,
                                kickoff_return_touchdowns = EXCLUDED.kickoff_return_touchdowns,
                                other_touchdowns = EXCLUDED.other_touchdowns,
                                total_touchdowns = EXCLUDED.total_touchdowns,
                                extra_points_made = EXCLUDED.extra_points_made,
                                field_goals_made = EXCLUDED.field_goals_made,
                                extra_points_attempted = EXCLUDED.extra_points_attempted,
                                extra_point_percentage = EXCLUDED.extra_point_percentage,
                                field_goals_attempted = EXCLUDED.field_goals_attempted,
                                field_goal_percentage = EXCLUDED.field_goal_percentage,
                                two_point_conversions = EXCLUDED.two_point_conversions,
                                safeties = EXCLUDED.safeties,
                                points = EXCLUDED.points;
                        """.format(
                    ",".join(columns)
                )
                values = [
                    [value for value in player.values()] for player in dedup_players
                ]
                execute_values(cursor, query, values)
                conn.commit()

                players = []


# def get_cfbd_data(cursor, year):
#     # Configure API key authorization: ApiKeyAuth
#     configuration = cfbd.Configuration()
#     configuration.api_key[
#         "Authorization"
#     ] = "S1+WAx+zu3+ab1Ueby09xmc2Wr5F54159c2WnvuoLxHot9wfOsVkRnnYDv+NahVc"
#     configuration.api_key_prefix["Authorization"] = "Bearer"

#     # create an instance of the API class
#     player_api_instance = cfbd.PlayersApi(cfbd.ApiClient(configuration))
#     metrics_api_instance = cfbd.MetricsApi(cfbd.ApiClient(configuration))

#     # get player ids

#     for idx, row in df.iterrows():
#         search_term = row["name"]
#         year = row["season"]
#         team = row["team"]
#         position = row["position"]

#     # find player id
#     try:
#         api_response = player_api_instance.player_search(
#             search_term, position=position, team=team, year=year
#         )
#         player_id = api_response[0]["id"]
#         row["playerId"] = player_id

#         # get usage stats
#         try:
#             api_response = player_api_instance.get_player_usage(
#                 playerId=player_id, year=year, team=team, position=position
#             )
#             row["usage_overall"] = api_response[0]["usage"]["overall"]
#             row["usage_pass"] = api_response[0]["usage"]["pass"]
#             row["usage_rush"] = api_response[0]["usage"]["rush"]
#             row["usage_firstDown"] = api_response[0]["usage"]["firstDown"]
#             row["usage_secondDown"] = api_response[0]["usage"]["secondDown"]
#             row["usage_thirdDown"] = api_response[0]["usage"]["thirdDown"]
#             row["usage_standardDowns"] = api_response[0]["usage"]["standardDowns"]
#             row["usage_passingDowns"] = api_response[0]["usage"]["passingDowns"]
#         except ApiException as e:
#             print("Exception when calling PlayersApi->get_player_usage: %s\n" % e)

#         # get predicted points added stats (PPA)
#         try:
#             api_response = metrics_api_instance.get_player_season_ppa(
#                 playerId=player_id, year=year, team=team, position=position
#             )
#             row["averagePPA_all"] = api_response[0]["averagePPA"]["all"]
#             row["averagePPA_pass"] = api_response[0]["averagePPA"]["pass"]
#             row["averagePPA_rush"] = api_response[0]["averagePPA"]["rush"]
#             row["averagePPA_firstDown"] = api_response[0]["averagePPA"]["firstDown"]
#             row["averagePPA_secondDown"] = api_response[0]["averagePPA"]["secondDown"]
#             row["averagePPA_thirdDown"] = api_response[0]["averagePPA"]["thirdDown"]
#             row["averagePPA_standardDowns"] = api_response[0]["averagePPA"][
#                 "standardDowns"
#             ]
#             row["averagePPA_passingDowns"] = api_response[0]["averagePPA"][
#                 "passingDowns"
#             ]
#         except ApiException as e:
#             print("Exception when calling PlayersApi->get_player_usage: %s\n" % e)

#     except ApiException as e:
#         print("Exception when calling PlayersApi->player_search: %s\n" % e)


def main(cursor, years=None):
    if years:
        get_sports_ref_data(cursor, years)
        get_heisman_votes_data(cursor, years)
    else:
        get_sports_ref_data(cursor)
        get_heisman_votes_data(cursor)
    # get_cfbd_data(cursor, year)


if __name__ == "__main__":

    config = get_settings()

    # connect to db
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
