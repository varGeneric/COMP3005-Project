import psycopg
import json
import sys
import os
# Do some directory hacking to import values from queries.py
directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(directory)
from queries import db_host, db_password, db_port, db_username, root_database_name

def ensure_database_exists(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {root_database_name};")
        cur.execute(f"""
            CREATE DATABASE {root_database_name}
                WITH
                OWNER = {db_username}
                ENCODING = 'UTF8'
                CONNECTION LIMIT = -1
                IS_TEMPLATE = False;
        """)
        cur.execute("""
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            GRANT ALL ON SCHEMA public TO postgres;
            GRANT ALL ON SCHEMA public TO public;
        """)


def create_tables(conn):
    with conn.cursor() as cur:
        # Create competitions table
        cur.execute(
            """CREATE TABLE competitions (
                competition_id INTEGER,
                country_name VARCHAR(32) NOT NULL,
                competition_name VARCHAR(32) NOT NULL,
                competition_gender VARCHAR(8) NOT NULL,
                competition_youth BOOLEAN,
                competition_international BOOLEAN,
                -- match_updated
                -- match_updated_360
                -- match_available_360
                -- match_available
                PRIMARY KEY (competition_id)
            );"""
        )
        # Create seasons table
        cur.execute(
            """CREATE TABLE seasons (
                season_id INTEGER,
                competition_id INTEGER,
                season_name VARCHAR(32) NOT NULL,
                PRIMARY KEY (season_id, competition_id),
                FOREIGN KEY (competition_id)
		            REFERENCES competitions (competition_id)
            );"""
        )
        # Create matches table
        cur.execute(
            """CREATE TABLE matches (
                match_id INTEGER,
                season_id INTEGER,
                competition_id INTEGER,
                PRIMARY KEY (match_id),
                FOREIGN KEY (competition_id)
		            REFERENCES competitions (competition_id),
                FOREIGN KEY (season_id, competition_id)
		            REFERENCES seasons (season_id, competition_id)
            );"""
        )
        # Create teams table
        cur.execute(
            """CREATE TABLE teams (
                team_id INTEGER,
                team_name VARCHAR(64) UNIQUE NOT NULL,
                PRIMARY KEY (team_id)
            );"""
        )
        # Create players table
        cur.execute(
            """CREATE TABLE players (
                player_id INTEGER,
                player_name VARCHAR(64) UNIQUE NOT NULL,
                team_id INTEGER NOT NULL,
                PRIMARY KEY (player_id),
                FOREIGN KEY (team_id)
		            REFERENCES teams (team_id)
            );"""
        )
        # Create events table
        cur.execute(
            """CREATE TABLE events (
                event_id VARCHAR(36),
                event_type_id INTEGER,
                match_id INTEGER,
                player_id INTEGER,
                PRIMARY KEY (event_id),
                FOREIGN KEY (match_id)
		            REFERENCES matches (match_id),
                FOREIGN KEY (player_id)
		            REFERENCES players (player_id)
            );"""
        )
        # Create shots table
        cur.execute(
            """CREATE TABLE shots (
                event_id VARCHAR(36),
                statsbomb_xg NUMERIC(10,10),
                first_time BOOLEAN DEFAULT FALSE NOT NULL,
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_id)
		            REFERENCES events (event_id)
            );"""
        )
        # Create passes table
        cur.execute(
            """CREATE TABLE passes (
                event_id VARCHAR(36),
                recipient_player_id INTEGER,
                succeeded BOOLEAN DEFAULT TRUE NOT NULL,
                through_ball BOOLEAN DEFAULT FALSE NOT NULL,
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_id)
		            REFERENCES events (event_id),
                FOREIGN KEY (recipient_player_id)
		            REFERENCES players (player_id)
            );"""
        )
        # Create dribbles table
        cur.execute(
            """CREATE TABLE dribbles (
                event_id VARCHAR(36),
                nutmeg BOOLEAN DEFAULT FALSE NOT NULL,
                outcome_id INTEGER,
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_id)
		            REFERENCES events (event_id)
            );"""
        )
        # Create dribbled past table
        cur.execute(
            """CREATE TABLE dribble_past (
                event_id VARCHAR(36),
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_id)
		            REFERENCES events (event_id)
            );"""
        )



def import_data(conn):
    cur = conn.cursor()
    # Import competitions
    with open("statsbomb-data/data/competitions.json", 'r') as competitions_json_file:
        competitions_json = json.load(competitions_json_file)
        for competition in competitions_json:
            # Insert competition
            cur.execute(
                "INSERT INTO competitions (competition_id, country_name, competition_name, competition_gender, competition_youth, competition_international) "
                f"VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (competition_id) DO NOTHING;",
                (
                    competition["competition_id"],
                    competition["country_name"],
                    competition["competition_name"],
                    competition["competition_gender"],
                    competition["competition_youth"],
                    competition["competition_international"]
                )
            )
            # Insert season
            cur.execute(
                "INSERT INTO seasons (season_id, season_name, competition_id) "
                f"VALUES (%s, %s, %s);",
                (
                    competition["season_id"],
                    competition["season_name"],
                    competition["competition_id"],
                )
            )

    # Import matches
    COMPETITIONS_WHITELIST = ["2", "11"]  # Premier League and La Liga
    SEASONS_WHITELIST = ["44", "90", "42", "4"]
    included_matches = set()  # Only get events for matches we care about
    
    match_dir = "statsbomb-data/data/matches"
    for competition in os.listdir(match_dir):
        for season_file in os.listdir(os.path.join(match_dir, competition)):
            season = os.path.splitext(season_file)[0]
            if competition not in COMPETITIONS_WHITELIST \
                or season not in SEASONS_WHITELIST:
                continue
            season_id = int(season)
            print(f"Importing match data for competition {competition}, season {season_id}")
            with open(os.path.join(match_dir, competition, season_file)) as matches_json_file:
                matches = json.load(matches_json_file)
                for match in matches:
                    included_matches.add(match["match_id"])
                    # Insert match
                    cur.execute(
                        "INSERT INTO matches (match_id, competition_id, season_id) "
                        f"VALUES (%s, %s, %s);",
                        (
                            match["match_id"],
                            match["competition"]["competition_id"],
                            match["season"]["season_id"],
                        )
                    )
                    # Insert team
                    cur.execute(
                        "INSERT INTO teams (team_id, team_name) "
                        f"VALUES (%s, %s) "
                        "ON CONFLICT (team_id) DO NOTHING;",
                        (
                            match["home_team"]["home_team_id"],
                            match["home_team"]["home_team_name"]
                        )
                    )
                    cur.execute(
                        "INSERT INTO teams (team_id, team_name) "
                        f"VALUES (%s, %s) "
                        "ON CONFLICT (team_id) DO NOTHING;",
                        (
                            match["away_team"]["away_team_id"],
                            match["away_team"]["away_team_name"]
                        )
                    )

    # Import lineups
    lineups_dir = "statsbomb-data/data/lineups"
    for lineup_for_match_file in os.listdir(lineups_dir):
        match_id = int(os.path.splitext(lineup_for_match_file)[0])
        if match_id not in included_matches:
            continue
        with open(os.path.join(lineups_dir, lineup_for_match_file), 'r') as lineup_file:
            lineups = json.load(lineup_file)
            for lineup in lineups:
                team_id = lineup["team_id"]
                for player in lineup["lineup"]:
                    # Insert player
                    cur.execute(
                        "INSERT INTO players (player_id, player_name, team_id) "
                        f"VALUES (%s, %s, %s) "
                        "ON CONFLICT (player_id) DO NOTHING;",
                        (
                            player["player_id"],
                            player["player_name"],
                            team_id
                        )
                    )
        

    # Import events
    events_dir = "statsbomb-data/data/events"
    for event_file_name in os.listdir(events_dir):
        match_id = int(os.path.splitext(event_file_name)[0])
        if match_id not in included_matches:
            continue
        with open(os.path.join(events_dir, event_file_name), 'r') as event_file:
            events_json = json.load(event_file)
            for event in events_json:
                # Insert event
                cur.execute(
                    "INSERT INTO events (event_id, event_type_id, match_id, player_id) "
                    f"VALUES (%s, %s, %s, %s);",
                    (
                        event["id"],
                        event["type"]["id"],
                        match_id,
                        event["player"]["id"] if event.get("player") is not None else None
                    )
                )
                if event["type"]["id"] == 16:  # Shot
                    # Insert shot
                    cur.execute(
                        "INSERT INTO shots (event_id, statsbomb_xg, first_time) "
                        f"VALUES (%s, %s, %s);",
                        (
                            event["id"],
                            event["shot"]["statsbomb_xg"],
                            (event["shot"].get("first_time") is not None) and (event["shot"]["first_time"])
                        )
                    )
                elif event["type"]["id"] == 14:  # Dribble
                    # Insert dribble
                    cur.execute(
                        "INSERT INTO dribbles (event_id, nutmeg, outcome_id) "
                        f"VALUES (%s, %s, %s);",
                        (
                            event["id"],
                            (event["dribble"].get("nutmeg") is not None) and (event["dribble"]["nutmeg"]),
                            event["dribble"]["outcome"]["id"]
                        )
                    )
                elif event["type"]["id"] == 39:  # Dribbled Past
                    # Insert dribbled past
                    cur.execute(
                        "INSERT INTO dribble_past (event_id) "
                        f"VALUES (%s);",
                        (
                            event["id"],
                        )
                    )
                elif event["type"]["id"] == 30:  # Pass
                    # Insert pass
                    cur.execute(
                        "INSERT INTO passes (event_id, recipient_player_id, through_ball, succeeded) "
                        f"VALUES (%s, %s, %s, %s);",
                        (
                            event["id"],
                            event["pass"]["recipient"]["id"] if event["pass"].get("recipient") is not None else None,
                            (event["pass"].get("through_ball") is not None) and (event["pass"]["through_ball"]),
                            False if event["pass"].get("outcome") else True
                        )
                    )


if __name__ == "__main__":
    with psycopg.connect(user=db_username, password=db_password, host=db_host, port=db_port, autocommit=True) as conn:
        ensure_database_exists(conn)
    with psycopg.connect(dbname=root_database_name, user=db_username, password=db_password, host=db_host, port=db_port) as conn:
        create_tables(conn)
        import_data(conn)
        conn.commit()
