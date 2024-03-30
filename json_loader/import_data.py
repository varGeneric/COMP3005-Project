import psycopg
import json
import sys
import os
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
		            REFERENCES seasons (season_id. competition_id)
            );"""
        )
        # Create teams table
        cur.execute(
            """CREATE TABLE teams (
                team_id INTEGER,
                team_name VARCHAR(64) UNIQUE,
                PRIMARY KEY (team_id)
            );"""
        )
        # Create players table
        cur.execute(
            """CREATE TABLE players (
                player_id INTEGER,
                player_name VARCHAR(64) UNIQUE,
                team_id INTEGER,
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
                statsbomb_xg NUMERIC(10),
                first_time BOOLEAN,
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
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_id)
		            REFERENCES events (event_id),
                FOREIGN KEY (recipient_player_id)
		            REFERENCES players (player_id)
            );"""
        )
        # Create through_balls table
        cur.execute(
            """CREATE TABLE through_balls (
                event_id VARCHAR(36),
                recipient_player_id INTEGER,
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_id)
		            REFERENCES events (event_id)
            );"""
        )
        # Create dribbles table
        cur.execute(
            """CREATE TABLE dribbles (
                event_id VARCHAR(36),
                nutmeg BOOLEAN,
                outcome_id INTEGER,
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
    included_matches = set()  # Only get events for matches we care about
    
    match_dir = "statsbomb-data/data/matches"
    for competition in os.listdir(match_dir):
        for season_file in os.listdir(os.path.join(match_dir, competition)):
            if competition not in COMPETITIONS_WHITELIST:
                continue
            season_id = int(os.path.splitext(season_file)[0])
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

    # Import events
    events_dir = "statsbomb-data/data/events"
    for event_file_name in os.listdir(events_dir):
        match_id = int(os.path.splitext(event_file_name)[0])
        if match_id not in included_matches:
            continue
        with open(os.path.join(events_dir, event_file_name), 'r') as event_file:
            events_json = json.load(event_file)


if __name__ == "__main__":
    with psycopg.connect(user=db_username, password=db_password, host=db_host, port=db_port, autocommit=True) as conn:
        ensure_database_exists(conn)
    with psycopg.connect(dbname=root_database_name, user=db_username, password=db_password, host=db_host, port=db_port) as conn:
        create_tables(conn)
        # import_data(conn)
        conn.commit()
