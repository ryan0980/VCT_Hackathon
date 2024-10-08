import requests
import json
import gzip
import shutil
import time
import os
from io import BytesIO

S3_BUCKET_URL = "https://vcthackathon-data.s3.us-west-2.amazonaws.com"
LEAGUE = "vct-international"  # Options: (game-changers, vct-international, vct-challengers)
YEAR = 2024  # Options: (2022, 2023, 2024)

def download_gzip_and_write_to_json(file_name):
    """
    Download a gzip file from S3 and write it as a JSON file.
    Return True if the file is downloaded and written, False otherwise.
    """

    # Skip if file already exists locally
    if os.path.isfile(f"{file_name}.json"):
        return False

    remote_file = f"{S3_BUCKET_URL}/{file_name}.json.gz"
    response = requests.get(remote_file, stream=True)

    if response.status_code == 200:
        gzip_bytes = BytesIO(response.content)
        with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
            with open(f"{file_name}.json", 'wb') as output_file:
                shutil.copyfileobj(gzipped_file, output_file)
            print(f"{file_name}.json written")
        return True
    elif response.status_code == 404:
        # File not found, ignore
        return False
    else:
        print(response)
        print(f"Failed to download {file_name}")
        return False


def download_esports_files():
    """
    Download key esports data files (leagues, tournaments, players, teams, mapping_data)
    and save them locally in the appropriate directory.
    """

    directory = f"{LEAGUE}/esports-data"
    os.makedirs(directory, exist_ok=True)

    esports_data_files = ["leagues", "tournaments", "players", "teams", "mapping_data"]

    for file_name in esports_data_files:
        download_gzip_and_write_to_json(f"{directory}/{file_name}")


def download_games():
    """
    Download individual game files based on the mapping_data file,
    and save them locally in the appropriate directory.
    """

    start_time = time.time()

    # Load the mapping_data.json file to get the list of games
    local_mapping_file = f"{LEAGUE}/esports-data/mapping_data.json"
    with open(local_mapping_file, "r") as json_file:
        mappings_data = json.load(json_file)

    local_directory = f"{LEAGUE}/games/{YEAR}"
    os.makedirs(local_directory, exist_ok=True)

    game_counter = 0

    for esports_game in mappings_data:
        s3_game_file = f"{LEAGUE}/games/{YEAR}/{esports_game['platformGameId']}"

        if download_gzip_and_write_to_json(s3_game_file):
            game_counter += 1
            if game_counter % 10 == 0:
                elapsed_time = round((time.time() - start_time) / 60, 2)
                print(f"----- Processed {game_counter} games, current run time: {elapsed_time} minutes")


if __name__ == "__main__":
    download_esports_files()
    download_games()
