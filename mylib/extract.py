import requests
import os


def extract(
    url="https://raw.githubusercontent.com/nogibjj/chris_moreira_week5_python_sql_db_project/main/data/Spotify_Most_Streamed_Songs.csv",
    file_path="data/Spotify_Most_Streamed_Songs.csv",
    timeout=10,  # Adding a timeout to avoid indefinite hanging
):
    """Extract a url to a file path"""

    # Ensure the 'data' directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Extract the file from the URL with a timeout
    with requests.get(url, timeout=timeout) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    return file_path
