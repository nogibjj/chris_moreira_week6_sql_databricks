# Query the Database

import sqlite3


def query_create():
    conn = sqlite3.connect("SpotifyDB.db")
    cursor = conn.cursor()

    # Inserting a random row into the SpotifyDB table
    random_record = (
        "Random Song",
        "Random Artist",
        1,
        2024,
        10,
        3,
        400,
        100,
        150000000,
        200,
        "C#",
        "Major",
        60,
        70,
        80,
        10,
        0,
        20,
        5,
        "https://coverurl.com",
    )

    # Insert the record
    cursor.execute(
        """
        INSERT INTO SpotifyDB (
            track_name, artist_name, artist_count, released_year, released_month, 
            released_day, in_spotify_playlists, in_spotify_charts, streams, 
            in_apple_playlists, key, mode, danceability_percent, valence_percent, 
            energy_percent, acousticness_percent, instrumentalness_percent, 
            liveness_percent, speechiness_percent, cover_url
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        random_record,
    )

    conn.commit()
    conn.close()
    return "Create Success"


def query_read():
    conn = sqlite3.connect("SpotifyDB.db")
    cursor = conn.cursor()

    # read execution
    cursor.execute("SELECT * FROM SpotifyDB LIMIT 10")

    conn.close()
    return "Read Success"


def query_update():
    conn = sqlite3.connect("SpotifyDB.db")
    cursor = conn.cursor()

    # Update the artist_name to 'Chris' for the record where id is 3
    cursor.execute(
        """
        UPDATE SpotifyDB 
        SET artist_name = 'Chris' 
        WHERE id = 3
        """
    )

    conn.commit()
    conn.close()
    return "Update Success"


def query_delete():
    conn = sqlite3.connect("SpotifyDB.db")
    cursor = conn.cursor()

    # Delete the record where id is 5
    cursor.execute("DELETE FROM SpotifyDB WHERE id = 5")

    conn.commit()
    conn.close()
    return "Delete Success"


if __name__ == "__main__":
    query_create()
    query_read()
    query_update()
    query_delete()
