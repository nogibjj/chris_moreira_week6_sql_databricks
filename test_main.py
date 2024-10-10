from main import main_results


def test_function():
    return main_results()


if __name__ == "__main__":
    assert test_function()["extract_to"] == "data/Spotify_Most_Streamed_Songs.csv"
    assert (
        test_function()["transform_db"] == "SpotifyDB.db"
    )  # Updated to "SpotifyDB.db"
    assert test_function()["create"] == "Create Success"
    assert test_function()["read"] == "Read Success"
    assert test_function()["update"] == "Update Success"
    assert test_function()["delete"] == "Delete Success"
