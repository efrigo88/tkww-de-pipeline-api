import ast
import sqlite3
import datetime
from typing import Any, Dict, Tuple, Union, Generator

from helpers.helpers import db_name
from flask import Flask, Response, jsonify, request

app = Flask(__name__)


class APIService:
    def __init__(self, db_path: str):
        self.db_name = db_path

    def bad_request(self, message: str) -> Response:
        """
        Returns a JSON response with a 400 Bad Request status code.

        Args:
            message (str): The error message to return in the JSON response.

        Returns:
            Response: A Flask Response object containing the error message and a 400 status code.
        """
        response = jsonify({"error": message})
        response.status_code = 400
        return response

    def query_db(
        self, query: str, args: Tuple = (), one: bool = False
    ) -> Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
        """
        Executes a query on the SQLite database and returns the results.

        Args:
            query (str): The SQL query to execute.
            args (Tuple, optional): The parameters to pass to the SQL query. Defaults to an empty tuple.
            one (bool, optional): If True, return only one result as a dictionary. Defaults to False.

        Returns:
            Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
                If `one` is False, returns a generator of dictionaries (each representing a row in the result set).
                If `one` is True, returns a single dictionary representing one row.
        """
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, args)
        rv = cur.fetchall()
        conn.close()

        # If `one` is True, return a single row as a dictionary, else return a generator of dictionaries
        return (dict(row) for row in rv) if not one else dict(rv[0])

    def get_movies_between_years(self, start_year, end_year):
        query = "SELECT * FROM movies WHERE year_from >= ? AND year_to <= ?"
        return self.query_db(query, (start_year, end_year))

    def get_movies_by_genre(self, genre):
        query = "SELECT * FROM movies WHERE genre LIKE ?"
        movies = self.query_db(query, ("%" + genre + "%",))
        # Convert the generator to a list to check its contents
        return list(movies)

    def get_best_director(self):
        query = "SELECT directors, rating FROM movies"
        return self.query_db(query)

    def get_movies_by_director(self, director):
        query = "SELECT * FROM movies WHERE directors LIKE ?"
        movies = self.query_db(query, (f'%"{director}"%',))
        # Convert the generator to a list to check its contents
        return list(movies)


# Flask routes
# 1. Movies between two years
@app.route("/movies_between_years", methods=["GET"])
def get_movies_between_years():
    start_year = request.args.get("start_year")
    end_year = request.args.get("end_year")
    api_service = APIService(db_name)

    # Validate presence of parameters
    if not start_year or not end_year:
        return api_service.bad_request(
            "Both 'start_year' and 'end_year' parameters are required."
        )

    # Validate the parameters are integers
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except ValueError:
        return api_service.bad_request(
            "'start_year' and 'end_year' must be valid integers."
        )

    # Validate the year range
    current_year = datetime.datetime.now().year
    if start_year < 1800 or end_year > current_year:
        return api_service.bad_request(
            f"Years must be between 1800 and {current_year}."
        )

    # Validate that start_year is less than or equal to end_year
    if start_year > end_year:
        return api_service.bad_request(
            "'start_year' cannot be greater than 'end_year'."
        )

    movies = api_service.get_movies_between_years(start_year, end_year)
    return jsonify(list(movies))


# 2. Movies from a specific genre
@app.route("/movies_by_genre", methods=["GET"])
def get_movies_by_genre():
    genre = request.args.get("genre")
    api_service = APIService(db_name)

    # Validate that the 'genre' parameter is provided
    if not genre:
        return api_service.bad_request("'genre' parameter is required.")
    movies = api_service.get_movies_by_genre(genre)

    # Check if movies is empty and return a custom message
    if not movies:
        return jsonify({"message": f"No movies found for genre '{genre}'."}), 404
    return jsonify(movies)


# 3. Best rated director (in average)
@app.route("/best_director", methods=["GET"])
def get_best_director():
    api_service = APIService(db_name)
    movies = api_service.get_best_director()

    director_ratings = {}

    for movie in movies:
        directors = movie.get("directors")
        rating = movie.get("rating")

        if directors and rating is not None:
            # Convert directors string to list
            for director in ast.literal_eval(directors):
                if director not in director_ratings:
                    director_ratings[director] = {"total_rating": 0, "movie_count": 0}

                director_ratings[director]["total_rating"] += rating
                director_ratings[director]["movie_count"] += 1

    # Calculate average ratings
    avg_ratings = {
        director: data["total_rating"] / data["movie_count"]
        for director, data in director_ratings.items()
        if data["movie_count"] > 0
    }

    # Find the best director
    best_director = max(avg_ratings, key=avg_ratings.get)
    return jsonify(
        {"best_director": best_director, "avg_rating": avg_ratings[best_director]}
    )


# 4. Movies from a specific director
@app.route("/movies_by_director", methods=["GET"])
def get_movies_by_director():
    director = request.args.get("director")
    api_service = APIService(db_name)

    # Validate that the 'director' parameter is provided
    if not director:
        return api_service.bad_request("'director' parameter is required.")
    movies = api_service.get_movies_by_director(director)

    # Check if movies is empty and return a custom message
    if not movies:
        return jsonify({"message": f"No movies found for director '{director}'."}), 404
    return jsonify(movies)


if __name__ == "__main__":
    app.run(host="localhost", port=4000, debug=True)
