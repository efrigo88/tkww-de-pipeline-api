import ast
import datetime

from flask import Flask, jsonify, request
from helpers.helpers import query_db, bad_request

app = Flask(__name__)


# 1. Movies between two years
@app.route("/movies_between_years", methods=["GET"])
def get_movies_between_years():
    start_year = request.args.get("start_year")
    end_year = request.args.get("end_year")

    # Validate presence of parameters
    if not start_year or not end_year:
        return bad_request("Both 'start_year' and 'end_year' parameters are required.")

    # Validate the parameters are integers
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except ValueError:
        return bad_request("'start_year' and 'end_year' must be valid integers.")

    # Validate the year range
    current_year = datetime.datetime.now().year
    if start_year < 1800 or end_year > current_year:
        return bad_request(f"Years must be between 1800 and {current_year}.")

    # Validate that start_year is less than or equal to end_year
    if start_year > end_year:
        return bad_request("'start_year' cannot be greater than 'end_year'.")

    query = """
        SELECT *
        FROM movies
        WHERE 1 = 1 
          AND year_from >= ? 
          AND year_to <= ?
    """
    movies = query_db(query, (start_year, end_year))
    return jsonify(list(movies))


# 2. Movies from a specific genre
@app.route("/movies_by_genre", methods=["GET"])
def get_movies_by_genre():
    genre = request.args.get("genre")

    # Validate that the 'genre' parameter is provided
    if not genre:
        return bad_request("'genre' parameter is required.")

    query = "SELECT * FROM movies WHERE genre LIKE ?"
    movies_generator = query_db(query, ("%" + genre + "%",))

    # Convert the generator to a list to check its contents
    movies = list(movies_generator)

    # Check if movies is empty and return a custom message
    if not movies:
        return jsonify({"message": f"No movies found for genre '{genre}'."}), 404
    return jsonify(movies)


# 3. Best rated director (in average)
@app.route("/best_director", methods=["GET"])
def best_director():
    query = "SELECT directors, rating FROM movies"
    movies = query_db(query)

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

    # Validate that the 'genre' parameter is provided
    if not director:
        return bad_request("'director' parameter is required.")

    query = "SELECT * FROM movies WHERE directors LIKE ?"
    movies_generator = query_db(query, (f'%"{director}"%',))

    # Convert the generator to a list to check its contents
    movies = list(movies_generator)

    # Check if movies is empty and return a custom message
    if not movies:
        return jsonify({"message": f"No movies found for director '{director}'."}), 404
    return jsonify(movies)


if __name__ == "__main__":
    app.run(host="localhost", port=4000, debug=True)
