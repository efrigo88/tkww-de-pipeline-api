import ast
import sqlite3
import datetime
from typing import Any, Dict, List, Tuple, Union, Generator

from flasgger import Swagger
from helpers.helpers import db_name
from flask import Flask, Response, jsonify, request
from pydantic import BaseModel, conint, validator, root_validator

# Initialize Flask and Swagger
app = Flask(__name__)
swagger = Swagger(app)


# Pydantic models for request validation
class MoviesBetweenYearsRequest(BaseModel):
    current_year = datetime.datetime.now().year
    start_year: conint(ge=1800, le=current_year)  # type: ignore
    end_year: conint(ge=1800, le=current_year)  # type: ignore

    @root_validator(pre=True)
    def check_years(cls, values: dict[str, Any]) -> dict[str, Any]:
        start_year = values.get("start_year")
        end_year = values.get("end_year")

        # Ensure end_year is not smaller than start_year
        if end_year and start_year and end_year < start_year:
            raise ValueError("'end_year' cannot be smaller than 'start_year'.")
        return values


class MoviesByGenreRequest(BaseModel):
    genre: str

    @validator("genre")
    def check_genre_not_empty(cls, value):
        if not value or value.strip() == "":
            raise ValueError("'genre' parameter cannot be empty.")
        return value


class MoviesByDirectorRequest(BaseModel):
    director: str

    @validator("director")
    def check_director_not_empty(cls, value):
        if not value or value.strip() == "":
            raise ValueError("'director' parameter cannot be empty.")
        return value


class APIService:
    def __init__(self, db_path: str):
        self.db_name = db_path

    def bad_request(self, message: str) -> Response:
        response = jsonify({"error": message})
        response.status_code = 400
        return response

    def query_db(
        self, query: str, args: Tuple = (), one: bool = False
    ) -> Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, args)
        rv = cur.fetchall()
        conn.close()

        # If `one` is True, return a single row as a dictionary, else return a generator of dictionaries
        return (dict(row) for row in rv) if not one else dict(rv[0])

    def get_movies_between_years(
        self, start_year: int, end_year: int
    ) -> Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
        query = "SELECT * FROM movies WHERE year_from >= ? AND year_to <= ?"
        return self.query_db(query, (start_year, end_year))

    def get_movies_by_genre(self, genre: str) -> List[Dict[str, Any]]:
        query = "SELECT * FROM movies WHERE genre LIKE ?"
        movies = self.query_db(query, ("%" + genre + "%",))
        # Convert the generator to a list to check its contents
        return list(movies)

    def get_best_director(
        self,
    ) -> Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
        query = "SELECT directors, rating FROM movies"
        return self.query_db(query)

    def get_movies_by_director(self, director: str) -> List[Dict[str, Any]]:
        query = "SELECT * FROM movies WHERE directors LIKE ?"
        movies = self.query_db(query, (f'%"{director}"%',))
        # Convert the generator to a list to check its contents
        return list(movies)


# Flask routes
@app.route("/movies_between_years", methods=["GET"])
def get_movies_between_years():
    """Get movies between two years
    ---
    parameters:
      - name: start_year
        in: query
        type: integer
        required: true
      - name: end_year
        in: query
        type: integer
        required: true
    responses:
      200:
        description: A list of movies
        schema:
          type: array
          items:
            type: object
      400:
        description: Bad Request
    """
    try:
        request_data = MoviesBetweenYearsRequest(
            start_year=request.args.get("start_year"),
            end_year=request.args.get("end_year"),
        )
    except ValueError as e:
        return APIService(db_name).bad_request(str(e))

    movies = APIService(db_name).get_movies_between_years(
        request_data.start_year, request_data.end_year
    )
    return jsonify(list(movies))


@app.route("/movies_by_genre", methods=["GET"])
def get_movies_by_genre():
    """Get movies by genre
    ---
    parameters:
      - name: genre
        in: query
        type: string
        required: true
    responses:
      200:
        description: A list of movies of the specified genre
        schema:
          type: array
          items:
            type: object
      400:
        description: Bad Request
    """
    try:
        request_data = MoviesByGenreRequest(genre=request.args.get("genre"))
    except ValueError as e:
        return APIService(db_name).bad_request(str(e))

    movies = APIService(db_name).get_movies_by_genre(request_data.genre)
    return jsonify(movies)


@app.route("/best_director", methods=["GET"])
def get_best_director():
    """Best rated director (in average)
    ---
    responses:
      200:
        description: Best rated director
        schema:
          type: array
          items:
            type: object
      400:
        description: Bad Request
    """
    movies = APIService(db_name).get_best_director()

    director_ratings = {}

    # Extract directors to cumulate ratings and movies they are in.
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

    # Calculate average ratings for each director.
    avg_ratings = {
        director: data["total_rating"] / data["movie_count"]
        for director, data in director_ratings.items()
        if data["movie_count"] > 0
    }

    # Find the best director.
    best_director = max(avg_ratings, key=avg_ratings.get)
    return jsonify(
        {"best_director": best_director, "avg_rating": avg_ratings[best_director]}
    )


@app.route("/movies_by_director", methods=["GET"])
def get_movies_by_director():
    """Get movies by director
    ---
    parameters:
      - name: director
        in: query
        type: string
        required: true
    responses:
      200:
        description: A list of movies directed by the specified director
        schema:
          type: array
          items:
            type: object
      400:
        description: Bad Request
    """
    try:
        request_data = MoviesByDirectorRequest(director=request.args.get("director"))
    except ValueError as e:
        return APIService(db_name).bad_request(str(e))

    movies = APIService(db_name).get_movies_by_director(request_data.director)
    return jsonify(movies)


if __name__ == "__main__":
    app.run(host="localhost", port=4000, debug=True)
