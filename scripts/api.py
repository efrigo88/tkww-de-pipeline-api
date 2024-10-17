import ast
import sqlite3
from pathlib import Path

from flask import Flask, jsonify, request

abs_path = Path(__file__).absolute()
base_path = str(abs_path.parent.parent)
db_name = f"{base_path}/tkww_movies_catalog.db"

app = Flask(__name__)

# Helper function to query the SQLite database
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (dict(row) for row in rv) if not one else dict(rv[0])


# 1. Movies between two years
@app.route("/movies_between_years", methods=["GET"])
def get_movies_between_years():
    start_year = request.args.get("start_year")
    end_year = request.args.get("end_year")

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

    query = """
        SELECT *
        FROM movies
        WHERE genre LIKE ?
    """
    movies = query_db(query, ("%" + genre + "%",))
    return jsonify(list(movies))


# 3. Best rated director (in average)
@app.route('/best_director', methods=['GET'])
def best_director():
    query = "SELECT directors, rating FROM movies"
    movies = query_db(query)

    director_ratings = {}

    for movie in movies:
        directors = movie.get('directors')
        rating = movie.get('rating')

        if directors and rating is not None:
            # Convert directors string to list
            for director in ast.literal_eval(directors):
                if director not in director_ratings:
                    director_ratings[director] = {'total_rating': 0, 'movie_count': 0}

                director_ratings[director]['total_rating'] += rating
                director_ratings[director]['movie_count'] += 1

    # Calculate average ratings
    avg_ratings = {
        director: data['total_rating'] / data['movie_count']
        for director, data in director_ratings.items() if data['movie_count'] > 0
    }

    # Find the best director
    best_director = max(avg_ratings, key=avg_ratings.get)
    return jsonify({'best_director': best_director, 'avg_rating': avg_ratings[best_director]})


# 4. Movies from a specific director
@app.route("/movies_by_director", methods=["GET"])
def get_movies_by_director():
    director = request.args.get("director")

    query = """
        SELECT *
        FROM movies
        WHERE directors LIKE ?
    """
    movies = query_db(query, ("%" + director + "%",))

    return jsonify(list(movies))


if __name__ == "__main__":
    app.run(
        host='localhost', 
        port=4000, 
        debug=True
    )
