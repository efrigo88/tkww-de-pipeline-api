import sqlite3
from pathlib import Path

from flask import Flask, jsonify, request

abs_path = Path(__file__).absolute()
base_path = str(abs_path.parent.parent)

DEFAULT_FLASK_PORT = 5000
db_name = f"{base_path}/tkww_movies_catalog.db"
app = Flask(__name__)

# Helper function to query the SQLite database
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv


# 1. Movies between two years
@app.route("/movies_between_years", methods=["GET"])
def get_movies_between_years():
    start_year = request.args.get("start_year")
    end_year = request.args.get("end_year")

    query = """
        SELECT *
        FROM movies
        WHERE year_from BETWEEN ? AND ?
    """
    movies = query_db(query, (start_year, end_year))

    return jsonify(movies)


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

    return jsonify(movies)


# 3. Best rated director (in average)
@app.route("/best_director", methods=["GET"])
def get_best_director():
    query = """
        SELECT directors, AVG(rating) as avg_rating
        FROM movies
        GROUP BY directors
        ORDER BY avg_rating DESC
        LIMIT 1
    """
    best_director = query_db(query, one=True)
    return jsonify(best_director)


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

    return jsonify(movies)


if __name__ == "__main__":
    app.run(debug=True, port=DEFAULT_FLASK_PORT)
