import sqlite3

from flask import Flask, jsonify, request

app = Flask(__name__)

def query_db(query, args=(), one=False):
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@app.route('/movies', methods=['GET'])
def get_movies():
    start_year = request.args.get('start_year')
    end_year = request.args.get('end_year')
    genre = request.args.get('genre')

    query = "SELECT * FROM movies WHERE 1=1"

    if start_year and end_year:
        query += f" AND year BETWEEN {start_year} AND {end_year}"

    if genre:
        query += f" AND genre LIKE '%{genre}%'"

    movies = query_db(query)

    return jsonify(movies)

@app.route('/best_director', methods=['GET'])
def best_director():
    query = """
    SELECT stars, AVG(rating) as avg_rating FROM movies
    GROUP BY stars ORDER BY avg_rating DESC LIMIT 1
    """
    result = query_db(query, one=True)
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
