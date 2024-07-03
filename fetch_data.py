import os
import sqlite3
import time
from datetime import datetime
import requests
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()

# Configuration
API_KEY = os.getenv('API_KEY')
BASE_URL = 'https://api.themoviedb.org/3'
DB_PATH = 'moviedata.db'
REQUEST_DELAY = 10  # delay in seconds

# SQLite Setup
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


def create_tables():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS actors (
                   id INTEGER PRIMARY KEY,
                   actor_name TEXT NOT NULL,
                   birthdate TEXT NOT NULL,
                   image_path TEXT
                   )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS movies (
                   id INTEGER PRIMARY KEY,
                   movie_title TEXT NOT NULL,
                   release_date TEXT NOT NULL,
                   poster_path TEXT
                   )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS roles (
                   id INTEGER PRIMARY KEY,
                   actor_id INTEGER,
                   movie_id INTEGER,
                   actor_age INTEGER,
                   FOREIGN KEY (actor_id) REFERENCES actors(id),
                   FOREIGN KEY (movie_id) REFERENCES movies(id)
                   )
    ''')
    conn.commit()


def clear_tables():
    cursor.execute('DELETE FROM roles')
    cursor.execute('DELETE FROM movies')
    cursor.execute('DELETE FROM actors')
    conn.commit()


def fetch_popular_actors():
    page = 1
    all_actors = []
    while page < 2:
        print(f'Fetching page {page}...')
        url = f'{BASE_URL}/person/popular'
        params = {'api_key': API_KEY, 'page': page}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        if not data['results']:
            break
        all_actors.extend(data['results'])
        page += 1
        print(f'Waiting for {REQUEST_DELAY} seconds before next fetch...')
        time.sleep(REQUEST_DELAY)  # wait for specified delay
    return all_actors


def fetch_movie_credits(person_id):
    url = f'{BASE_URL}/person/{person_id}/movie_credits'
    params = {'api_key': API_KEY}
    response = requests.get(url, params=params, timeout=10)
    return response.json()


def fetch_actor_details(person_id):
    url = f'{BASE_URL}/person/{person_id}'
    params = {'api_key': API_KEY}
    response = requests.get(url, params=params, timeout=10)
    return response.json()


def calculate_age(birthdate, release_date):
    if not birthdate or not release_date:  # Check if release_date is None or empty
        return None
    try:
        birth_dt = datetime.strptime(birthdate, '%Y-%m-%d')
        release_dt = datetime.strptime(release_date, '%Y-%m-%d')
        age = release_dt.year - birth_dt.year - \
            ((release_dt.month, release_dt.day) < (birth_dt.month, birth_dt.day))
        return age
    except ValueError:
        return None


def process_actors(all_actors):
    actors_batch = []
    movies_batch = []
    roles_batch = []

    for actor in all_actors:
        actor_details = fetch_actor_details(actor['id'])
        birthdate = actor_details.get('birthday', '1900-01-01')
        truncated_actor = (actor['id'], actor['name'],
                           birthdate, actor.get('profile_path'))
        actors_batch.append(truncated_actor)

        credits_data = fetch_movie_credits(actor['id'])
        for movie in credits_data.get('cast', []):
            truncated_movie = (movie['id'], movie['title'], movie.get(
                'release_date'), movie.get('poster_path'))
            movies_batch.append(truncated_movie)

            actor_age = calculate_age(birthdate, movie.get('release_date'))
            if actor_age is not None:
                # None for auto-incrementing ID
                role = (None, actor['id'], movie['id'], actor_age)
                roles_batch.append(role)

        print(f'Processed actor {actor['name']} and their movies.')

    return actors_batch, movies_batch, roles_batch


def batch_insert(table, data):
    if data:
        placeholders = ', '.join(['?' for _ in data[0]])
        query = f'INSERT OR REPLACE INTO {table} VALUES ({placeholders})'
        cursor.executemany(query, data)
        conn.commit()


def main():
    create_tables()
    clear_tables()

    print('Fetching all popular actors...')
    all_actors = fetch_popular_actors()

    print('Processing actors and their movie credits...')
    actors_batch, movies_batch, roles_batch = process_actors(all_actors)

    print('Batch inserting actors...')
    batch_insert('actors', actors_batch)

    print('Batch inserting movies...')
    batch_insert('movies', movies_batch)

    print('Batch inserting roles...')
    batch_insert('roles', roles_batch)

    print('Data refresh complete.')


if __name__ == '__main__':
    main()
