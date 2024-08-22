'''Process actors and update database.'''
import os
import sqlite3
import time
import logging
from datetime import datetime
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configuration
API_KEY = os.getenv('API_KEY')
BASE_URL = 'https://api.themoviedb.org/3'
DB_PATH = os.path.join(os.path.abspath(
    os.path.dirname(__file__)), 'instance', 'moviedata.db')
REQUEST_DELAY = 2  # delay in seconds
PAGES_TO_FETCH = 21
BATCH_SIZE = 10  # save incremental updates every N pages

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite Setup
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


def create_tables():
    '''Create tables for actors, movies, and roles.'''
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
    '''Remove any existing data during db refresh.'''
    cursor.execute('DELETE FROM roles')
    cursor.execute('DELETE FROM movies')
    cursor.execute('DELETE FROM actors')
    conn.commit()


def fetch_actor_details(person_id):
    '''Get biographical data for given actor.'''
    url = f'{BASE_URL}/person/{person_id}'
    params = {'api_key': API_KEY}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        time.sleep(REQUEST_DELAY)  # Sleep after each request
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(
            "Failed to fetch details for person ID %s: %s", person_id, e)
        return {}


def fetch_movie_credits(person_id):
    '''Get movie credit data for given actor.'''
    url = f'{BASE_URL}/person/{person_id}/movie_credits'
    params = {'api_key': API_KEY}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        time.sleep(REQUEST_DELAY)  # Sleep after each request
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(
            "Failed to fetch movie credits for person ID %s: %s", person_id, e)
        return {}


def calculate_age(birthdate, release_date):
    '''Calculate age of actor given birthdate and release date of movie.'''
    if not birthdate or not release_date:
        return None
    try:
        birth_dt = datetime.strptime(birthdate, '%Y-%m-%d')
        release_dt = datetime.strptime(release_date, '%Y-%m-%d')
        age = release_dt.year - birth_dt.year - \
            ((release_dt.month, release_dt.day) < (birth_dt.month, birth_dt.day))
        return age
    except ValueError:
        return None


def process_actors():
    '''Assemble actor data for database insertion.'''
    logging.info('Reviewing list of actors...')
    with open('popular_actors.txt', 'r', encoding='utf-8') as file:
        actor_id_list = [line.strip() for line in file]

    actors_batch = []
    movies_batch = []
    roles_batch = []
    processed_movies = set()

    for actor_id in actor_id_list:
        # get actor details
        logging.info("Fetching data for actor ID %s...", actor_id)
        actor_details = fetch_actor_details(actor_id)

        person_id = actor_details.get('id')
        person_name = actor_details.get('name')
        profile_path = actor_details.get('profile_path')
        birthdate = actor_details.get('birthday')
        # skip actor if birthdate is missing or invalid
        if not birthdate:
            logging.info(
                "Skipping actor %s due to missing birthdate.", person_name)
            continue

        # append actor details to batch
        logging.info("Processing actor: %s, Birthdate: %s",
                     person_name, birthdate)
        actors_batch.append((person_id, person_name, birthdate, profile_path))

        # get movie credits for actor
        credits_data = fetch_movie_credits(person_id)

        for movie in credits_data.get('cast', []):
            if movie['id'] not in processed_movies:
                movie_id = movie.get('id')
                movie_title = movie.get('title')
                movie_release_date = movie.get('release_date')
                movie_poster_path = movie.get('poster_path')

                movies_batch.append(
                    (movie_id, movie_title, movie_release_date, movie_poster_path))
                processed_movies.add(movie_id)

                actor_age = calculate_age(birthdate, movie_release_date)
                if actor_age is not None:
                    role = (None, person_id, movie_id, actor_age)
                    roles_batch.append(role)

        logging.info("Done processing %s and their movies.", person_name)

    return actors_batch, movies_batch, roles_batch


def save_batch(actors_batch, movies_batch, roles_batch):
    '''Save actor, movie, and role data for insertion.'''
    logging.info("Batch inserting actors...")
    batch_insert('actors', actors_batch)

    logging.info("Batch inserting movies...")
    batch_insert('movies', movies_batch)

    logging.info("Batch inserting roles...")
    batch_insert('roles', roles_batch)


def batch_insert(table, data):
    '''Insert data to database in batches.'''
    if data:
        placeholders = ', '.join(['?' for _ in data[0]])
        query = f'INSERT OR REPLACE INTO {
            table} VALUES ({placeholders})'
        cursor.executemany(query, data)
        conn.commit()


def main():
    '''Program to grab data from TMDB API and insert into local database for web app.'''
    logging.info("Creating tables...")
    create_tables()
    logging.info("Clearing tables...")
    clear_tables()

    logging.info("Fetching all popular actors...")
    actors_batch, movies_batch, roles_batch = process_actors()
    save_batch(actors_batch, movies_batch, roles_batch)

    logging.info("Data refresh complete.")


if __name__ == '__main__':
    main()
