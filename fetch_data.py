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
    processed_actor_ids = set()

    while page < PAGES_TO_FETCH:
        logging.info("Fetching page %s...", page)
        url = f'{BASE_URL}/person/popular'
        params = {'api_key': API_KEY, 'page': page}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logging.error("Request failed: %s", e)
            break

        if not data['results']:
            break

        for actor in data['results']:
            if actor['id'] not in processed_actor_ids:
                all_actors.append(actor)
                processed_actor_ids.add(actor['id'])

        if page % BATCH_SIZE == 0:
            # Process and save the current batch of actors
            logging.info("Processing batch up to page %s...", page)
            save_batch(all_actors)
            all_actors.clear()  # Clear the list after saving

        total_pages = data.get('total_pages', 1)
        if page >= total_pages:
            break

        page += 1
        logging.info(
            "Waiting for %s seconds before next fetch...", REQUEST_DELAY)
        time.sleep(REQUEST_DELAY)

    # Final save for any remaining actors
    if all_actors:
        save_batch(all_actors)

    return all_actors


def fetch_movie_credits(person_id):
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


def fetch_actor_details(person_id):
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


def calculate_age(birthdate, release_date):
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


def process_actors(all_actors):
    actors_batch = []
    movies_batch = []
    roles_batch = []
    processed_movies = set()

    for actor in all_actors:
        actor_details = fetch_actor_details(actor['id'])
        birthdate = actor_details.get('birthday')

        # Skip actor if birthdate is missing or invalid
        if not birthdate:
            logging.info(
                "Skipping actor %s due to missing birthdate.", actor['name'])
            continue

        logging.info("Processing actor: %s, Birthdate: %s",
                     actor['name'], birthdate)

        truncated_actor = (actor['id'], actor['name'],
                           birthdate, actor.get('profile_path'))
        actors_batch.append(truncated_actor)

        credits_data = fetch_movie_credits(actor['id'])
        for movie in credits_data.get('cast', []):
            if movie['id'] not in processed_movies:
                truncated_movie = (movie['id'], movie['title'], movie.get(
                    'release_date'), movie.get('poster_path'))
                movies_batch.append(truncated_movie)
                processed_movies.add(movie['id'])

            actor_age = calculate_age(birthdate, movie.get('release_date'))
            if actor_age is not None:
                role = (None, actor['id'], movie['id'], actor_age)
                roles_batch.append(role)

        logging.info("Processed actor %s and their movies.", actor['name'])

    return actors_batch, movies_batch, roles_batch


def save_batch(all_actors):
    # Process actors and save to DB
    actors_batch, movies_batch, roles_batch = process_actors(all_actors)

    logging.info('Batch inserting actors...')
    batch_insert('actors', actors_batch)

    logging.info('Batch inserting movies...')
    batch_insert('movies', movies_batch)

    logging.info('Batch inserting roles...')
    batch_insert('roles', roles_batch)


def batch_insert(table, data):
    if data:
        placeholders = ', '.join(['?' for _ in data[0]])
        query = f'INSERT OR REPLACE INTO {table} VALUES ({placeholders})'
        cursor.executemany(query, data)
        conn.commit()


def main():
    create_tables()
    clear_tables()

    logging.info('Fetching all popular actors...')
    fetch_popular_actors()

    logging.info('Data refresh complete.')


if __name__ == '__main__':
    main()
