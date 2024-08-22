'''Script to fetch archives from TMDB.'''
import os
import logging
import gzip
import shutil
import json
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests

# Load environment variables from .env
load_dotenv()

# Configuration
API_KEY = os.getenv('API_KEY')
DAILY_EXPORT_URL = 'http://files.tmdb.org/p/exports/'
ARCHIVE_DIR = 'archives'

NUMBER_OF_EXPORTS = 3
DAYS_BETWEEN_EXPORTS = 28
REQUEST_DELAY = 3  # delay in seconds

NUMBER_OF_ACTORS = 1000
POPULARITY_CUTOFF = 20.0


# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def generate_export_filenames():
    '''Generate export filenames to fetch.'''
    # filename format = person_ids_MM_DD_YYYY.json.gz
    # start with yesterday's date
    start_date = datetime.today() - timedelta(days=1)
    filenames = []

    # grab the last NUMBER_OF_EXPORTS
    for _ in range(NUMBER_OF_EXPORTS):
        date_str = start_date.strftime('%m_%d_%Y')
        file_url = f'{DAILY_EXPORT_URL}person_ids_{date_str}.json.gz'
        filenames.append(file_url)

        # go back every DAYS_BETWEEN_EXPORTS days
        start_date -= timedelta(days=DAYS_BETWEEN_EXPORTS)

    # return a list of filenames to fetch
    print(filenames)
    return filenames


def download_file_with_progress(filenames):
    '''Download files from provided URL'''
    downloads = []

    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)

    for url in filenames:
        dest_file = url.replace(DAILY_EXPORT_URL, '').replace('.gz', '')
        output_path = f'{ARCHIVE_DIR}/{dest_file}.gz'

        with requests.get(url, stream=True, timeout=7) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            chunk_size = 1024  # 1KB chunks

            with open(output_path, 'wb') as output_file:
                logging.info("Starting download: %s", url)

                downloaded_size = 0
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        output_file.write(chunk)
                        downloaded_size += len(chunk)

                        # print progress
                        progress = (downloaded_size / total_size) * 100
                        print(f"\rDownload progress: {progress:.2f}%", end='')
                logging.info("\nDownload complete")

        # verify download
        if downloaded_size == total_size:
            logging.info("Download verified")
        else:
            logging.error("Download incomplete. %s of %s downloaded.",
                          downloaded_size, total_size)

        downloads.append(output_path)

    logging.info(downloads)
    return downloads


def unzip_archives(downloads):
    '''Unzip relevant files with validation.'''
    unzipped_archives = []

    for file in downloads:
        # designate where to save
        new_file_name = os.path.basename(file).replace('.gz', '')
        archive_path = os.path.join(ARCHIVE_DIR, new_file_name)

        try:
            # decompress the file
            logging.info("Decompressing %s to %s...", file, archive_path)

            with gzip.open(file, 'rb') as gz_file:
                with open(archive_path, 'wb') as output_file:
                    shutil.copyfileobj(gz_file, output_file)

            if os.path.getsize(archive_path) == 0:
                logging.error(
                    "Unzipped file %s is empty, skipping.", archive_path)
                os.remove(archive_path)
            else:
                logging.info("Decompressed %s.", file)
                unzipped_archives.append(archive_path)

        except OSError as e:
            logging.error("Failed to unzip %s: %s", file, e)

    return unzipped_archives


def filter_and_combine(filenames):
    '''Filter and combine JSON data using streaming.'''
    combined_data = []
    logging.info(
        "Streaming popular people into one combined file, please hold...")
    for file_name in filenames:
        with open(file_name, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    record = json.loads(line)
                    if record['popularity'] >= POPULARITY_CUTOFF:
                        combined_data.append(record)
                except json.JSONDecodeError as e:
                    logging.error(
                        "Error decoding JSON in %s: %s", file_name, e)
    logging.info("Finished filtering and combining data.")
    return combined_data

# TODO: experiment with writing directly to output instead of holding in memory:
# maybe define a 'whatever.json' output name and return that if it's not empty
# def filter_and_combine_to_file(filenames, output_file):
#     '''Filter and combine JSON data directly into an output file.'''
#     logging.info(
#         "Streaming popular actors into one combined file, please hold...")
#     with open(output_file, 'w', encoding='utf-8') as outfile:
#         for file_name in filenames:
#             with open(file_name, 'r', encoding='utf-8') as file:
#                 for line in file:
#                     try:
#                         record = json.loads(line)
#                         if record['popularity'] >= POPULARITY_CUTOFF:
#                             outfile.write(json.dumps(record) + '\n')
#                     except json.JSONDecodeError as e:
#                         logging.error(
#                             "Error decoding JSON in %s: %s", file_name, e)
#     logging.info("Finished filtering and combining data.")


def calculate_average_popularity(data):
    '''Calculate average popularity across exported months.'''
    popularity_dict = defaultdict(list)

    # collect popularity values for each ID
    logging.info("Calculating average popularity for actors...")
    for record in data:
        popularity_dict[record['id']].append(record['popularity'])

    # calculate average popularity for each ID
    average_popularity = []
    for actor_id, popularities in popularity_dict.items():
        # assume 0 if missing to penalize less popular people
        avg_pop = sum(popularities) / NUMBER_OF_EXPORTS
        average_popularity.append(
            {'id': actor_id, 'average_popularity': avg_pop})

    logging.info("Writing average popularity data to output file...")
    with open('archives/average_popularity.json', 'w', encoding='utf-8') as output_file:
        for entry in average_popularity:
            json.dump(entry, output_file)
            output_file.write('\n')

    print("Printing sample for average_popularity...")
    print(average_popularity[:5])


def select_popular_actors(input_file, output_file):
    '''Filter for most popular actors.'''
    # load average_popularity data
    logging.info("Filtering actors by their popularity...")
    with open(input_file, 'r', encoding='utf-8') as file:
        data = [json.loads(line) for line in file]

    # sort popularity in descending order
    sorted_data = sorted(
        data, key=lambda x: x['average_popularity'], reverse=True)

    # trim to most popular and grab their ID
    top_actors = sorted_data[:NUMBER_OF_ACTORS]
    popular_actor_ids = [str(actor['id']) for actor in top_actors]

    # save IDs to text file
    logging.info("Writing list of most popular actors...")
    with open(output_file, 'w', encoding='utf-8') as outfile:
        outfile.write('\n'.join(popular_actor_ids))
    logging.info("Completed saving the %s most popular actors.",
                 NUMBER_OF_ACTORS)


def cleanup_temp_files():
    '''Remove archives after processing.'''
    for file in os.listdir(ARCHIVE_DIR):
        file_path = os.path.join(ARCHIVE_DIR, file)
        logging.info("Remember to delete this file: %s", file_path)
        # os.remove(file_path)
    logging.info("Don't for get to clean up!")


print("----------START----------")
archives_to_fetch = generate_export_filenames()
downloaded_archives = download_file_with_progress(archives_to_fetch)
unzipped_files = unzip_archives(downloaded_archives)
combined_archives = filter_and_combine(unzipped_files)
calculate_average_popularity(combined_archives)
select_popular_actors('archives/average_popularity.json', 'popular_actors.txt')
cleanup_temp_files()
print("--------------------")
print("TMDB archive download complete. Actors list ready for processing. Don't forget to clean up!")
print("----------END----------")
