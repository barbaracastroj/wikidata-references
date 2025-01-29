import gzip
import os
import requests
import time
import tldextract


from itertools import islice
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter


WARC_PATHS = "warc.paths.gz"
COMMON_CRAWL_URL = "https://data.commoncrawl.org"
TOP_DOMAINS_URL = "../domains/top_domains.txt"
CHECKPOINT_FILE = "last_warc_path_processed.txt"


def read_top_domains(file_path, max_lines=None):
    top_domains = []

    with open(file_path, "r", encoding="utf-8") as infile:
        if max_lines is None:
            top_domains = [line.strip() for line in infile]
        else:
            top_domains = [line.strip() for line in islice(infile, max_lines)]

    return top_domains


def filter_warc_documents(input_file, output_file):

    print(f"Filtrando archivo {input_file}")

    start_time = time.time()
    filtered_count = 0

    domains = read_top_domains(TOP_DOMAINS_URL, 1000)
    unique_domains = set(domains)

    with gzip.open(input_file, "rb") as input_f, gzip.open(
        output_file, "wb"
    ) as output_f:
        writer = WARCWriter(output_f)
        for record in ArchiveIterator(input_f):
            warc_type = record.rec_headers.get_header("WARC-Type")

            if warc_type == "response":
                uri = record.rec_headers.get_header("WARC-Target-URI")

                if uri is not None:
                    extracted = tldextract.extract(uri)
                    registered_domain = f"{extracted.domain}.{extracted.suffix}"

                    if registered_domain and registered_domain in unique_domains:
                        try:
                            writer.write_record(record)
                            filtered_count += 1
                        except UnicodeEncodeError as e:
                            continue

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Documentos filtrados: {filtered_count}")
    print(f"Tiempo filtrado: {elapsed_time:.6f} segundos")

    return filtered_count


def save_checkpoint(last_warc_path):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(last_warc_path)


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return None


def download_file(url, output_file):

    print(f"Descargando archivo {url}")
    start_time = time.time()

    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(output_file, "wb") as warc_file:
                for chunk in response.iter_content(chunk_size=8192):
                    warc_file.write(chunk)

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tiempo descarga: {elapsed_time}")
            return True

        else:
            print(f"Error al descargar {url}. Status code: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Error de conexión: {e}")
        return False


def download_warc_files(warc_paths, max_paths=None):
    start_time = time.time()

    last_processed_file = load_checkpoint()
    found_checkpoint = last_processed_file is None

    with gzip.open(warc_paths, "rt") as infile:
        counter = 0

        for warc_path in islice(infile, max_paths or None):
            if last_processed_file and not found_checkpoint:
                if warc_path.strip() == last_processed_file:
                    found_checkpoint = True
                continue

            output_file = f"warc-docs-1000/{warc_path.strip().replace('/', '-')}"
            url = f"{COMMON_CRAWL_URL}/{warc_path.strip()}"

            success = download_file(url, output_file)

            if success:
                count = filter_warc_documents(
                    output_file,
                    f"warc-docs-1000/filtered-{os.path.basename(output_file)}",
                )
                counter += count

                save_checkpoint(warc_path)
                os.remove(output_file)

            else:
                print(f"Error al descargar el archivo {url}")
                break

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo total: {elapsed_time}")


if __name__ == "__main__":
    download_warc_files(WARC_PATHS, 100)
