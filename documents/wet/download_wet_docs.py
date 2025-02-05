import gzip
import os
import requests
import time
import tldextract


from itertools import islice
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter


WET_PATHS = "wet.paths.gz"
COMMON_CRAWL_URL = "https://data.commoncrawl.org"
TOP_DOMAINS_URL = "../../domains/top_domains.txt"
CHECKPOINT_FILE = "last_path_processed.txt"


def read_top_domains(file_path, max_lines=None):
    top_domains = []

    with open(file_path, "r", encoding="utf-8") as in_file:
        if max_lines is None:
            top_domains = [line.strip() for line in in_file]
        else:
            top_domains = [line.strip() for line in islice(in_file, max_lines)]

    return top_domains


def filter_wet_documents(input_file, output_file, domains):

    print(f"Filtrando archivo {input_file}")

    start_time = time.time()
    filtered_count = 0

    with gzip.open(input_file, "rb") as in_file, gzip.open(
        output_file, "wb"
    ) as out_file:

        writer = WARCWriter(out_file)
        for record in ArchiveIterator(in_file):
            warc_type = record.rec_headers.get_header("WARC-Type", "")

            if warc_type == "conversion":
                uri = record.rec_headers.get_header("WARC-Target-URI")
                lang = record.rec_headers.get_header(
                    "WARC-Identified-Content-Language", ""
                )

                if uri is not None and lang in ["eng", ""]:
                    extracted = tldextract.extract(uri)
                    registered_domain = f"{extracted.domain}.{extracted.suffix}"

                    if registered_domain is not None and registered_domain in domains:
                        try:
                            writer.write_record(record)
                            filtered_count += 1
                        except UnicodeEncodeError as e:
                            print(e)
                            continue

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Documentos filtrados: {filtered_count}")
    print(f"Tiempo filtrado: {elapsed_time:.6f} segundos")

    return filtered_count


def save_checkpoint(last_wet_path):
    with open(CHECKPOINT_FILE, "w") as in_file:
        in_file.write(last_wet_path)


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as in_file:
            return in_file.read().strip()
    return None


def download_file(url, output_file):

    print(f"Descargando archivo {url}")
    start_time = time.time()

    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(output_file, "wb") as wet_file:
                for chunk in response.iter_content(chunk_size=8192):
                    wet_file.write(chunk)

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


def download_wet_files(wet_paths, domains, max_paths=None):
    start_time = time.time()

    last_processed_file = load_checkpoint()
    found_checkpoint = last_processed_file is None

    with gzip.open(wet_paths, "rt") as in_file:
        for wet_path in islice(in_file, max_paths or None):
            wet_path = wet_path.strip()

            if last_processed_file and not found_checkpoint:
                if wet_path == last_processed_file:
                    found_checkpoint = True
                else:
                    continue

            output_file = f"docs/{wet_path.replace('/', '-')}"
            url = f"{COMMON_CRAWL_URL}/{wet_path}"

            success = download_file(url, output_file)

            if success:
                filter_wet_documents(
                    output_file,
                    f"docs/filtered-{os.path.basename(output_file)}",
                    domains,
                )

                save_checkpoint(wet_path)
                os.remove(output_file)

            else:
                print(f"Error al descargar el archivo {url}")
                break

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo total: {elapsed_time}")


if __name__ == "__main__":
    domains = set(read_top_domains(TOP_DOMAINS_URL))
    download_wet_files(WET_PATHS, domains)
