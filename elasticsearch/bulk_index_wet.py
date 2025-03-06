import gzip
import json
import os
import warnings

import tldextract
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning
from warcio.archiveiterator import ArchiveIterator

from elasticsearch import Elasticsearch, helpers

load_dotenv()

# Configuracion de Elasticsearch
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=(
        "elastic",
        os.getenv("ELASTIC-PASSWORD"),
    ),
    verify_certs=False,
)
INDEX_NAME = "wikidata-references"
CHECKPOINT_FILE = "checkpoint.txt"
BATCH_SIZE = 10
BASE_PATH = os.getenv("BASE_PATH")
WET_FILES_DIR = f"{BASE_PATH}wikidata-references/documents/wet/docs/"
WET_DOCS_PATHS = f"{BASE_PATH}wikidata-references/documents/wet/filtered_wet_docs_paths.txt"
RANKING_FILE = f"{BASE_PATH}wikidata-references/domains/top_domains.json"


def load_ranking_data(ranking_file_path):
    """Carga el archivo de ranking en memoria."""
    if not os.path.exists(ranking_file_path):
        raise FileNotFoundError(f"El archivo de ranking no existe: {ranking_file_path}")
    with open(ranking_file_path, "r") as f:
        return json.load(f)


def load_checkpoint():
    """Carga el último archivo procesado desde el checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return None


def save_checkpoint(last_processed_file):
    """Guarda el último archivo procesado correctamente."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(last_processed_file + "\n")


def extract_wikipedia_references_count(record, ranking_data):
    """Extrae la posición de Wikipedia según el dominio."""
    warc_target_uri = record.rec_headers.get_header("WARC-Target-URI")
    extracted = tldextract.extract(warc_target_uri)
    registered_domain = f"{extracted.domain}.{extracted.suffix}"
    return ranking_data.get(registered_domain, None)


def process_wet_file(wet_file_path, ranking_data):
    """Procesa un archivo WET y extrae documentos para indexar."""
    documents = []
    try:
        with gzip.open(wet_file_path, "rb") as wet_file:
            for record in ArchiveIterator(wet_file):
                if record.rec_type == "conversion":
                    warc_record_id = record.rec_headers.get_header("WARC-Record-ID")
                    warc_date = record.rec_headers.get_header("WARC-Date")
                    warc_target_uri = record.rec_headers.get_header("WARC-Target-URI")
                    warc_refers_to = record.rec_headers.get_header("WARC-Refers-To")
                    content = (
                        record.content_stream().read().decode("utf-8", errors="ignore")
                    )
                    wikipedia_references_count = extract_wikipedia_references_count(
                        record, ranking_data
                    )

                    document = {
                        "_op_type": "index",
                        "_index": INDEX_NAME,
                        "_id": warc_record_id,
                        "_source": {
                            "warc-record-id": warc_record_id,
                            "warc-date": warc_date,
                            "warc-target-uri": warc_target_uri,
                            "warc-refers-to": warc_refers_to,
                            "wikipedia-references-count": wikipedia_references_count,
                            "content": content,
                        },
                    }
                    documents.append(document)
        return documents
    except Exception as e:
        print(f"Error procesando {wet_file_path}: {e}")
        return []


def main():
    """Procesa archivos WET en lotes de 5 y los indexa en Elasticsearch."""
    ranking_data = load_ranking_data(RANKING_FILE)
    last_processed = load_checkpoint()
    indexing_count = 0

    with open(WET_DOCS_PATHS, "r") as f:
        wet_files = [line.strip() for line in f.readlines()]

    # Si hay un checkpoint, continuar desde ahí
    if last_processed and last_processed in wet_files:
        last_index = wet_files.index(last_processed) + 1
    else:
        last_index = 0

    while last_index < len(wet_files):
        batch_files = wet_files[last_index : last_index + BATCH_SIZE]
        all_documents = []

        for wet_file in batch_files:
            full_wet_path = os.path.join(WET_FILES_DIR, wet_file)
            docs = process_wet_file(full_wet_path, ranking_data)
            all_documents.extend(docs)

        if all_documents:
            response = helpers.bulk(es, all_documents)
            indexing_count += response[0]  # Documentos indexados con éxito
            print(f"Indexados {response[0]} documentos de {len(all_documents)} en lote")
            save_checkpoint(batch_files[-1])

        last_index += BATCH_SIZE

    print(f"Indexación completada. Total documentos indexados: {indexing_count}")


if __name__ == "__main__":
    main()
