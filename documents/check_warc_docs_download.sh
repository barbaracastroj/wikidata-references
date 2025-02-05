#!/bin/bash

SCRIPT_NAME="download_warc_docs.py"
SCRIPT_PATH="path/to/wikidata-references/documents/$SCRIPT_NAME"
VENV_PATH="path/to/wikidata-references/wikidata-env"
SCREEN_NAME="warc_download"
LOG_FILE="path/to/wikidata-references/documents/log.txt"

log() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $1" >> "$LOG_FILE"
}

if pgrep -f "$SCRIPT_NAME" > /dev/null; then
    log "La descarga de documentos warc está en ejecución."
else
    log "La descarga de documentos warc está detenida."

    if screen -list | grep -q "$SCREEN_NAME"; then
        log "La sesión de screen '$SCREEN_NAME' ya existe. Reiniciando la descarga..."
        screen -S "$SCREEN_NAME" -X stuff "source $VENV_PATH/bin/activate && python3 $SCRIPT_PATH\n"
    else
        log "La sesión de screen '$SCREEN_NAME' no existe. Creándola y reiniciando la descarga..."
        screen -dmS "$SCREEN_NAME" bash -c "source $VENV_PATH/bin/activate && python3 $SCRIPT_PATH"
    fi
fi