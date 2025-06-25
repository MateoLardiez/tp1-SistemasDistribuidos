#!/bin/bash

# Script para manejar contenedores del sistema distribuido usando killer

KILLER_CONTAINER="killer"

show_help() {
    echo "=== Sistema de Control de Contenedores ==="
    echo ""
    echo "Uso: $0 [OPCIÓN] [CONTENEDOR]"
    echo ""
    echo "Opciones:"
    echo "  -i, --interactive    Modo interactivo para matar contenedores"
    echo "  -k, --kill NAME      Mata un contenedor específico"
    echo "  -l, --list          Lista todos los contenedores ejecutándose"
    echo "  -s, --show-system   Muestra contenedores del sistema distribuido"
    echo "  -h, --help          Muestra esta ayuda"
    echo ""
    echo "Ejemplos:"
    echo "  $0 --interactive                    # Modo interactivo"
    echo "  $0 --kill movies_preprocessor_0     # Mata un preprocessor específico"
    echo "  $0 --list                          # Lista todos los contenedores"
    echo "  $0 --show-system                   # Muestra solo contenedores del sistema"
    echo ""
}

check_killer_container() {
    if ! docker ps --format "{{.Names}}" | grep -q "^${KILLER_CONTAINER}$"; then
        echo "Error: El contenedor 'killer' no está ejecutándose."
        echo "Asegúrate de que el docker-compose esté levantado con el killer incluido."
        exit 1
    fi
}

run_interactive_mode() {
    echo "=== Modo Interactivo del Killer ==="
    echo "Conectando al contenedor killer..."
    docker exec -it "$KILLER_CONTAINER" python main.py --interactive
}

kill_specific_container() {
    local container_name="$1"
    if [ -z "$container_name" ]; then
        echo "Error: Debes especificar el nombre del contenedor."
        echo "Uso: $0 --kill <nombre_contenedor>"
        exit 1
    fi
    
    echo "Matando contenedor: $container_name"
    docker exec "$KILLER_CONTAINER" python main.py --kill "$container_name"
}

list_all_containers() {
    echo "=== Todos los Contenedores Ejecutándose ==="
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
}

show_system_containers() {
    echo "=== Contenedores del Sistema Distribuido ==="
    echo ""
    echo "Preprocessors:"
    docker ps --format "{{.Names}}" | grep -E "(movies_preprocessor|ratings_preprocessor|credits_preprocessor)" | sort
    echo ""
    echo "Filters:"
    docker ps --format "{{.Names}}" | grep -E "filter_by_" | sort
    echo ""
    echo "Groupers:"
    docker ps --format "{{.Names}}" | grep -E "group_by_" | sort
    echo ""
    echo "Joiners:"
    docker ps --format "{{.Names}}" | grep -E "joiner_" | sort
    echo ""
    echo "Aggregators:"
    docker ps --format "{{.Names}}" | grep -E "aggregator_" | sort
    echo ""
    echo "Query Sinkers:"
    docker ps --format "{{.Names}}" | grep -E "query_.*_sinker" | sort
    echo ""
    echo "Clients:"
    docker ps --format "{{.Names}}" | grep -E "^client[0-9]+$" | sort
    echo ""
    echo "Infrastructure:"
    docker ps --format "{{.Names}}" | grep -E "(rabbitmq|gateway|results_tester|killer)" | sort
}

kill_random() {
    echo "=== Modo Interactivo del Killer ==="
    echo "Conectando al contenedor killer..."
    docker exec -it "$KILLER_CONTAINER" python main.py
}

# Procesar argumentos
case "$1" in
    -r|--random)
        check_killer_container
        kill_random
        ;;

    -i|--interactive)
        check_killer_container
        run_interactive_mode
        ;;
    -k|--kill)
        check_killer_container
        kill_specific_container "$2"
        ;;
    -l|--list)
        list_all_containers
        ;;
    -s|--show-system)
        show_system_containers
        ;;
    -h|--help|"")
        show_help
        ;;
    *)
        echo "Error: Opción desconocida '$1'"
        echo "Usa '$0 --help' para ver las opciones disponibles."
        exit 1
        ;;
esac
