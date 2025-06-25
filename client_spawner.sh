#!/bin/bash

# Script para crear y gestionar instancias de clientes del sistema distribuido

show_help() {
    echo "=== Sistema de Gestión de Clientes ==="
    echo ""
    echo "Uso: $0 [OPCIÓN] [PARÁMETROS]"
    echo ""
    echo "Opciones:"
    echo "  -c, --spawn N        Inicia N instancias de clientes"
    echo "  -v, --view N         Muestra la consola del cliente número N"
    echo "  -l, --list           Lista todos los clientes en ejecución"
    echo "  -k, --kill N         Detiene el cliente número N"
    echo "  -K, --kill-all       Detiene todos los clientes en ejecución"
    echo "  -h, --help           Muestra esta ayuda"
    echo ""
    echo "Ejemplos:"
    echo "  $0 --spawn 5         # Inicia 5 clientes"
    echo "  $0 --view 3          # Muestra la consola del cliente 3"
    echo "  $0 --list            # Lista todos los clientes en ejecución"
    echo "  $0 --kill 2          # Detiene el cliente número 2"
    echo "  $0 --kill-all        # Detiene todos los clientes en ejecución"
    echo ""
}

spawn_clients() {
    local num_clients="$1"
    if [ -z "$num_clients" ]; then
        echo "Error: Debes especificar el número de clientes a iniciar."
        echo "Uso: $0 --spawn <número_de_clientes>"
        exit 1
    fi
    
    if ! [[ "$num_clients" =~ ^[0-9]+$ ]]; then
        echo "Error: El número de clientes debe ser un valor numérico."
        exit 1
    fi
    
    echo "=== Iniciando $num_clients clientes ==="
    
    for ((i=0; i<num_clients; i++)); do
        client_name="spawned_client_$i"
        echo "Iniciando cliente: $client_name"
        
        # Comprobar si el cliente ya existe
        if docker ps -a --format "{{.Names}}" | grep -q "^${client_name}$"; then
            echo "El cliente $client_name ya existe. Eliminándolo..."
            docker rm -f "$client_name" > /dev/null 2>&1
        fi
        
        # Crear el cliente con la misma configuración que en generar-compose.sh
        docker run -d --name "$client_name" \
            --network tp1_testing_net \
            -e CLIENT_ID="$i" \
            -v "$(pwd)/client/config.yaml:/config.yaml:ro" \
            -v "$(pwd)/.data/movies_metadata.csv:/movies.csv:ro" \
            -v "$(pwd)/.data/ratings.csv:/ratings.csv:ro" \
            -v "$(pwd)/.data/credits.csv:/credits.csv:ro" \
            client:latest -c "/client"
        
        if [ $? -eq 0 ]; then
            echo "✓ Cliente $client_name iniciado correctamente"
        else
            echo "✗ Error al iniciar el cliente $client_name"
        fi
    done
    
    echo ""
    echo "Clientes iniciados:"
    docker ps --format "table {{.Names}}\t{{.Status}}" | grep "^spawned_client_[0-9]"
    echo ""
    echo "Puedes ver la consola de un cliente específico con:"
    echo "  $0 --view <número_de_cliente>"
}

view_client_console() {
    local client_num="$1"
    if [ -z "$client_num" ]; then
        echo "Error: Debes especificar el número del cliente."
        echo "Uso: $0 --view <número_de_cliente>"
        exit 1
    fi
    
    if ! [[ "$client_num" =~ ^[0-9]+$ ]]; then
        echo "Error: El número de cliente debe ser un valor numérico."
        exit 1
    fi
    
    local client_name="spawned_client_$client_num"
    
    # Comprobar si el cliente existe
    if ! docker ps --format "{{.Names}}" | grep -q "^${client_name}$"; then
        echo "Error: El cliente $client_name no está en ejecución."
        echo "Usa '$0 --spawn' para iniciar nuevos clientes o '$0 --list' para ver los clientes en ejecución."
        exit 1
    fi
    
    echo "=== Conectando a la consola del $client_name ==="
    echo "Para salir: presiona Ctrl+C"
    echo ""
    
    docker logs -f "$client_name"
}

list_clients() {
    echo "=== Clientes en Ejecución ==="
    if ! docker ps --format "table {{.Names}}" | grep -q "^spawned_client_[0-9]"; then
        echo "No hay clientes en ejecución."
        echo "Usa '$0 --spawn N' para iniciar N clientes."
    else
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}" | grep "^spawned_client_[0-9]"
        echo ""
        echo "Puedes ver la consola de un cliente específico con:"
        echo "  $0 --view <número_de_cliente>"
    fi
}

kill_client() {
    local client_num="$1"
    if [ -z "$client_num" ]; then
        echo "Error: Debes especificar el número del cliente a detener."
        echo "Uso: $0 --kill <número_de_cliente>"
        exit 1
    fi
    
    if ! [[ "$client_num" =~ ^[0-9]+$ ]]; then
        echo "Error: El número de cliente debe ser un valor numérico."
        exit 1
    fi
    
    local client_name="spawned_client_$client_num"
    
    # Comprobar si el cliente existe
    if ! docker ps --format "{{.Names}}" | grep -q "^${client_name}$"; then
        echo "Error: El cliente $client_name no está en ejecución."
        exit 1
    fi
    
    echo "Deteniendo y eliminando cliente: $client_name"
    docker stop "$client_name" > /dev/null
    docker rm "$client_name" > /dev/null
    
    echo "✓ Cliente $client_name eliminado correctamente"
}

kill_all_clients() {
    echo "Deteniendo todos los clientes en ejecución..."
    
    local clients=$(docker ps --format "{{.Names}}" | grep "^spawned_client_[0-9]")
    if [ -z "$clients" ]; then
        echo "No hay clientes en ejecución."
        return
    fi
    
    for client in $clients; do
        echo "Deteniendo y eliminando $client..."
        docker stop "$client" > /dev/null
        docker rm "$client" > /dev/null
    done
    
    echo "✓ Todos los clientes han sido detenidos y eliminados."
}

# Procesar argumentos
case "$1" in
    -c|--spawn)
        spawn_clients "$2"
        ;;
    -v|--view)
        view_client_console "$2"
        ;;
    -l|--list)
        list_clients
        ;;
    -k|--kill)
        kill_client "$2"
        ;;
    -K|--kill-all)
        kill_all_clients
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
