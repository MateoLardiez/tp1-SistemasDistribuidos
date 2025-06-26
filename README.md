# Universidad de Buenos Aires
## Ingeniería en Informática
### TP Tolerancia a Fallos - Movies Analysis
#### Junio, 2025

### Grupo 16:

| Nombre | Email | Padrón |
|--------|-------|--------|
| Chávez Cabanillas, José Eduardo | jchavez@fi.uba.ar | 96467 |
| Davico, Mauricio | mdavico@fi.uba.ar | 106171 |
| Lardiez, Mateo | mlardiez@fi.uba.ar | 107992 |



# Enunciado

## Requerimientos funcionales

Se solicita un sistema distribuido que analice la información de películas y los ratings de sus espectadores en plataformas como iMDb.
Los ratings son un valor numérico de 1 al 5. Las películas tienen información como género, fecha de estreno, países involucrados en la producción, idioma, presupuesto e ingreso.
Se debe obtener:
1. Películas y sus géneros de los años 00' con producción Argentina y Española.
2. Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países.
3. Película de producción Argentina estrenada a partir del 2000, con mayor y con menor promedio de rating.
4. Top 10 de actores con mayor participación en películas de producción Argentina con fecha de estreno posterior al 2000
5. Average de la tasa ingreso/presupuesto de peliculas con overview de sentimiento positivo vs. sentimiento negativo, para películas de habla inglesa con producción americana, estrenadas a partir del año 2000

## Requerimientos no funcionales

- El sistema debe estar optimizado para entornos multicomputadoras
- Se debe soportar el incremento de los elementos de cómputo para escalar los volúmenes de información a procesar
- Se requiere del desarrollo de un Middleware para abstraer la comunicación basada en grupos.
- Se debe soportar una única ejecución del procesamiento y proveer graceful quit frente a señales SIGTERM.
- Soporte para varias ejecuciones de las consultas para un cliente, sin reinicio del servidor.
- Ejecución con varios clientes concurrentemente.
- Correcta limpieza de los recursos luego de cada ejecución.
- El sistema debe ser tolerante a fallos por caídas de procesos.

## Datos necesarios

Para construir la simulacion: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset

Valores como resultados patron: https://www.kaggle.com/code/gabrielrobles/fiuba-distribuidos-1-the-movies

# Ejecucion

## Levantar y terminar el sistema

Se debe enviar 5 parametros a la hora de ejecutar el generar-compose, los cuales son:
1) cantidad de clientes
2) cantidad de workers
3) cantidad de sinkers
4) cantidad de aggregators nlp
5) cantidad de healthcheckers

Luego levantar el sistema completo

```bash
# 1. Generar docker-compose
./generar-compose.sh {1} {2} {3} {4} {5}

Ejemplo:
./generar-compose.sh 2 2 2 2 3

# 2. Levantar sistema completo
make docker-compose-up

# Para ver los logs
make docker-compose-logs

# Para terminar el sistema
make docker-compose-down
```


## Sistema Killer

Iniciarlo en una nueva terminal ejecutando el comando correspondiente:
```bash
# Killer en modo interactivo
make killer-interactive
```

### Comandos más usados:

| Comando                   | Descripción                              |
|---------------------------|------------------------------------------|
| `make killer-interactive` | Modo interactivo (recomendado)           |
| `make killer-random`      | Modo aleatorio                        |
| `make killer-show-system` | Ver contenedores organizados por tipo    |
| `make killer-list`        | Listar todos los contenedores            |
| `make killer-kill`        | Matar un contenedor (pide nombre)        |

| `make killer-random`      | Mata de forma aleatoria los contenedores |

### Modo interactivo - Comandos internos:

Una vez en modo interactivo (`make killer-interactive`):

- `list` - Ver contenedores disponibles
- `kill <nombre_contenedor>` - Matar contenedor específico
- `exit` - Salir del modo interactivo
- `fatality` - Mata bruscamente (SIGKILL) a todos los contenedores del sistema

## Sistema Client Spawner

Iniciarlo en una nueva terminal, ejecutando el comando correspondiente:

```bash

# Usar client spawner en modo interactivo
make client-spawn-n 1
```

### Comandos más usados:

| Comando               | Descripción                                                              |
|-----------------------|--------------------------------------------------------------------------|
| `make client-spawn`   | Pide el ingreso de la cantidad de clientes a ser spawneados (recomendado)|
| `make client-spawn-n` | Pasar por linea de comando la cantidad de clientes                       |
| `make client-list`    | Listar todos los contenedores clientes spawneados                        |
| `make client-kill`    | Matar un contenedor (pide id del cliente)                                |


### Informe

El informe se puede visualizar en el archivo "Informe TP Tolerancia a Fallos - Movies Analysis.pdf"
