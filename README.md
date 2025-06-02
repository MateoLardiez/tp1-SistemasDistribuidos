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

## Datos necesarios

Para construir la simulacion: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset

Valores como resultados patron: https://www.kaggle.com/code/gabrielrobles/fiuba-distribuidos-1-the-movies

# Diseno

# Sistema Killer

## Inicio en 3 pasos:

```bash
# 1. Generar docker-compose con killer
./generar-compose.sh 2 3 2 1

# 2. Levantar sistema completo
make docker-compose-up

# 3. Usar killer en modo interactivo
make killer-interactive
```

## Comandos más usados:

| Comando | Descripción |
|---------|-------------|
| `make killer-interactive` | Modo interactivo (recomendado) |
| `make killer-show-system` | Ver contenedores organizados por tipo |
| `make killer-list` | Listar todos los contenedores |
| `make killer-kill` | Matar un contenedor (pide nombre) |

## Ejemplos prácticos:

### Simular falla en preprocessors:
```bash
./manage_containers.sh --kill movies_preprocessor_0
./manage_containers.sh --kill ratings_preprocessor_1
```

### Simular falla en filtros:
```bash
./manage_containers.sh --kill filter_by_year_0
./manage_containers.sh --kill filter_by_country_1
```

### Simular falla en sinkers:
```bash
./manage_containers.sh --kill query_1_sinker_0
./manage_containers.sh --kill query_2_sinker_1
```

### Ver impacto en el sistema:
```bash
# Antes de matar contenedores
make killer-show-system

# Matar algunos contenedores
./manage_containers.sh --kill movies_preprocessor_0

# Ver estado después
make killer-show-system
```

## Modo interactivo - Comandos internos:

Una vez en modo interactivo (`make killer-interactive`):

- `list` - Ver contenedores disponibles
- `<nombre_contenedor>` - Matar contenedor específico
- `exit` - Salir del modo interactivo

## Reiniciar servicios eliminados:

```bash
# Reiniciar un servicio específico
docker-compose up -d movies_preprocessor_0

# Reiniciar todos los servicios
make docker-compose-up
```

## Troubleshooting:

```bash
# Verificar que killer esté ejecutándose
docker ps | grep killer

# Si killer no está disponible
make docker-compose-down
make docker-compose-up

# Verificar conectividad con Docker
docker exec killer python -c "import docker; print(docker.APIClient().ping())"
```

## Casos de uso para testing:

1. **Test de tolerancia a fallos**: Mata workers de diferentes tipos
2. **Test de balanceo de carga**: Mata algunos workers de un tipo específico
3. **Test de recuperación**: Mata servicios críticos y observa comportamiento
4. **Test de cascada**: Mata múltiples servicios en secuencia

...