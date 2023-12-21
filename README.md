# natureVillage

### Instalación

pasos para instalar el proyecto.

1. Crear un archivo para guardar las variables de entorno (`.env`).
```sh
DB_USER=${DB_USER}
DB_PWD=${DB_PWD}
DB_HOST=${DB_HOST}
DB_DATABASE_NAME=${DB_NAME}
MQTT_URL=${MQTT_URL}
```
2. Instalar las dependencias y ejecutar el programa.
```sh
pip install -r requirements.txt
python main.py
```
3. Correr los scripts en la carpeta llamada `migrations/`.

