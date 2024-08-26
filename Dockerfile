# Usar una imagen base de Python
FROM python:3.11
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

COPY . .
# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

# Ejecutar el script principal
CMD ["python", "server.py",">>","logs.txt"]
