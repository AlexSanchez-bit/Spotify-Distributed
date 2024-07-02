# Usar una imagen base de Python
FROM python:3.11

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

COPY . .
# Define el entrypoint
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

# Ejecutar el script principal
CMD ["python", "__main__.py"]
