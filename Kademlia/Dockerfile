# Usar una imagen base de Python
FROM python:3.11

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar el archivo de requisitos (si existe)
COPY requirements.txt .

# Instalar las dependencias necesarias (si existen)
#RUN pip install socket typing

# Copiar el resto del código de la aplicación
COPY . .

# Ejecutar el script principal
CMD ["python", "__main__.py"]
