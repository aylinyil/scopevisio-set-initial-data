# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

# Set environment variables
ENV DB_HOST=host.docker.internal \
    DB_PASSWORD=postgres \
    DB_NAME=postgres \
    DB_USER=postgres \
    DB_PORT=5632

# The command to run when the container launches
CMD ["python", "main.py"]
