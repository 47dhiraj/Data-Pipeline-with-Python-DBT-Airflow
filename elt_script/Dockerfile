FROM python:3.8-slim

# Mentioning working directory inside of Container 
WORKDIR /app

# prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE 1

# ensure Python output is sent directly to the terminal without buffering
ENV PYTHONBUFFERED 1

# Install PostgreSQL command-line tools
RUN apt-get update && apt-get install -y postgresql-client

# Copy the ELT script from local to Docker working directory
COPY ./elt_script.py /app/elt_script.py

# Set the default command to run the ELT script
CMD ["python", "elt_script.py"]
