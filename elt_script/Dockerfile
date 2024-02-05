FROM python:3.8-slim

# Install PostgreSQL command-line tools
RUN apt-get update && apt-get install -y postgresql-client

# Copy the ELT script from local to Docker working directory
COPY elt_script.py .

# Set the default command to run the ELT script
CMD ["python", "elt_script.py"]