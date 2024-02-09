FROM python:3.8-slim

# Install PostgreSQL command-line tools
RUN apt-get update && apt-get install -y postgresql-client

# Copy the ELT script from local to Docker working directory
COPY elt_script.py .

# Set the default command to run the ELT script
CMD ["python", "elt_script.py"]



# FROM python:3.8-slim

# # Install PostgreSQL command-line tools
# RUN apt-get update && apt-get install -y postgresql-client

# # Copy the wait-for-it.sh script into the container
# COPY ./wait-for-it.sh /usr/local/bin/

# # Set execute permissions for wait-for-it.sh
# RUN chmod +x /usr/local/bin/wait-for-it.sh

# # Copy the ELT script from local to Docker working directory
# COPY elt_script.py .

# # Set the default command to run the ELT script
# CMD ["/usr/local/bin/wait-for-it.sh", "source_postgres:5432", "--", "python", "elt_script.py"]