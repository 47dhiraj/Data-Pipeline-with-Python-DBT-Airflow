import subprocess
import time


# Configuration for the source PostgreSQL database
source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'        
}

# Configuration for the destination PostgreSQL database
destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'  
}


# Command to check if the data already in destination_db or not
check_destination_db_tables = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'], 
    '-c', "SELECT EXISTS (SELECT 1 FROM public.users LIMIT 1);"     
]

# Set the PGPASSWORD environment variable for the source & destination database 
source_pwd = dict(PGPASSWORD=source_config['password'])
destination_pwd = dict(PGPASSWORD=destination_config['password'])

# Execute the command to check if table/data in source_db & destination_db
destination_db_result = subprocess.run(check_destination_db_tables, env=destination_pwd, capture_output=True, text=True)


if destination_db_result.returncode != 0:

    print("Destination Database is Empty!! Initiate executing ELT script ...")

    # Use postgres ibuilt 'pg_dump' command to dump source database data to a .sql file
    dump_command = [
        'pg_dump',
        '-h', source_config['host'],
        '-U', source_config['user'],
        '-d', source_config['dbname'],
        '-f', 'data_dump.sql',
        '-w'                    # -w flag, do not prompt for password
    ]

    # Executing dump_command using subprocess.run()
    subprocess.run(dump_command, env=source_pwd, check=True)


    # Use psql to load the dumped SQL file into the destination database
    load_command = [
        'psql',
        '-h', destination_config['host'],
        '-U', destination_config['user'],
        '-d', destination_config['dbname'],
        '-a', '-f', 'data_dump.sql'
    ]


    # Execute the load command
    subprocess.run(load_command, env=destination_pwd, check=True)

    print("Successfully finished loading data into destination database ...")


elif destination_db_result.returncode == 0:
    print("There may already have data inside the destination_db table OR Database connectivity issue.")
