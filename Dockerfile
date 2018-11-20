FROM python:3.6

# Install SQLite dependency
RUN apt -y update && apt install -y sqlite3 toilet

WORKDIR /usr/src/app

# Python dependencies
COPY pip-packages ./
RUN pip install --no-cache-dir -r pip-packages

# Move sources
COPY source/ .

# Set database location for export
VOLUME /var/MiEIBot/

CMD [ "python3", "__main__.py" ]
