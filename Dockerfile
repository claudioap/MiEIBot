FROM python:3.6

# Install SQLite dependency
RUN apt -y update && apt install -y sqlite3

WORKDIR /usr/src/app

# Python dependencies
COPY pip-packages ./
RUN pip install --no-cache-dir -r pip-packages

# Move sources
COPY source/ .

# Set for export
VOLUME /var/MiEIBot/

CMD [ "python", "__main__.py" ]
