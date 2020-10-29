# streams
From multiple data sources to one dashboard.

### Requirements

- This project uses [Poetry](https://python-poetry.org/).
    - You'll need to install poetry and type `poetry update' in this directory to replicate this environment.

- Install Redis, default config should do. 
    - This project uses a local redis server as a cache for incoming data.
    - If you're using windows, redis in WSL is *strongly advised*.
    - Make sure the service is running.

### Details

#### EC2

- The EC2 instance hosts a PostgreSQL DB, with a TimescaleDB add-on for better time-series performance.
- Connection to the PostgreSQL DB is managed by PGBouncer, which takes care of the numerous threaded connections
  that are made to the database at all times.

#### multi-thread-streams.py

- This is where all the logic resides. 
- 