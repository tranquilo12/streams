# streams
From multiple data sources to one dashboard.

### Requirements

- This project uses [Poetry](https://python-poetry.org/).
    - You'll need to install poetry and type `poetry update' in this directory to replicate this environment.

- Install Redis, default config should do. 
    - This project uses a local redis server as a cache for incoming data.
    - If you're using windows, redis in WSL is *strongly advised*.
    - Make sure the service is running.

### Documentation

#### Backend Infrastructure
- The time-series data for the dashboard is hosted on an EC2 instance, in a PostgresDB (with a TimeScaleDB add-on).
- The EC2 instance demands an SSL sign in.
- The quickest way (latency wise) to overcome this issue is to open a local port (local to where the dashboard server is running), 
that forwards the instructions through the SSL, to a port in the EC2 instance that accepts these instructions.  AKA Port-Forwarding.
- So we always need a tunnel that connects a local port to the EC2 instance (preferably in the background), when the dashboard is running.

#### SSH Port Forwarding issues (wrt Heroku)
- As a pre-deployment procedure, Heroku scans the project for a .profile.d/ssh-setup.sh file, 
  which it then executes.
- All SSH tunnel port-forwarding is controlled through `ssh-setup.sh`.
- **Potential Issue**: The `SSH_CMD` variable's `-L` flag must be all-integer. 
    - e.g 5433:localhost:6432 will not work, it has to be 5433:127.0.0.1:6432
- **Potential Issue**: gunicorn is not set up. 
    - Connection to the PostgreSQL DB is managed by PGBouncer, which takes care of the numerous threaded connections
      that are made to the database at all times.
    - Let gunicorn manage all the connections that are going through the SSL, it's much easier that way.
    - Make sure `Procfile` contains `web: gunicorn app:server`.
    - Make sure the `server` variable is set in the app.py as well.
- **Potential Issue**: Do not instantiate a tunnel every time a request is being made, that will result in errors that 
will ask if the port is already in use.

#### multi-thread-streams.py
- This is where all the logic for the below reside: 
    - Pull data from polygon-io
    - Use redis.
    - Make postgres ingest data.
    - Query postgres.

### Features (TODO)
Arranged in descending order of importance, within each group.

[] Create a new card that filters for industry.

[] Transformations
    [] day-on-day returns 
    [] day-to-first returns
    [] View holiday markers
    [] View earnings markers
    [] Add ta-lib 
        [] Will have to build from source [TA-LIB] (https://github.com/mrjbq7/ta-lib), which may involve rebuilding 
           the entire app into a docker instance to have more control over the stack.

[] Implement tabs [using this](https://dash-bootstrap-components.opensource.faculty.ai/examples/graphs-in-tabs/)
    [] Tab 1 contains just an overview of what's been selected. 
    [] Tab 2 can only be related to returns. 
        [] Correlation between returns
            [] Between 2 and N number of assets.
        [] PACF, ACF.
        [] Decomposition.
        [] Wavelet analysis.
        
[] Create login screen for each user.
    [] Each user must be able to save/view their favourites.