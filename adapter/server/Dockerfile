# Get the basic image in shape
FROM python:3.9
RUN apt-get update && apt-get install -y git
RUN pip install --upgrade pip virtualenv wheel

# Since system-level debian does not comply with
# the sysconfig, and we need to install a bunch
# of dependencies (like dbt-core), we are going to
# use a virtualenv.
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install the basic dependencies for the demo.
RUN pip install "git+https://github.com/fal-ai/dbt-core.git@act1#subdirectory=core" \
                "dbt-fal[snowflake] @ git+https://github.com/fal-ai/fal.git@act1-isolate-server#subdirectory=adapter" \
                dbt-duckdb==1.2.0 \
                dbt-snowflake==1.3.0b2 \
                dbt-bigquery==1.3.0b2 \
                "git+https://github.com/fal-ai/dbt-core.git@act1#subdirectory=plugins/postgres" \
                dill==0.3.5.1 \
                isolate[server]==0.1.0

# Turn on inheritance for isolate server
ENV ISOLATE_INHERIT_FROM_LOCAL=1

CMD ["flask", "--app", "isolate.server", "--debug", "run", "--host", "0.0.0.0", "--port", "5000"]
