# For building this image, you need to be in the project
# root and then pass -f tools/Dockerfile to docker build.
#
# $ docker build -f tools/Dockerfile -t isolate_server [--network=host]
#
# This is important since we want to be able to access src/ directory
# for copying the source code into the container (so the dockerfile
# will stay in a different directory than its context).
FROM python:3.9

RUN apt-get update && apt-get install -y git
RUN pip install --upgrade pip virtualenv wheel poetry-core

# Since system-level debian does not comply with
# the sysconfig, and we need to install a bunch
# of dependencies (like dbt-core), we are going to
# use a virtualenv.
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY . .
RUN pip install .

ENV ISOLATE_INHERIT_FROM_LOCAL=1

CMD ["python", "-m", "isolate.server.server"]
