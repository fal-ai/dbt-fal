FROM python:3.8-slim-buster
COPY scripts/ /tmp/scripts/
WORKDIR /tmp/scripts/
RUN chmod +x run_scripts.sh
RUN pip3 install temporal-python-sdk
ENTRYPOINT [ "./run_scripts.sh" ]
