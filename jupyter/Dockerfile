# Jupyter lab for exploring the lakehouse (DuckDB directly over Iceberg).
#
# Lean image based on python:3.12-slim. Creates the "jovyan" user (the Jupyter
# ecosystem default) so the ./orchestration/notebooks volume mounted at
# /home/jovyan/work has the correct owner.
FROM python:3.12-slim

# Default non-root user of the Jupyter ecosystem.
ARG NB_USER=jovyan
ARG NB_UID=1000
RUN useradd --create-home --uid ${NB_UID} ${NB_USER}

WORKDIR /home/${NB_USER}/work

COPY jupyter/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER ${NB_USER}

EXPOSE 8888

# The token comes from the JUPYTER_TOKEN environment variable (set in compose).
CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=${JUPYTER_TOKEN}"]
