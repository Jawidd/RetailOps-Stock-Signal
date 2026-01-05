FROM rocker/tidyverse:4.3.3

RUN apt-get update && apt-get install -y --no-install-recommends \
  libpq-dev pandoc \
  && rm -rf /var/lib/apt/lists/*

RUN R -e "install.packages(c( \
    'data.table','skimr','DataExplorer', \
    'logger','janitor','glue','assertr','validate','scales','patchwork' \
), repos='https://cloud.r-project.org')"

WORKDIR /app
COPY . /app
CMD ["bash"]
