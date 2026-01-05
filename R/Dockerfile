FROM rocker/r-ver:4.3.3

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*


RUN R -e "install.packages(c('tidyverse','readr','dplyr','ggplot2','lubridate','janitor'), repos='https://cloud.r-project.org')"

CMD ["bash"]
