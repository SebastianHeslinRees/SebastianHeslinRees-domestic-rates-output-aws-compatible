# Use the rocker/r-ver base image for R
FROM rocker/r-ver:4.1.0

# Set working directory in the container
WORKDIR /usr/src/app

# Install usrmerge to handle the /bin directory issue
RUN apt-get update && apt-get install -y usrmerge

# Install system dependencies required for devtools and other R packages
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libgit2-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    git \
    make \
    cmake \
    g++ \
    && rm -rf /var/lib/apt/lists/*


# Install remotes (lighter alternative to devtools)
RUN R -e "install.packages('remotes', repos='https://cloud.r-project.org/')"

# Use remotes to install packages instead of devtools
RUN R -e "remotes::install_local('popmodules')"

# Install required R packages from CRAN
RUN R -e "install.packages(c('dplyr', 'assertthat', 'aws.s3', 'data.table', 'httr', 'jsonlite', 'dtplyr', 'pryr', 'profvis', 'magrittr', 'rlang', 'tidyr', 'plyr', 'tibble', 'rprojroot', 'purrr', 'minpack.lm', 'testthat', 'stringr', 'loggr', 'tidyselect'), repos='https://cloud.r-project.org/')"


# Copy and run the AWS configuration script
COPY config/aws_config.sh /usr/src/app/config/aws_config.sh
COPY domestic_rates_outputs.R /usr/src/app/domestic_rates_outputs.R
COPY popmodules /usr/src/app/popmodules

# Verify that the popmodules directory has been copied
RUN ls -l /usr/src/app/popmodules

RUN chmod +x /usr/src/app/config/aws_config.sh
RUN /usr/src/app/config/aws_config.sh

# Set the command to run the main R script
CMD ["Rscript", "domestic_rates_outputs.R"]
