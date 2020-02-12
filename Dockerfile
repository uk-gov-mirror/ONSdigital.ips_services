FROM python:3.7.3-stretch


COPY . /ips_services
WORKDIR /ips_services

ENV DB_SERVER=${DB_SERVER}
ENV DB_USER_NAME=${DB_USER_NAME}
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_NAME=${DB_NAME}

RUN pip install -r requirements.txt && \
    apt-get update && \
    apt-get install -y --no-install-recommends apt-utils r-base && \
    R CMD INSTALL r-packages/ReGenesees_1.9.tar.gz && \
    R CMD INSTALL r-packages/DBI_1.0.0.tar.gz && \
    R CMD INSTALL r-packages/RMySQL_0.10.17.tar.gz

CMD [ "waitress-serve", "--listen=*:5000", "--threads=16", "ips.app:app" ]
