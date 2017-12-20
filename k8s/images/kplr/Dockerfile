FROM debian:stable

RUN apt-get -q -y update && \
    apt-get install --no-install-recommends -y -q \
    procps \
    curl \
    less \
    dnsutils

RUN mkdir -p /opt/kplr

COPY ["build/kplr", "config.json", "log4g.properties", "/opt/kplr/"]

ENTRYPOINT ["/opt/kplr/kplr", "--config-file", "/opt/kplr/config.json", "--log-config", "/opt/kplr/log4g.properties"]