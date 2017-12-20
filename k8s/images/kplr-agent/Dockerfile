FROM debian:stable

RUN apt-get -q -y update && \
    apt-get install --no-install-recommends -y -q \
    procps \
    curl \
    less \
    dnsutils

RUN mkdir -p /opt/kplr

COPY ["build/kplr-agent", "config.json", "/opt/kplr/"]

ENTRYPOINT ["/opt/kplr/kplr-agent", "--config-file", "/opt/kplr/config.json"]
