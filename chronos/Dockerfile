# Originally from: https://github.com/jpillora/docker-dnsmasq

FROM python:3

MAINTAINER eric@busboom.org

USER root
WORKDIR /root

RUN apt-get update && \
    apt-get install --no-install-recommends -y cron && \
	apt-get autoremove -y && \
	rm -rf /var/lib/apt/lists/*


RUN pip install -U git+https://github.com/CivicKnowledge/monkey-scraper.git #3
    
ADD monkeyscrape.cron /etc/cron.d/monkeyscrape.cron
RUN chmod 0644 /etc/cron.d/monkeyscrape.cron
RUN crontab /etc/cron.d/monkeyscrape.cron 

ADD monscrape.yaml /root
ADD uwsd-covid-19-data-48c99e07e2c8.json /root

#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
#RUN chmod +x /tini
#ENTRYPOINT ["/tini", "--"]

ADD chronos.sh /root/chronos.sh

CMD ["cron", "-f"]

