FROM sachingpta/dkr-python3.4.3 
ADD switch /opt/switch

COPY convert  /opt/

RUN mkdir  -p /root/.pg_chameleon/config

RUN chmod 777 /opt/convert

RUN pip3 install pg_chameleon

RUN  cd /opt/switch/scripts

COPY default.yaml /root/.pg_chameleon/config/default.yaml

#CMD ["/usr/local/bin/chameleon.py", "start_replica"]
WORKDIR /opt/

CMD ["./convert"]
