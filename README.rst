
============================
AMQP Tool for Twisted Python
============================

Simple tools for litening to AMQP queues and sending messages to AMQP exchanges.

Example start listening on a queue::

    $ python txqconsumer.py test_q -e 'tls:host=thor.lafayette.edu:port=5671:certificate=/home/waldbiec/dev-certs/rabbit.cert.pem:privateKey=/home/waldbiec/dev-certs/rabbit.key.pem:trustRoots=./tls/cacerts' -u guest

Example to send a message to an exchange::

    $ python txqproducer.py test_exchange 'test.green' ./my-message.txt -e "tls:host=$EXCHANGE_HOST:port=$EXCHANGE_PORT:certificate=$CLIENT_CERT:privateKey=$CLIENT_KEY:trustRoots=$CACERTS_DIR" -u guest

