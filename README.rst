
============================
AMQP Tool for Twisted Python
============================

Example start listening on a queue::

    $ python txamqp_tool.py test_q -e 'tls:host=thor.lafayette.edu:port=5671:certificate=/home/waldbiec/dev-certs/rabbit.cert.pem:privateKey=/home/waldbiec/dev-certs/rabbit.key.pem:trustRoots=./tls/cacerts' -u guest


