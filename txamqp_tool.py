#! /usr/bin/env python

from __future__ import print_function
import argparse
import sys
#from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.internet.task import react
from twisted.python import filepath
from txamqp.client import TwistedDelegate           
from txamqp.protocol import AMQClient               
from txamqp.queue import Closed as QueueClosedError 
import txamqp.spec                                  

def main(reactor):
    vhost = '/'
    spec_path = '/home/waldbiec/git-repos/txamqp_tool/spec/amqp0-9-1.stripped.xml'
    spec = txamqp.spec.load(spec_path)
    params = {
        'creds': ('guest', 'guest'),
        'queue_name': 'test_q',
        'consumer_tag': 'mytag',
    }
    endpoint_s = "tls:host=thor.lafayette.edu:port=5671:trustRoots=/home/waldbiec/git-repos/txamqp_tool/tls/cacerts/:certificate=/home/waldbiec/dev-certs/rabbit.cert.pem:privateKey=/home/waldbiec/dev-certs/rabbit.key.pem"
    #endpoint_s = "ssl:host=thor.lafayette.edu:port=5671:caCertsDir=/home/waldbiec/git-repos/txamqp_tool/tls/cacerts"
    e = clientFromString(reactor, endpoint_s)
    delegate = TwistedDelegate()
    amqp_protocol = AMQClient(            
        delegate=delegate,
        vhost=vhost,      
        spec=spec)       
    d = connectProtocol(e, amqp_protocol)
    d.addCallback(on_amqp_connect, params)
    return d

@inlineCallbacks
def on_amqp_connect(conn, params):
    channel = None
    try:
        user, passwd = params['creds']
        yield conn.authenticate(user, passwd)
        print("[DEBUG] Authenticated.", file=sys.stderr)
        channel = yield conn.channel(1)                                                    
        yield channel.channel_open()                                                       
        print("[DEBUG] Channel opened.", file=sys.stderr)
        queue_name = params["queue_name"]
        yield channel.queue_declare(queue=queue_name, durable=True)                          
        print("[DEBUG] Queue declared.", file=sys.stderr)
        consumer_tag = params['consumer_tag']                              
        yield channel.basic_consume(queue=queue_name, consumer_tag=consumer_tag)
        print("[DEBUG] Channel set for basic_consume.", file=sys.stderr)
        queue = yield conn.queue(consumer_tag)
        print("[DEBUG] Queue obtained.", file=sys.stderr)
        while True:
            try:
                yield process_amqp_message(queue, channel, consumer_tag)
            except QueueClosedError as ex:
                print("[INFO] Queue was closed.")
                break
            print("[DEBUG] Message processed.", file=sys.stderr)
    except Exception as ex:
        print("ERROR: {0}".format(ex), file=sys.stderr)
        try:                                                                           
            if channel is not None:
                yield channel.channel_close()                                              
        except Exception as ex:                                                        
            print("Error while trying to close AMQP channel: {error}".format(error=ex), file=sys.stderr)   
        try:                                                                           
            if hasattr(conn, 'connection_close'):
                yield conn.connection_close()                                              
        except Exception as ex:                                                        
            print("Error while trying to close AMQP connection: {error}".format(error=ex), file=sys.stderr)

@inlineCallbacks                                                                                  
def process_amqp_message(queue, channel, consumer_tag): 
    try:                                                                                          
        msg = yield queue.get()                                                                   
    except QueueClosedError:                                                                      
        raise
    else:
        print("Message: {0}".format(msg))
        yield channel.basic_ack(delivery_tag=msg.delivery_tag)                                
                                                                                       
if __name__ == "__main__":
    try:
        react(main)
    except filepath.UnlistableError as ex:
        print(ex.originalException)
