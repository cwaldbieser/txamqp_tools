#! /usr/bin/env python

from __future__ import print_function
import argparse
import os.path
import sys
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.internet.task import react
from twisted.python import filepath
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient               
from txamqp.queue import Closed as QueueClosedError 
import txamqp.spec                                  

def main(reactor, args):
    vhost = args.vhost
    user = args.user
    passwd_file = args.passwd_file
    if passwd_file is None:
        passwd = 'guest'
    else:
        passwd = passwd_file.read().rstrip("\n\r")
        passwd_file.close()
    spec_path = os.path.join(
        os.path.dirname(__file__),
        'spec/amqp0-9-1.stripped.xml')
    spec = txamqp.spec.load(spec_path)
    params = {
        'creds': (user, passwd),
        'queue_name': args.queue_name,
        'consumer_tag': args.consumer_tag,
    }
    endpoint_s = args.endpoint 
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

@inlineCallbacks                                                                                  
def process_amqp_message(queue, channel, consumer_tag): 
    try:                                                                                          
        msg = yield queue.get()                                                                   
    except QueueClosedError:                                                                      
        raise
    else:
        consumer_tag, delivery_tag, redelivered, exchange_name, route_key = msg.fields
        print("Consumer tag: {0}".format(consumer_tag))
        print("Delivery tag: {0}".format(msg.delivery_tag))
        print("Redelivered: {0}".format(redelivered))
        print("Exchange: {0}".format(exchange_name))
        print("Routing key: {0}".format(route_key))
        print(msg.content.body)
        yield channel.basic_ack(delivery_tag=msg.delivery_tag)                                
                                                                                       
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMQP message consumer.")
    parser.add_argument(
        "queue_name",
        action="store",
        help="A message queue name.")
    parser.add_argument(
        "-e",
        "--endpoint",
        action="store",
        help="The client connection endpoint string.")
    parser.add_argument(
        "--vhost",
        action="store",
        default="/",
        help="The port where the exchange can be located.")
    parser.add_argument(
        "-u",
        "--user",
        action="store",
        default="guest",
        help="The user used to log into the exchange.")
    parser.add_argument(
        "--passwd-file",
        action="store",
        type=argparse.FileType('r'),
        help="A file containing the password used to log into the exchange.")
    parser.add_argument(
        "--consumer-tag",
        action="store",
        default="mytag",
        help="The consumer tag for this consume (default 'mytag').")
    args = parser.parse_args()
    try:
        react(main, [args])
    except filepath.UnlistableError as ex:
        print(ex.originalException)
