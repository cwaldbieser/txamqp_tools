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
from txamqp.content import Content
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
        'exchange': args.exchange,
        'content': args.msg_file.read(),
        'route_key': args.route_key,
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
    try:
        user, passwd = params['creds']
        yield conn.authenticate(user, passwd)
        print("[DEBUG] Authenticated.", file=sys.stderr)
        channel = yield conn.channel(1)                                                    
        yield channel.channel_open()                                                       
        print("[DEBUG] Channel opened.", file=sys.stderr)
        exchange = params["exchange"]
        print("[DEBUG] Exchange '{0}'.".format(exchange), file=sys.stderr)
        # delivery_mode: 1 - non-persistent
        #                2 - persistent
        # Other properties:
        #  'content type'
        #  'content encoding'
        #  'application headers'
        #  'priority'
        #  'correlation id'
        #  'reply to'
        #  'expiration'
        #  'message id'
        #  'timestamp'
        #  'type'
        #  'user id'
        #  'app id'
        #  'cluster id'
        content = params["content"]
        print("[DEBUG] Content: {0}".format(content), file=sys.stderr)
        msg = Content(content)
        msg["delivery mode"] = 2
        route_key = params["route_key"]
        print("[DEBUG] Routing key '{0}'.".format(route_key), file=sys.stderr)
        channel.basic_publish(
            exchange=exchange,
            content=msg,
            routing_key=route_key)
        print("[DEBUG] Message sent.", file=sys.stderr)
        yield channel.channel_close()
        print("[DEBUG] Channel closed.", file=sys.stderr)
        channel0 = yield conn.channel(0)
        yield channel0.connection_close()
        print("[DEBUG] Connection closed.", file=sys.stderr)
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
        consumer_tag, delivery_tag, redelivered, exchange_name, route_key = msg.fields
        print("Consumer tag: {0}".format(consumer_tag))
        print("Delivery tag: {0}".format(msg.delivery_tag))
        print("Redelivered: {0}".format(redelivered))
        print("Exchange: {0}".format(exchange_name))
        print("Routing key: {0}".format(route_key))
        print(msg.content.body)
        yield channel.basic_ack(delivery_tag=msg.delivery_tag)                                
                                                                                       
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMQP message producer.")
    parser.add_argument(
        "exchange",
        action="store",
        help="The exchange to which the message is sent.")
    parser.add_argument(
        "route_key",
        action="store",
        help="The route key used to send the message.")
    parser.add_argument(
        "msg_file",
        action="store",
        type=argparse.FileType('r'),
        help="A file containing the message to send (use '-' for STDIN).")
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
    parser.add_argument(
        "-e",
        "--endpoint",
        action="store",
        help="The client connection endpoint string.")
    args = parser.parse_args()
    try:
        react(main, [args])
    except filepath.UnlistableError as ex:
        print(ex.originalException)
    except Exception as ex:
        print(ex)
