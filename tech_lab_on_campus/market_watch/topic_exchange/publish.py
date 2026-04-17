# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
import pika

from solution.producer_sol import mqProducer  # pylint: disable=import-error


def main(ticker: str, price: float, sector: str) -> None:
    
    # Implement Logic to Create Routing Key from the ticker and sector variable -  Step 2
    #
    #                       WRITE CODE HERE!!!
    #
    # We'll first set up the connection and channel
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq', port=5672))
    channel = connection.channel()

    # Declare the topic exchange
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    # Set the routing key and publish a message with that topic exchange:
    routing_key = ticker + "." + sector if len(sys.argv) > 2 else 'anonymous.info'
    message = ticker + " price is now $" + str(price) if ticker or price or sector else 'Hello World!'
    channel.basic_publish(
        exchange='topic_logs', routing_key=routing_key, body=message)
    print(f" [x] Sent {routing_key}:{message}")

    producer = mqProducer(routing_key=routing_key,exchange_name="Tech Lab Topic Exchange")


    # Implement Logic To Create a message variable from the variable EG. "TSLA price is now $500" - Step 3
    #
    #                       WRITE CODE HERE!!!
    #
    
    producer.publishOrder(message)

if __name__ == "__main__":

    # Implement Logic to read the ticker, price and sector string from the command line and save them - Step 1
    #
    #                       WRITE CODE HERE!!!
    #
    ticker = price = sector = None
    
    if len(sys.argv) > 3:
        ticker = sys.argv[1]
        price = sys.argv[2]
        sector = sys.argv[3]

    sys.exit(main(ticker,price,sector))
