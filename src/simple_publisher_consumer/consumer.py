import argparse

from pika.adapters.blocking_connection import BlockingChannel

from common.connections import connect

EXCHANGE_NAME = "comments"
ROUTING_KEY = "r_k"
QUEUE_NAME = "queue_1"

import pika

# Topology ownership: the publisher owns the exchange, the consumer owns its queue and binding.
# The publisher only knows EXCHANGE_NAME + ROUTING_KEY. It never needs to know QUEUE_NAME.

def declare_exchange(channel: BlockingChannel) -> None:
    """Publisher side: declare the exchange we publish to. Safe to call from both sides (same args → no 406)."""
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)

def declare_queue(channel: BlockingChannel) -> None:
    """Consumer side: declare our queue and bind it to the exchange with the routing key we want."""
    declare_exchange(channel)  # the bind fails with 404 if the exchange doesn't exist yet, so make sure it does
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY)

def on_message(channel, method, properties, body) -> None:
    print(f"Received message: {body.decode()}", flush=True)
    channel.basic_ack(delivery_tag=method.delivery_tag)     # manual ack: a crash won't lose the message


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume comments for a live video")
    parser.add_argument("--video", type=str, default="video-1",  help="Video ID", required=False)
    args = parser.parse_args()


    # The queue + binding must exist BEFORE we publish, or mandatory=True raises UnroutableError.
    # In production the consumer is a separate process that starts first. This script plays both roles, so set up the consumer side first.
    with connect() as connection:
        channel = connection.channel()
        declare_exchange(channel)
        declare_queue(channel)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
        try:
            print("Starting consumer")
            channel.start_consuming()
        except KeyboardInterrupt:
            print("Stopping consumer")
            channel.stop_consuming()

if __name__ == "__main__":
    main()
