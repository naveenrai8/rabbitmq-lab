import argparse
from time import sleep

from pika.adapters.blocking_connection import BlockingChannel

from rabbitmq_lab.common.connections import connect

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

def publish_wrapper(channel: BlockingChannel, msg: str) -> None:
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
        body=msg,
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        mandatory=True,
    )

def consume_wrapper(channel: BlockingChannel) -> None:
    pass


def publish(video_id: str, number_of_comments: int):
    with connect() as connection:
        # SUGGESTION: this docstring sits inside a `with` block, so it does nothing. Move it under `def` or delete it.
        """
        establish channel
        """
        channel = connection.channel()
        declare_exchange(channel)

        channel.confirm_delivery()
        for i in range(number_of_comments):
            sleep(1)
            msg = f"Video {video_id} comment {i}"
            # SUGGESTION: add mandatory=True so an unroutable message raises pika.exceptions.UnroutableError.
            # Without it, confirm_delivery() still "confirms" a message the broker dropped.
            # delivery_mode=Persistent makes the message survive a broker restart (the queue is durable, the message is not by default).
            #
            publish_wrapper(channel, msg)
            # channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY, body=msg)
            print(f"Sent message: {msg}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish comments for a live video")
    parser.add_argument("--video", type=str, default="video-1",  help="Video ID", required=False)
    parser.add_argument("--count", type=int, default=10, help="Number of comments to publish")
    args = parser.parse_args()


    # The queue + binding must exist BEFORE we publish, or mandatory=True raises UnroutableError.
    # In production the consumer is a separate process that starts first. This script plays both roles, so set up the consumer side first.
    with connect() as connection:
        channel = connection.channel()
        declare_exchange(channel)
        declare_queue(channel)

    publish(args.video, args.count)


if __name__ == "__main__":
    main()
