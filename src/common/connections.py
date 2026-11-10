import logging
import os
from dataclasses import dataclass
from typing import Dict

import pika

rmq_profile = os.getenv("RMQ_PROFILE", "single")
rmq_user = os.getenv("RMQ_USER", "guest")
rmq_pass = os.getenv("RMQ_PASSWORD", "guest")
rmq_vhost = os.getenv("RMQ_VHOST", "/")


@dataclass(frozen=True)
class RabbitMQEndpoints:
    host: str
    port: int
    mgmt_port: int
    stream_port: int


def _endpoints() -> RabbitMQEndpoints:
    endpoint_lambda = lambda x, default: int(os.getenv(x, default))
    return RabbitMQEndpoints(
        host="localhost",
        port=5672,
        mgmt_port= 15672,
        stream_port= 5552,
    )


def pika_connection_settings(**overrides) -> pika.ConnectionParameters:
    endpoints = _endpoints()
    connection_params = dict(
        host=endpoints.host,
        port=endpoints.port,
        virtual_host=rmq_vhost,
        credentials=pika.PlainCredentials(rmq_user, rmq_pass),
        heartbeat=20,
        blocked_connection_timeout=60,
    )
    connection_params.update(overrides)
    return pika.ConnectionParameters(**connection_params)

def connect(**overrides) -> pika.BlockingConnection:
    """Open one blocking connection.

        pika is not thread-safe at the *connection* level: in pika, use one connection per thread
        (the generic AMQP advice "one channel per thread" assumes a thread-safe client like Java's).
    """
    connection_settings = pika_connection_settings(**overrides)
    return pika.BlockingConnection(connection_settings)