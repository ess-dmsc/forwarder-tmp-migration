import time
from logging import Logger
from typing import Any, Dict

import graphyte  # type: ignore

from forwarder.common import Channel
from forwarder.repeat_timer import RepeatTimer
from forwarder.update_handlers.create_update_handler import UpdateHandler
from forwarder.utils import Counter

STATISTICS_REPORTER = None

def set_statistics_reporter(reporter):
    global STATISTICS_REPORTER
    STATISTICS_REPORTER = reporter

def get_statistics_reporter():
    return STATISTICS_REPORTER

class StatisticsReporter:
    def __init__(
        self,
        graphyte_server: str,
        update_handlers: Dict[Channel, UpdateHandler],
        update_msg_counter: Counter,
        update_buffer_err_counter: Counter,
        update_delivery_err_counter: Counter,
        receive_latency_counter: Counter,
        processing_latency_counter: Counter,
        send_latency_counter: Counter,
        logger: Logger,
        prefix: str = "forwarder",
        update_interval_s: int = 10,
    ):
        self._graphyte_server = graphyte_server
        self._update_handlers = update_handlers
        self._update_msg_counter = update_msg_counter
        self._update_buffer_err_counter = update_buffer_err_counter
        self._update_delivery_err_counter = update_delivery_err_counter
        self._receive_latency_counter = receive_latency_counter
        self._processing_latency_counter = processing_latency_counter
        self._send_latency_counter = send_latency_counter
        self._logger = logger

        self._sender = graphyte.Sender(self._graphyte_server, prefix=prefix)
        self._repeating_timer = RepeatTimer(update_interval_s, self.send_statistics)

    def start(self):
        self._repeating_timer.start()

    def send_statistic(self, metric: Any, value: Any, tags: Any = {}):
        timestamp = time.time()
        try:
            self._sender.send(
                metric, value, timestamp
            )
        except Exception as ex:
            self._logger.error(f"Could not send statistic {metric}({tags}): {ex}")

    def send_statistics(self):
        timestamp = time.time()
        try:
            self._sender.send(
                "number_pvs", len(self._update_handlers.keys()), timestamp
            )
            self._sender.send(
                "total_updates", self._update_msg_counter.value, timestamp
            )
            self._sender.send(
                "data_loss_errors", self._update_buffer_err_counter.value, timestamp
            )
            self._sender.send(
                "kafka_delivery_errors",
                self._update_delivery_err_counter.value,
                timestamp,
            )
            self._sender.send(
                "receive_latency", self._receive_latency_counter.value, timestamp
            )
            self._sender.send(
                "processing_latency", self._processing_latency_counter.value, timestamp
            )
            self._sender.send(
                "send_latency", self._send_latency_counter.value, timestamp
            )
        except Exception as ex:
            self._logger.error(f"Could not send statistic: {ex}")

    def stop(self):
        if self._repeating_timer:
            self._repeating_timer.cancel()
