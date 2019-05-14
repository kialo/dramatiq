from datetime import datetime, timedelta
from queue import PriorityQueue
from unittest.mock import Mock

from dramatiq import Broker, Consumer, Message, MessageProxy
from dramatiq.worker import _ConsumerThread

from .common import worker


def test_workers_dont_register_queues_that_arent_whitelisted(stub_broker):
    # Given that I have a worker object with a restricted set of queues
    with worker(stub_broker, queues={"a", "b"}) as stub_worker:
        # When I try to register a consumer for a queue that hasn't been whitelisted
        stub_broker.declare_queue("c")
        stub_broker.declare_queue("c.DQ")

        # Then a consumer should not get spun up for that queue
        assert "c" not in stub_worker.consumers
        assert "c.DQ" not in stub_worker.consumers


def test_consumer_is_notified_when_message_is_immediately_processed():
    consumer = Mock(spec=Consumer)

    process_single_message(consumer, message_options={})

    consumer.notify_message_is_ready_for_processing.assert_called_once()


def test_consumer_is_notified_when_message_with_eta_in_past_is_processed():
    consumer = Mock(spec=Consumer)
    time_in_past = (datetime.now() - timedelta(hours=1)).timestamp() * 1000

    process_single_message(consumer, message_options={'eta': time_in_past})

    consumer.notify_message_is_ready_for_processing.assert_called_once()


def test_consumer_is_not_notified_when_message_with_eta_in_future_is_put_on_delay_queue():
    consumer = Mock(spec=Consumer)
    time_in_future = (datetime.now() + timedelta(hours=1)).timestamp() * 1000

    process_single_message(consumer, message_options={'eta': time_in_future})

    consumer.notify_message_is_ready_for_processing.assert_not_called()


def process_single_message(consumer, message_options):
    broker = Mock(spec=Broker)
    work_queue = PriorityQueue()
    consumer_thread = _ConsumerThread(
        broker=broker, queue_name='queue', prefetch=2, work_queue=work_queue, worker_timeout=1000
    )

    def mocked_next_message():
        consumer_thread.stop()
        message = Message(queue_name='queue', actor_name='actor', args=[], kwargs={}, options=message_options)
        return MessageProxy(message)

    broker.consume = Mock(return_value=consumer)
    consumer.__iter__ = Mock(return_value=consumer)
    consumer.__next__ = Mock(side_effect=mocked_next_message)

    consumer_thread.run()
