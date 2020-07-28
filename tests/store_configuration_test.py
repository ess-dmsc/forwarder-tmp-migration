from unittest import mock
from confluent_kafka import Consumer
from streaming_data_types.forwarder_config_update_rf5k import (
    serialise_rf5k,
    StreamInfo,
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from forwarder.parse_config_update import (
    parse_config_update,
    config_change_to_command_type,
)

from forwarder.configuration_store import ConfigurationStore
from forwarder.parse_config_update import Channel, EpicsProtocol
from tests.kafka.fake_producer import FakeProducer


DUMMY_UPDATE_HANDLER = None

CHANNELS_TO_STORE = {
    Channel("channel1", EpicsProtocol.PVA, "topic1", "f142"): DUMMY_UPDATE_HANDLER,
    Channel("channel2", EpicsProtocol.CA, "topic2", "tdct"): DUMMY_UPDATE_HANDLER,
}

STREAMS_TO_RETRIEVE = [
    StreamInfo(
        channel.name,
        channel.schema,
        channel.output_topic,
        Protocol.Protocol.PVA
        if channel.protocol == EpicsProtocol.PVA
        else Protocol.Protocol.CA,
    )
    for channel in CHANNELS_TO_STORE.keys()
]


class FakeKafkaMessage:
    def __init__(self, message):
        self._message = message

    def value(self):
        return self._message


def assert_stored_channel_correct(outputted_channel):
    # Will only be found if key exists and as the key is the channel
    # it will only match if the values are exactly the same.
    assert outputted_channel in CHANNELS_TO_STORE


def test_when_multiple_pvs_dumped_config_contains_all_pv_details():
    producer = FakeProducer()
    store = ConfigurationStore(producer, consumer=None, topic="store_topic")

    store.save_configuration(CHANNELS_TO_STORE)

    stored_message = parse_config_update(producer.published_payload)  # type: ignore
    stored_channels = stored_message.channels

    assert_stored_channel_correct(stored_channels[0])  # type: ignore
    assert_stored_channel_correct(stored_channels[1])  # type: ignore


def test_when_no_pvs_stored_message_type_is_remove_all():
    producer = FakeProducer()
    store = ConfigurationStore(producer, consumer=None, topic="store_topic")

    store.save_configuration({})

    stored_message = parse_config_update(producer.published_payload)  # type: ignore

    assert stored_message.channels is None
    assert (
        stored_message.command_type
        == config_change_to_command_type[UpdateType.REMOVEALL]
    )


def test_retrieving_stored_info_with_no_pvs_gets_message_without_streams():
    mock_consumer = mock.create_autospec(Consumer)
    mock_consumer.get_watermark_offsets.return_value = (0, 100)
    message = serialise_rf5k(UpdateType.REMOVEALL, [])
    mock_consumer.consume.return_value = [FakeKafkaMessage(message)]
    store = ConfigurationStore(
        producer=None, consumer=mock_consumer, topic="store_topic"
    )

    config = parse_config_update(store.retrieve_configuration())

    assert config.channels is None


def test_retrieving_stored_info_with_multiple_pvs_gets_streams():
    mock_consumer = mock.create_autospec(Consumer)
    mock_consumer.get_watermark_offsets.return_value = (0, 100)
    message = serialise_rf5k(UpdateType.ADD, STREAMS_TO_RETRIEVE)
    mock_consumer.consume.return_value = [FakeKafkaMessage(message)]
    store = ConfigurationStore(
        producer=None, consumer=mock_consumer, topic="store_topic"
    )

    config = parse_config_update(store.retrieve_configuration())
    channels = config.channels

    assert_stored_channel_correct(channels[0])  # type: ignore
    assert_stored_channel_correct(channels[1])  # type: ignore
