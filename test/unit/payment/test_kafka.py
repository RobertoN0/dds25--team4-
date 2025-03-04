import unittest
from unittest.mock import AsyncMock, patch
import asyncio

import payment.routing.kafka as kafka  # Import your Kafka event handling module


class TestKafka(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        """Set up mock Kafka producer and consumer before each test."""
        self.mock_producer = AsyncMock()
        self.mock_consumer = AsyncMock()

        # Patch Kafka producer and consumer singletons
        patcher_producer = patch("payment.routing.kafka.KafkaProducer", self.mock_producer)
        patcher_consumer = patch("payment.routing.kafka.KafkaConsumer", self.mock_consumer)
        self.addCleanup(patcher_producer.stop)
        self.addCleanup(patcher_consumer.stop)
        patcher_producer.start()
        patcher_consumer.start()


    async def test_handle_pay_event_success(self):
        """Test handling a successful payment event."""
        amount = 50
        new_credit = 25
        event = {"type": "pay", "uuid": "user123", "amount": amount}

        with patch("payment.routing.kafka.remove_credit", new_callable=AsyncMock) as mock_remove_credit:
            mock_remove_credit.return_value = (new_credit, None)

            await kafka.handle_pay_event(event)

            # Ensure the payment logic was called
            mock_remove_credit.assert_called_once_with("user123", amount)

            # Ensure Kafka producer sends a success event
            self.mock_producer.send_event.assert_called_once_with(
                "payment", "payment-success", {"type": "pay", "uuid": "user123", "amount": amount, "credit": new_credit}
            )


    async def test_handle_pay_event_failure(self):
        """Test handling a payment event with insufficient funds."""
        amount = 50
        event = {"type": "pay", "uuid": "user123", "amount": amount}

        with patch("payment.routing.kafka.remove_credit", new_callable=AsyncMock) as mock_remove_credit:
            mock_remove_credit.return_value = ("", Exception("Not enough credit"))

            await kafka.handle_pay_event(event)

            # Ensure the payment logic was called
            mock_remove_credit.assert_called_once_with("user123", amount)

            # Ensure Kafka producer sends an error event
            self.mock_producer.send_event.assert_called_once_with(
                "payment", "payment-error", {"type": "pay", "uuid": "user123", "amount": amount, "error": "Not enough credit"}
            )


    async def test_handle_refund_event_success(self):
        """Test handling a successful refund event."""
        amount = 50
        new_credit = 75
        event = {"type": "refund", "uuid": "user123", "amount": amount}

        with patch("payment.routing.kafka.add_credit", new_callable=AsyncMock) as mock_add_credit:
            mock_add_credit.return_value = (new_credit, None)

            await kafka.handle_refund_event(event)

            # Ensure the refund logic was called
            mock_add_credit.assert_called_once_with("user123", amount)

            # Ensure Kafka producer sends a success event
            self.mock_producer.send_event.assert_called_once_with(
                "payment", "refund-success", {"type": "refund", "uuid": "user123", "amount": amount, "credit": new_credit}
            )


    async def test_handle_refund_event_failure(self):
        """Test handling a refund event that fails."""
        amount = 50
        event = {"type": "refund", "uuid": "user123", "amount": amount}

        with patch("payment.routing.kafka.add_credit", new_callable=AsyncMock) as mock_add_credit:
            mock_add_credit.return_value = ("", Exception("DB error"))

            await kafka.handle_refund_event(event)

            # Ensure the refund logic was called
            mock_add_credit.assert_called_once_with("user123", amount)

            # Ensure Kafka producer sends an error event
            self.mock_producer.send_event.assert_called_once_with(
                "payment", "refund-error", {"type": "refund", "uuid": "user123", "amount": amount, "error": "DB error"}
            )


    async def test_kafka_init(self):
        """Test initializing Kafka consumer and producer."""
        await kafka.init()

        # Ensure Kafka consumer and producer are started
        self.mock_consumer.get_instance.assert_called_once()
        self.mock_producer.get_instance.assert_called_once()

        # Ensure Kafka producer sends startup event
        self.mock_producer.send_event.assert_called_once_with(
            "app-events", "payment-startup",
            {
                "type": "AppStarted",
                "service": "payment-service",
                "message": "Payment Service is up and running!"
            }
        )

    async def test_kafka_close(self):
        """Test closing Kafka consumer and producer."""
        await kafka.close()

        # Ensure Kafka consumer and producer are stopped
        self.mock_producer.close.assert_called_once()
        self.mock_consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
