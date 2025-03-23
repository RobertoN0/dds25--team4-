import unittest
from saga import SagaManager, SagaError, Saga

called_compensations = []

def transaction_success(event, **kwargs):
    context = kwargs.copy()
    context['trans'] = context.get('trans', 0) + 1
    return context

def transaction_failure(event, **kwargs):
    raise Exception("Transaction failure")

def compensation_dummy(event, **kwargs):
    global called_compensations
    called_compensations.append("compensation_dummy")
    context = kwargs.copy()
    context['compensated'] = True
    return context

class TestSagaManager(unittest.TestCase):

    def setUp(self):
        global called_compensations
        called_compensations = []

    def test_saga_success(self):
        transactions = [transaction_success, transaction_success]
        compensations = [compensation_dummy, compensation_dummy]
        event_mapping = {
            "CorrectEvents": ["event1", "event2"],
            "ErrorEvents": ["error1", "error2"]
        }
        
        saga_manager = SagaManager()
        saga_manager.build_distributed_transaction(event_mapping, transactions, compensations)
        
        saga_id = list(saga_manager.ongoing_sagas.keys())[0]
        saga: Saga = saga_manager.ongoing_sagas.get(saga_id)["saga_instance"]
        transaction_correlation_ids = saga_manager.ongoing_sagas.get(saga_id).get("transactions")

        event1 = {"type": "event1", "correlation_id": transaction_correlation_ids[0]}
        event2 = {"type": "event2", "correlation_id": transaction_correlation_ids[1]}
        

        try:
            saga_manager.event_handling(event1)
            saga_manager.event_handling(event2)
        except SagaError:
            self.fail("Unexpected SagaError in success scenario")
        
        self.assertEqual(called_compensations, [])

    def test_saga_failure(self):
        transactions = [transaction_success, transaction_failure, transaction_success]
        compensations = [compensation_dummy, compensation_dummy, compensation_dummy]
        event_mapping = {
            "CorrectEvents": ["event1", "event2", "event3"],
            "ErrorEvents": ["error1", "error2", "error3"]
        }
        
        saga_manager = SagaManager()
        saga_manager.build_distributed_transaction(event_mapping, transactions, compensations)

        saga_id = list(saga_manager.ongoing_sagas.keys())[0]
        saga: Saga = saga_manager.ongoing_sagas.get(saga_id)["saga_instance"]
        transaction_correlation_ids = saga_manager.ongoing_sagas.get(saga_id).get("transactions")
        
        
        event1 = {"type": "event1", "correlation_id": transaction_correlation_ids[0]}
        event2 = {"type": "event2", "correlation_id": transaction_correlation_ids[1]}
        
        try:
            saga_manager.event_handling(event1)
            saga_manager.event_handling(event2)  # Questa transazione fallirà
        except SagaError as e:
            # Verifica che la compensazione sia stata chiamata correttamente
            self.assertEqual(called_compensations, ["compensation_dummy"])
            self.assertIn("Transaction failure", str(e.action))
        else:
            self.fail("Expected SagaError not raised")

if __name__ == "__main__":
    unittest.main()
