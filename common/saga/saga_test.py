import unittest
from saga import SagaManager, SagaError

called_compensations = []

def transaction_success(**kwargs):
    context = kwargs.copy()
    context['trans'] = context.get('trans', 0) + 1
    return context

def transaction_failure(**kwargs):
    raise Exception("Transaction failure")

def compensation_dummy(**kwargs):
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
        saga_manager.start_distributed_transaction(event_mapping, transactions, compensations, initial=0)
        
        event1 = {"type": "event1", "correlation_id": list(saga_manager.ongoing_sagas.keys())[0]}
        event2 = {"type": "event2", "correlation_id": list(saga_manager.ongoing_sagas.keys())[0]}
        
        try:
            saga_manager.event_handling(event1, initial=0)
            saga_manager.event_handling(event2, initial=0)
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
        saga_manager.start_distributed_transaction(event_mapping, transactions, compensations, initial=0)
        
        correlation_id = list(saga_manager.ongoing_sagas.keys())[0]
        
        event1 = {"type": "event1", "correlation_id": correlation_id}
        event2 = {"type": "event2", "correlation_id": correlation_id}
        
        try:
            saga_manager.event_handling(event1, initial=0)
            saga_manager.event_handling(event2, initial=0)  # Questa transazione fallirà
        except SagaError as e:
            # Verifica che la compensazione sia stata chiamata correttamente
            self.assertEqual(called_compensations, ["compensation_dummy"])
            self.assertIn("Transaction failure", str(e.action))
        else:
            self.fail("Expected SagaError not raised")

if __name__ == "__main__":
    unittest.main()
