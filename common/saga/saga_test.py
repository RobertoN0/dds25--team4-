import unittest
from saga import Saga, SagaError

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

class TestSaga(unittest.TestCase):

    def setUp(self):
        global called_compensations
        called_compensations = []

    def test_saga_success(self):
        transactions = [transaction_success, transaction_success]
        compensations = [compensation_dummy, compensation_dummy]
        
        saga = Saga(1, transactions, compensations)
        try:
            saga.execute(initial=0)
        except SagaError:
            self.fail("Unexpected SagaError in success scenario")
        
        self.assertEqual(called_compensations, [])

    def test_saga_failure(self):
        transactions = [transaction_success, transaction_failure, transaction_success]
        compensations = [compensation_dummy, compensation_dummy, compensation_dummy]
        
        saga = Saga(1, transactions, compensations)
        
        with self.assertRaises(SagaError) as context:
            saga.execute(initial=0)
        
        self.assertEqual(called_compensations, ["compensation_dummy"])
        self.assertIn("Transaction failure", str(context.exception.action))

if __name__ == "__main__":
    unittest.main()