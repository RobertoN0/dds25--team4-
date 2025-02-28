import uuid
from typing import Callable
from saga_utils import BackwardRecovery, LocalTransaction

SAGA_TYPES = (0, "choreography", 1, "orchestrator")

class SagaError(BaseException):
    def __init__(self, exception, compensation_exceptions):
        self.action = exception
        self.compensations = compensation_exceptions


class Saga(object):

    def __init__(self, type, transactions: list[Callable], compensations: list[Callable]):
        if type == 0 or type == "choreography":
            raise NotImplementedError("Choreographed SAGA not implemented yet.")
        if type not in SAGA_TYPES:
            raise AttributeError("SAGA type could be only 0 ('choreography') or 1 ('orchestrator')")
        self.correlation_id = str(uuid.uuid4())       # This generates an UUID (Universal Unique IDentifier) for a specific SAGA. It should be attached to ANY transaction within the SAGA.
        self._type = type

        self._transactions = [LocalTransaction(t, self.correlation_id) for t in transactions]
        self._compensations = [BackwardRecovery(c, self.correlation_id) for c in compensations]


    def execute(self, **kwargs):
        print(f"[SAGA {self.correlation_id}] - Executing SAGA of type {SAGA_TYPES[self._type]}")
        
        for transaction_index, transaction in enumerate(self._transactions):
            try:
                kwargs = transaction.execute(**kwargs) or {}
            except BaseException as e:
                compensation_exceptions = self._abort(transaction_index)
                raise SagaError(e, compensation_exceptions)

            print(f"[SAGA {self.correlation_id}] - Transaction {transaction_index + 1} of {len(self._transactions)} COMMITTED.")
            if type(kwargs) is not dict:
                raise TypeError('Transaciton return type should be dict or None but is {}'.format(type(kwargs)))

        self._commit()


    def _abort(self, transaction_index):
        print(f"[SAGA {self.correlation_id}] - Aborting SAGA, starting recovery process. Transaction {transaction_index + 1}^ of {len(self._transactions)} failed.")

        kwargs = {}
        compensation_exceptions = []
        for compensation_index in range(transaction_index - 1, -1, -1):
            try:
                kwargs = self._compensations[compensation_index].recover(**kwargs) or {}
            except BaseException as e:
                compensation_exceptions.append(e)
        return compensation_exceptions

    def _commit(self):
        print(f"[SAGA {self.correlation_id}] - SAGA COMMITTED!")