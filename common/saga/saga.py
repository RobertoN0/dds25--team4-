import uuid
from typing import Callable
from saga_utils import BackwardRecovery, LocalTransaction


class SagaError(BaseException):
    def __init__(self, exception):
        self.action = exception
        #self.compensations = compensation_exceptions


class Saga(object):

    def __init__(self, event_mapping: list, transactions: list[Callable], compensations: list[Callable]):
        self.correlation_id = uuid.uuid4()
        self._event_mapping: dict[list] = event_mapping
        self._transactions: list[LocalTransaction] = [LocalTransaction(t, self.correlation_id) for t in transactions]
        self._compensations: list[BackwardRecovery]= [BackwardRecovery(c, self.correlation_id) for c in compensations]
        self.current_transaction_id: int = -1

    def execute_transaction(self, transaction: LocalTransaction, *args, **kwargs):
        try:
            kwargs = transaction.execute(*args, **kwargs) or {}
        except BaseException as e:
            raise SagaError(e)

        if type(kwargs) is not dict:
            raise SagaError('Transaciton return type should be dict or None but is {}'.format(type(kwargs)))

        return kwargs


    def next_transaction(self, *args, **kwargs):
        if self.current_transaction_id + 1 < len(self._transactions):
                
            self.current_transaction_id += 1
            print(f"[SAGA {self.correlation_id}] - Executing Transaction {self.current_transaction_id + 1}/{len(self._transactions)} - [EVENT-TYPE: {self._event_mapping["CorrectEvents"][self.current_transaction_id]}].")

            try:
                transaction = self._transactions[self.current_transaction_id]
                result =  self.execute_transaction(transaction, *args, **kwargs)
                print(f"[SAGA {self.correlation_id}] - Transaction Committed {self.current_transaction_id + 1}/{len(self._transactions)} - [EVENT-TYPE: {self._event_mapping["CorrectEvents"][self.current_transaction_id]}].")
                return result
            except BaseException as e:
                self.current_transaction_id -= 1
                raise SagaError(e)
        else:
            return None

    def compensate(self, event, *args, **kwargs):
        for compensation_index in range(self.current_transaction_id - 1, -1 , -1):
            print("HERE")
            print("compensating", compensation_index)
            compensation = self._compensations[compensation_index]
            compensation.compensate(event, *args, **kwargs)
    
    def is_next(self, event_type: str):
        return self._event_mapping["CorrectEvents"].index(event_type) == self.current_transaction_id + 1



class SagaManager:

    def __init__(self):
        self.ongoing_sagas: dict[str, dict] = {} # "SAGAs UUIDs": <...>


    def event_handling(self, event, *args, **kwargs):
        event_type: str = event["type"]
        event_correlation_id: str = event["correlation_id"]
        saga_id: str = self.get_saga_id_from_event(event_correlation_id)

        if saga_id is None:
            raise RuntimeWarning(f"The event [EVENT-ID: {event_correlation_id}] does not belong to any Saga of the SagaManager.")
            return
        
        saga: Saga = self.ongoing_sagas.get(saga_id)["saga_instance"]

        def aborting(error_message):
            self.compensate_distributed_transaction(saga_id, event)
            self.abort_distributed_trasaction(saga_id)
            raise SagaError(error_message)

        if event_type not in saga._event_mapping["CorrectEvents"]:
            if event_type in saga._event_mapping["ErrorEvents"]:
                aborting(f"[EVENT-ID: {event_correlation_id}] [EVENT-TPYE: {event_type}] - Transaction {saga.current_transaction_id + 1}/{len(saga._transactions)} Aborted. Starting Compensation.")
            else:
                raise RuntimeWarning(f"This event [EVENT-ID: {event_correlation_id}] does not belong to the SAGA [SAGA-ID: {saga_id}]. Event discarded.")
        
        if saga.is_next(event_type):   # Base Case
            try:
                result = saga.next_transaction(event, *args, **kwargs)
                if result is None:
                    self.commit_distributed_transaction(saga_id)
            except BaseException as e:
                aborting(e)
        else:
            aborting(f"This event [EVENT-ID: {event_correlation_id}] [EVENT-TPYE: {event_type}] this event should be executed after.")
        

    def start_distributed_transaction(self, event_mapping: dict[list], transactions: list[Callable], compensations: list[Callable], *args, **kwargs):
        saga = Saga(event_mapping, transactions, compensations)
        
        self.ongoing_sagas.update({
            saga.correlation_id: {
                "saga_instance": saga,
                "transactions": [t.correlation_id for t in saga._transactions], 
                "compensations": [c.correlation_id for c in saga._compensations],
                }
            })
        
        print(f"[SAGA-ID: {saga.correlation_id}] Distributed Transaction Started.")

        result = saga.next_transaction(*args, **kwargs)
        if result is None:
            self.commit_distributed_transaction(saga.correlation_id)


    def commit_distributed_transaction(self, saga_correlation_id: str):
        saga_instance = self.ongoing_sagas.pop(saga_correlation_id)["saga_instance"]
        print(f"[SAGA-ID: {saga_instance.correlation_id}] Distributed Transaction Committed.")
        del saga_instance
    

    def abort_distributed_trasaction(self, saga_correlation_id: str):
        saga_instance = self.ongoing_sagas.pop(saga_correlation_id)["saga_instance"]
        print(f"[SAGA-ID: {saga_instance.correlation_id}] Distributed Transaction Aborted.")
        del saga_instance


    def compensate_distributed_transaction(self, saga_correlation_id: str, event):
        print(f"[SAGA-ID: {saga_correlation_id}] Distributed Transaction Compensation Started.")
        saga = self.ongoing_sagas.get(saga_correlation_id)["saga_instance"]
        saga.compensate(event)
        print(f"[SAGA-ID: {saga_correlation_id}] Distributed Transaction Compensation Finished.")

    
    def get_saga_id_from_event(self, correlation_id: str):
        for saga_id, d in self.ongoing_sagas.items():
            if correlation_id in d["transactions"]:
                return saga_id
        return None 