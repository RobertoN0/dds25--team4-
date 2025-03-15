import uuid
from typing import Callable

from common.saga.saga_utils.local_transactions import LocalTransaction
from common.saga.saga_utils.recoveries import BackwardRecovery
import logging


class SagaError(BaseException):
    def __init__(self, exception, correlation_id: str = None,):
        self.action = exception
        self.correlation_id = correlation_id
        #self.compensations = compensation_exceptions


class Saga(object):

    def __init__(self, 
                 correlation_id: str, 
                 event_mapping: dict[str, list[str]], 
                 transactions: list[Callable], 
                 compensations: list[Callable],
                 commit_transaction: Callable = None,
                 abort_transaction: Callable = None):
        
        self.correlation_id = correlation_id or uuid.uuid4()
        self._event_mapping: dict[str, list[str]] = event_mapping
        self._transactions: list[LocalTransaction] = [LocalTransaction(t, self.correlation_id) for t in transactions]
        self._compensations: list[BackwardRecovery]= [BackwardRecovery(c, self.correlation_id) for c in compensations]
        self._commit_transaction: LocalTransaction = LocalTransaction(commit_transaction, self.correlation_id)
        self._abort_transaction: LocalTransaction = LocalTransaction(abort_transaction, self.correlation_id)
        self.current_transaction_index: int = 0

    async def execute_transaction(self, transaction: LocalTransaction, *args, **kwargs):
        try:
            kwargs = await transaction.execute(*args, **kwargs) or {}
        except BaseException as e:
            raise SagaError(e, transaction.correlation_id)

        if type(kwargs) is not dict:
            raise SagaError('Transaciton return type should be dict or None but is {}'.format(type(kwargs)), transaction.correlation_id)

        return kwargs


    async def next_transaction(self, *args, **kwargs):
        if self.current_transaction_index < len(self._transactions):
            logging.info(f"[SAGA {self.correlation_id}] - Executing Transaction {self.current_transaction_index + 1}/{len(self._transactions)} - [EVENT-TYPE: {self._event_mapping["CorrectEvents"][self.current_transaction_index]}].")

            try:
                transaction: LocalTransaction = self._transactions[self.current_transaction_index]
                result =  await self.execute_transaction(transaction, *args, **kwargs)
                #logging.info(f"[SAGA {self.correlation_id}] - Transaction Committed {self.current_transaction_index + 1}/{len(self._transactions)} - [EVENT-TYPE: {self._event_mapping["CorrectEvents"][self.current_transaction_index]}].")
                return result
            except SagaError as e:
                raise SagaError(e, transaction.correlation_id)
        else:
            return None

    async def compensate(self, event, *args, **kwargs):
        for compensation_index in range(self.current_transaction_index - 1, -1 , -1):
            logging.info(f"compensating {compensation_index}")
            compensation = self._compensations[compensation_index]
            await compensation.recover(event, *args, **kwargs)
    
    async def commit(self, event: dict, *args, **kwargs):    # Here we assume that the commit execution never raises a SagaError
        event.update({
            "correlation_id": self.correlation_id,
            "type": self._event_mapping["CommitEvent"][0],
            })
        await self._commit_transaction.execute(event, *args, **kwargs)

    async def abort(self, event: dict, *args, **kwargs):     # Here we assume that the abort execution never raises a SagaError
        event.update({
            "correlation_id": self.correlation_id,
            "type": self._event_mapping["AbortEvent"][0],
        })
        await self._abort_transaction.execute(event, *args, **kwargs)

    def is_next(self, event_type: str):
        return self._event_mapping["CorrectEvents"].index(event_type) == self.current_transaction_index

    def is_finished(self): 
        return self.current_transaction_index == len(self._transactions)
    
    def __str__(self):
        return f"Saga {self.correlation_id}"
    
    def mark_transaction_complete(self):
        logging.info(f"[SAGA {self.correlation_id}] - Marking Transaction {self.current_transaction_index + 1} as complete")
        self.current_transaction_index += 1

    def is_expected_event(self, event_type: str) -> bool:
        expected = self._event_mapping["CorrectEvents"][self.current_transaction_index]
        logging.info(f"[SAGA {self.correlation_id}] - Expected event for step {self.current_transaction_index + 1}: {expected} | Received event: {event_type}")
        return event_type == expected


class SagaManager:

    def __init__(self):
        self.ongoing_sagas: dict[str, dict] = {} # "SAGAs UUIDs": <...>


    async def event_handling(self, event: dict, *args, **kwargs):
        event_type: str = event.get("type")
        event_correlation_id: str = event.get("correlation_id")
        saga_id: str = self.get_saga_id_from_event(event_correlation_id)

        if saga_id is None:
            raise RuntimeWarning(f"The event [EVENT-ID: {event_correlation_id}] does not belong to any Saga of the SagaManager.")
            return
        saga: Saga = self.ongoing_sagas.get(saga_id).get("saga_instance")

        async def aborting(error_message):
            await self.compensate_distributed_transaction(saga_id, event)
            await self.abort_distributed_transaction(saga_id, event)
            raise SagaError(error_message)

        if event_type not in saga._event_mapping["CorrectEvents"]:
            if event_type in saga._event_mapping["ErrorEvents"]:
                await aborting(f"[EVENT-ID: {event_correlation_id}] [EVENT-TPYE: {event_type}] - Transaction {saga.current_transaction_index + 1}/{len(saga._transactions)} Aborted. Starting Compensation.")
            else:
                raise RuntimeWarning(f"This event [EVENT-ID: {event_correlation_id}] does not belong to the SAGA [SAGA-ID: {saga_id}]. Event discarded.")

        if saga.is_expected_event(event_type):
            try:
                # Mark the current step as complete
                saga.mark_transaction_complete()
                # If the saga is not finished, send the request for the next step
                if not saga.is_finished():
                    logging.info(f"Launching next step for saga {saga_id}")
                    await saga.next_transaction(event, *args, **kwargs)
                else:
                    # If the saga is finished, commit the distributed transaction
                    logging.info(f"Saga {saga_id} completed. Committing the distributed transaction.")
                    await self.commit_distributed_transaction(saga_id, event)
            except BaseException as e:
                await aborting(e)
        else:
            await aborting(f"Event {event_type} is not expected for the current step in saga {saga_id}.")
        

    def build_distributed_transaction(self, 
                                      saga_id: str, 
                                      event_mapping: dict[str, list[str]], 
                                      transactions: list[Callable], 
                                      compensations: list[Callable],
                                      commit_transaction: Callable = None,
                                      abort_transaction: Callable = None,
                                      *args, 
                                      **kwargs) -> Saga:
        
        saga = Saga(saga_id, event_mapping, transactions, compensations, commit_transaction, abort_transaction)
        
        self.ongoing_sagas.update({
            saga.correlation_id: {
                "saga_instance": saga,
                "transactions": [t.correlation_id for t in saga._transactions], 
                "compensations": [c.correlation_id for c in saga._compensations],
                }
            })
        
        logging.info(f"[SAGA-ID: {saga.correlation_id}] Distributed Transaction Built and Ready.")
        return saga


    async def commit_distributed_transaction(self, saga_correlation_id: str, event):
        saga: Saga = self.ongoing_sagas.get(saga_correlation_id).get("saga_instance")
        await saga.commit(event)
        logging.info(f"[SAGA-ID: {saga.correlation_id}] Distributed Transaction Committed.")
        if saga_correlation_id in self.ongoing_sagas:
            del self.ongoing_sagas[saga_correlation_id]
    

    async def abort_distributed_transaction(self, saga_correlation_id: str, event):
        saga: Saga = self.ongoing_sagas.get(saga_correlation_id).get("saga_instance")
        await saga.abort(event)
        logging.info(f"[SAGA-ID: {saga.correlation_id}] Distributed Transaction Aborted.")
        if saga_correlation_id in self.ongoing_sagas:
            del self.ongoing_sagas[saga_correlation_id]


    async def compensate_distributed_transaction(self, saga_correlation_id: str, event):
        logging.info(f"[SAGA-ID: {saga_correlation_id}] Distributed Transaction Compensation Started.")
        saga: Saga = self.ongoing_sagas.get(saga_correlation_id).get("saga_instance")
        await saga.compensate(event)
        logging.info(f"[SAGA-ID: {saga_correlation_id}] Distributed Transaction Compensation Finished.")

    
    def get_saga_id_from_event(self, correlation_id: str):
        for saga_id, d in self.ongoing_sagas.items():
            if correlation_id in d.get("transactions") or correlation_id in d.get("compensations"):
                return saga_id
        return None 