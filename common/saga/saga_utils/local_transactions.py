from typing import Callable
from .interfaces import LocalTransactionInterface

class LocalTransaction(LocalTransactionInterface):

    def __init__(self, transaction: Callable, correlation_id):
        super().__init__(transaction, correlation_id)

    def execute(self, **kwargs):
        self._kwargs = kwargs

        transaction_result = self._transaction(**kwargs)
        
        if not isinstance(transaction_result, dict):
            raise TypeError("LocalTransaction results must return dictionaries as {{'var_name': 'result'}}. They may contain more than one key-value pair.")

        return transaction_result