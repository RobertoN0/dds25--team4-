from abc import ABC, abstractmethod
from typing import Callable

# Transactions and Reovery Interfaces

class LocalTransactionInterface(ABC):

    def __init__(self, transaction: Callable, correlation_id: str):
        self._kwargs = None
        self._transaction = transaction
        self.correlation_id = correlation_id
    
    @abstractmethod
    def execute(self, **kwargs):
        pass

class RecoveryInterface(ABC):
    
    def __init__(self, recovery: Callable, correlation_id: str):
        self._kwargs = None
        self._recovery = recovery
        self.correlation_id = correlation_id

    @abstractmethod
    def recover(self, **kwargs):
        pass