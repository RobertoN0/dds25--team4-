import uuid
from abc import ABC, abstractmethod
from typing import Callable

# Transactions and Reovery Interfaces

class LocalTransactionInterface(ABC):

    def __init__(self, transaction: Callable, saga_correlation_id: str):
        self._args = None
        self._kwargs = None
        self._transaction = transaction
        self.saga_correlation_id = saga_correlation_id
        self.correlation_id = saga_correlation_id
    
    @abstractmethod
    async def execute(self, *args, **kwargs):
        pass

class RecoveryInterface(ABC):
    
    def __init__(self, recovery: Callable, saga_correlation_id: str):
        self._args = None
        self._kwargs = None
        self._recovery = recovery
        self.saga_correlation_id = saga_correlation_id
        self.correlation_id = saga_correlation_id

    @abstractmethod
    async def recover(self, *args, **kwargs):
        pass