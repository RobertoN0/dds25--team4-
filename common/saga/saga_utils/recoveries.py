from .interfaces import RecoveryInterface

# TODO: IMplement ForwardRecovery Class
class ForwardRecovery(RecoveryInterface):
    
    def __init__(self, recovery, correlation_id):
        super().__init__(recovery, correlation_id)
    
    def recover(self, *args, **kwargs):
        print(f"[{self.correlation_id}] - Forward recovery triggered")
        self._kwargs = kwargs
        self._args = args
        self.retry()

    def retry(self):
        pass

class BackwardRecovery(RecoveryInterface):
    
    def __init__(self, recovery, correlation_id):
        super().__init__(recovery, correlation_id)
    
    def recover(self, *args, **kwargs):
        print(f"[{self.correlation_id}] - Backward recovery triggered")
        self._args = args
        self._kwargs = kwargs
        return self.compensate()

    def compensate(self):
        recovery_result = self._recovery(**self._args, **self._kwargs)

        if not isinstance(recovery_result, dict):
            raise TypeError("BackwardRecovery results must return dictionaries as {{'var_name': 'result'}}. They may contain more than one key-value pair.")

        return recovery_result