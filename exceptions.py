class PermanentFailureException(Exception):
    """Exception raised for permanent failures that should not be retried."""
    pass

class TransientFailureException(Exception):
    """Exception raised for transient failures that can be retried."""
    pass