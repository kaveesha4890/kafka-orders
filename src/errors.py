"""Error taxonomy for the order processing pipeline.

The distinction between transient and permanent is the single most
important design decision in the retry/DLQ mechanism. Retrying a
permanent failure blocks the partition forever; sending a transient
failure to the DLQ loses recoverable data.
"""


class TransientError(Exception):
    """Failure likely to succeed if retried.

    Examples: downstream database timeout, network blip, HTTP 503,
    rate limit exceeded, broker leader election in progress.
    """


class PermanentError(Exception):
    """Failure that will recur identically no matter how many times
    it is retried — a "poison pill" message.

    Examples: schema violation, business rule breach (negative price),
    malformed payload, unknown product code.
    """