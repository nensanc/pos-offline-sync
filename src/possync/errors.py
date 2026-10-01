"""Error types shared by the node and the transport."""


class NetworkError(Exception):
    """The server could not be reached, or its reply was lost.

    The outcome on the server is unknown, so the operation must be retried later.
    Network errors never count toward an outbox item's attempts: they say nothing
    about whether the item itself is valid.
    """
