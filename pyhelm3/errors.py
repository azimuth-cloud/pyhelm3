class Error(Exception):
    """
    Raised when an error occurs with a Helm command.
    """
    def __init__(self, returncode: int, stdout: bytes, stderr: bytes):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(stderr.decode())


class ConnectionError(Error):
    """
    Raised when there is a problem connecting to the Kubernetes API.
    """


class ChartNotFoundError(Error):
    """
    Raised when a chart is not found.
    """


class FailedToRenderChartError(Error):
    """
    Raised when a chart fails to render.
    """


class ReleaseNotFoundError(Error):
    """
    Raised when a release is not found.
    """


class ResourceAlreadyExistsError(Error):
    """
    Raised when Helm attempts to create a resource that already exists.
    """


class InvalidResourceError(Error):
    """
    Raised when Helm attempts to create or update a resource in a way that is not valid.
    """


class CommandCancelledError(Error):
    """
    Raised when a Helm command is cancelled.
    """
