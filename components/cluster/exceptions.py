class ServerNotRunning(Exception):
    pass


class ClusterException(Exception):
    pass


class CommandFailed(ClusterException):
    pass


class LockException(ClusterException):
    pass


class IncomingDataError(ClusterException):
    pass


class ResponseError(ClusterException):
    pass


class OfflinePeer(ClusterException):
    pass


class FilesException(ClusterException):
    pass


class FilePutException(FilesException):
    pass


class FileDelException(FilesException):
    pass


class FileGetException(FilesException):
    pass
