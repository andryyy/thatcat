class ServerNotRunning(Exception):
    pass


class PatchException(Exception):
    pass


class DocumentNotUpdated(PatchException):
    pass


class DocumentNotInserted(PatchException):
    pass


class DocumentNotRemoved(PatchException):
    pass


class ClusterException(Exception):
    pass


class ClusterCommandFailed(ClusterException):
    pass


class LockException(ClusterException):
    pass


class IncompleteClusterResponses(ClusterException):
    pass


class MonitoringTaskExists(ClusterException):
    pass


class UnknownPeer(ClusterException):
    pass


class ZombiePeer(ClusterException):
    pass


class FilesException(ClusterException):
    pass


class FilePutException(FilesException):
    pass


class FileDelException(FilesException):
    pass


class FileGetException(FilesException):
    pass
