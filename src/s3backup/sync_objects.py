import abc
import typing


class SyncObject(abc.ABC):
    """
    Generic object supporting sync operation.
    """
    _sync_objects: typing.List

    def __init__(self):
        self._sync_objects = []

    def sync(self):
        """
        Abstract method. The result of this call should be a synchronisation operation, whose kind will be dependent
        on the class implementation.
        """
        pass


class SyncFile(SyncObject):
    pass


class SyncDir(SyncObject):
    pass
