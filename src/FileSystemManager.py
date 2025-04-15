import fsspec
from fsspec.implementations.local import LocalFileSystem
import MEETExceptions as me
import psutil
import logging
import os


def loadS3FileSystem():
    """
    Function to load custom filesystem abstracted by the fsspec python module i.e s3, ftp, local, e.t.c
    """
    return fsspec.filesystem(
            protocol='s3',
            key=os.getenv('access_key', 'hrsb5fuR86g91pQPGQ3Z'),
            secret=os.getenv('secret_key', '6o7toFzW4w610MOCOM7S0Bb3RGqrahJxI8GYtzOJ'),
            client_kwargs={
                'endpoint_url': os.getenv('endpoint_url', 'http://localhost:9000'),
                'use_ssl': os.getenv('use_ssl', False)
            }
        )


class FileStorageManager:
    """
    File Storage Manager Class
    """
    
    FILE_SYSTEM_MANAGER_SINGLETON = None
    

    @classmethod
    def getFSManager(cls):
        """
        Get the singleton instance of the file system manager.
        
        Returns:
            BaseFSManager: The singleton instance of the file system manager.
        """
        if cls.FILE_SYSTEM_MANAGER_SINGLETON is None:
            logging.error("Unable to find any filesystem instance")
            raise me.IllegalElementError("Unable to find any valid FileSystemManager instance!")
        return cls.FILE_SYSTEM_MANAGER_SINGLETON
    
    @classmethod
    def _initializeSingleton(cls, config):
        cls.FILE_SYSTEM_MANAGER_SINGLETON = cls(config)
        return cls.FILE_SYSTEM_MANAGER_SINGLETON
    
    
    def __init__(self, config):
        """Initialize the base file system"""
        self.config = config
        if self.config['fsType'] == 's3':
            self.fileSystem = loadS3FileSystem()
        elif self.config['fsType'] == 'local':
            self.fileSystem = LocalFileSystem()



    def open(self, path, *args, **kwargs):
        return self.fileSystem.open(path, *args, **kwargs)