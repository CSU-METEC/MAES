import fsspec
from fsspec.implementations.local import LocalFileSystem as lfs
import MEETExceptions as me
import psutil
import logging
import os
import logging
from abc import ABC, abstractmethod


class BaseFileSystem(ABC):
    """
    Base File System Class
    """

    @abstractmethod
    def __init__(self, config):
        """
        Initialize the base file system.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the file system.
        """
        self.config = config


class LocalFileSystem(BaseFileSystem):
    """
    Local File System Class
    """

    def __init__(self, config):
        """
        Initialize the local file system.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the file system.
        """
        super().__init__(config)
        self.fs = lfs()


class S3FileSystem(BaseFileSystem):
    """
    S3 File System Class
    """

    def __init__(self, config):
        """
        Initialize the S3 file system.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the file system.
        """
        super().__init__(config)
        self.fs = fsspec.filesystem(
            protocol='s3',
            key=os.getenv('S3_ACCESS_KEY', ValueError("S3_ACCESS_KEY not set")),
            secret=os.getenv('S3_SECRET_KEY', ValueError("S3_SECRET_KEY not set")),
            client_kwargs={
                'endpoint_url': os.getenv('S3_ENDPOINT_URL', ValueError("S3_ENDPOINT_URL not set")),
                'use_ssl': os.getenv('S3_USE_SSL', ValueError("S3_USE_SSL not set")),
            }
        )
        self.bucket_id = os.getenv('S3_BUCKET_ID', ValueError("S3_BUCKET_ID not set"))

        self.load_input_folder()
        self.load_config_folder()



    def load_input_folder(self, input_folder = "input"):
        """
        Load the input folder from S3.
        
        Args:
            input_folder (str): The path to the input folder in S3.
        """
        logging.info(f"Copying Input folder to local file system")
        self.fs.get(rpath=f"{self.bucket_id}/input", lpath=f".", recursive=True)
        logging.info(f"Input folder copied to local file system")

    
    def load_config_folder(self, config = "config"):
        """
        Load the configuration files from S3.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the file system.
        """
        logging.info(f"Copying Configs Folder to local file system")
        self.fs.get(rpath=f"{self.bucket_id}/config", lpath=f".", recursive=True)
        logging.info(f"Configs folder copied to local file system")



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
    
    
    def __init__(self, config):
        """Initialize the base file system"""
        self.config = config
        if self.config['fsType'] == 's3':
            self.FileSystem = S3FileSystem(config)
        elif self.config['fsType'] == 'local':
            self.FileSystem = LocalFileSystem(config)
        else:
            logging.error(f"Unsupported file system type: {self.config['fsType']}")
            raise me.IllegalElementError(f"Unsupported file system type: {self.config['fsType']}")


    @staticmethod
    def clean_paths(path):
        if psutil.MACOS or psutil.LINUX:
            logging.debug("Converting Path to Linux Compatible")
            return str(path).replace("\\", "/")
        elif psutil.WINDOWS:
            logging.debug("Converting Path to Windows Compatible")
            return str(path).replace("/", "\\")
        else:
            return path