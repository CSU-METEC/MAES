from s3fs import S3FileSystem
import SimDataManager as sdm
import MEETExceptions as me
import os


class BaseFSManager():
    """
    Base class for file system operations using a singleton pattern.
    """
    
    FILE_SYSTEM_MANAGER_SINGLETON = None
    

    @classmethod
    def getFSManager(cls):
        """
        Get the singleton instance of the file system manager.
        
        Returns:
            BaseFSManager: The singleton instance of the file system manager.
        """
        if (sdm.SimDataManager.getSimDataManager() is None
                or sdm.SimDataManager.getSimDataManager().filesystem is None):
            raise me.IllegalElementError("No sim data manager gc table available")
        return sdm.SimDataManager.getSimDataManager().filesystem
    
    
    def __init__(self, config):
        """Initialize the base file system"""
        self.config = config
        self.fileSystem = None
    

class S3FileSystemManager(BaseFSManager):
    """
    S3-specific implementation of the file system operations.
    """
    
    def __init__(self, config):
        """
        Initialize the S3FileSystem with connection parameters.
        
        Args:
            access_key (str): Acces Key
            access_secret (str): Secret Key
            bucket_name (str): S3 bucket name
            host (str): S3 endpoint host (default: "localhost")
            port (str): S3 endpoint port (default: "9000")
            use_ssl (bool): Whether to use SSL (default: False)
        """
        super().__init__(config=config)
        self.fileSystem = S3FileSystem(
            key=os.environ.get("S3_ACCESS_KEY"),
            secret=os.environ.get("S3_SECRET_KEY"),
            client_kwargs={
                'endpoint_url': "http://localhost:9000",
                'use_ssl': False
            }
        )



def instantiateFSManager(config):
    """
    Instantiate the file system manager based on the configuration.
    
    Args:
        config (dict): Configuration dictionary containing file system parameters.
        
    Returns:
        BaseFSManager: An instance of the appropriate file system manager.
    """
    fs_manager = None
    if config['fsType'] == 's3':
        fs_manager = S3FileSystemManager(
                config=config,
                access_key="hrsb5fuR86g91pQPGQ3Z",
                access_secret="6o7toFzW4w610MOCOM7S0Bb3RGqrahJxI8GYtzOJ",
                bucket_name="meet",
                host="localhost",
                port=9000,
                use_ssl=False
            )
        
    elif config['fsType'] == 'local':
        fs_manager = BaseFSManager(config)
    else:
        raise ValueError(f"Unsupported file system type: {config['fsType']}")
    return fs_manager