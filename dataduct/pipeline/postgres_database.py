"""
Pipeline object class for Rds database
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils.exceptions import ETLConfigError

config = Config()

if not hasattr(config, 'postgres'):
    raise ETLConfigError('Postgres credentials missing from config')

class PostgresDatabase(PipelineObject):
    """Postgres resource class
    """

    def __init__(self,
                 id,
                 region=None,
                 rds_instance_id=None,
                 username=None,
                 password=None):
        """Constructor for the Postgres class

        Args:
            id(str): id of the object
            region(str): code for the region where the database exists
            rds_instance_id(str): identifier of the DB instance
            username(str): username for the database
            password(str): password for the database
        """

        if (None in [ region, rds_instance_id, username, password]):
            raise ETLConfigError('Postgres credentials missing from config')

        kwargs = {
            'id': id,
            'type': 'RdsDatabase',
            'region': region,
            'rdsInstanceId': rds_instance_id,
            'username': username,
            '*password': password,
        }
        super(PostgresDatabase, self).__init__(**kwargs)
