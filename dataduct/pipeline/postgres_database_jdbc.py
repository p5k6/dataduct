"""
Pipeline object class for Rds database
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils.exceptions import ETLConfigError

config = Config()

if not hasattr(config, 'postgres'):
    raise ETLConfigError('Postgres credentials missing from config')


postgres_jar = config.etl['POSTGRES_JDBC_JAR_LOCATION']
if not postgres_jar:
    raise ETLConfigError('Postgres Jar missing from config')

class PostgresDatabaseJdbc(PipelineObject):
    """Postgres resource class
    """

    def __init__(self,
                 id,
                 region=None,
                 rds_instance_id=None,
                 port=None,
                 database=None,
                 endpoint=None,
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

        if (None in [ database, endpoint, username, password]):
            raise ETLConfigError('Postgres credentials missing from config')
        if !port:
            port = 5432  # set default


# {
#   "id" : "MyJdbcDatabase",
#   "type" : "JdbcDatabase",
#   "connectionString" : "jdbc:redshift://hostname:portnumber/dbname",
#   "com.amazon.redshift.jdbc41.Driver",
#   "s3://redshift-downloads/drivers/RedshiftJDBC41-1.1.6.1006.jar",
#   "username" : "user_name",
#   "*password" : "my_password"
# }
# 
        connection_string = "jdbc:postgres://" + endpoint + ":" + port + "/" + database
        kwargs = {
            'id': id,
            'type': 'JdbcDatabase',
            'jdbcDriverClass': 'org.postgresql.Driver',
            'jdbcDriverJarUri': postgres_jar,
            'username': username,
            '*password': password,
        }
        super(PostgresDatabase, self).__init__(**kwargs)
