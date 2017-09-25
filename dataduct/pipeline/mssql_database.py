"""
Pipeline object class for MsSql Jdbc database
see http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-jdbcdatabase.html
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils.exceptions import ETLConfigError

config = Config()
if not hasattr(config, 'mssql'):
    raise ETLConfigError('MSSQL credentials missing from config')

class MssqlDatabase(PipelineObject):
    """Jdbc resource class
    """

    def __init__(self,
                 id,
                 host=None,
                 port=None,
                 database=None,
                 username=None,
                 jdbc_driver_uri=None,
                 encrypt=None,
                 trust_server_certificate=None,
                 password=None):
        """Constructor for the MSSQL class

        Args:
            id(str): id of the object
            host(str): 
            port(str): 
            database(str): 
            jdbc_driver_uri(str): 
            username(str): username for the database
            password(str): password for the database
        """

        if (None in [ jdbc_driver_uri, username, password]):
            raise ETLConfigError('MSSQL credentials missing from config')

        encrypt_cxn_string = ""
        trust_server = ""

        if encrypt is not None:
            encrypt_cxn_string = "encrypt=" + str(encrypt).lower() + ";"
        if trust_server_certificate is not None:
            trust_server = "trustServerCertificate=" + str(trust_server_certificate).lower() + ";"

        connection_string = "jdbc:sqlserver://" + host + ":" + str(port) + ";database=" + database + ";" + encrypt_cxn_string + trust_server
        jdbc_driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        kwargs = {
            'id': id,
            'type': 'JdbcDatabase',
            'connectionString': connection_string,
            'jdbcDriverClass': jdbc_driver_class,
            'jdbcDriverJarUri': jdbc_driver_uri,
            'username': username,
            '*password': password,
        }
        super(MssqlDatabase, self).__init__(**kwargs)

