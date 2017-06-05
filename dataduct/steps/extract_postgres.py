"""
ETL step wrapper to extract data from RDS to S3
"""
from ..config import Config
from .etl_step import ETLStep
from ..pipeline import CopyActivity
from ..pipeline import PostgresNode
from ..pipeline import PostgresDatabase
from ..pipeline import PipelineObject
from ..pipeline import ShellCommandActivity
from ..utils.helpers import exactly_one
from dataduct.utils.helpers import get_modified_s3_path
from ..utils.exceptions import ETLInputError
from ..database import SelectStatement

config = Config()
if not hasattr(config, 'postgres'):
    raise ETLInputError('Postgres config not specified in ETL')

POSTGRES_CONFIG = config.postgres


class ExtractPostgresStep(ETLStep):
    """Extract Postgres Step class that helps get data out of postgres
    """

    def __init__(self,
                 table=None,
                 sql=None,
                 host_name=None,
                 output_path=None,
                 intermediate_path=None,
                 splits=4,
                 **kwargs):
        """Constructor for the ExtractPostgresStep class

        Args:
            schema(str): schema from which table should be extracted
            table(path): table name for extract
            sql(str): sql query to be executed
            output_path(str): s3 path where sql output should be saved
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(table, sql):
            raise ETLInputError('Only one of table, sql needed')

        super(ExtractPostgresStep, self).__init__(**kwargs)

        if table:
            sql = 'SELECT * FROM %s;' % table
        elif sql:
            table = SelectStatement(sql).dependencies[0]
        else:
            raise ETLInputError('Provide a sql statement or a table name')

        region = POSTGRES_CONFIG[host_name]['REGION']
        rds_instance_id = POSTGRES_CONFIG[host_name]['RDS_INSTANCE_ID']
        user = POSTGRES_CONFIG[host_name]['USERNAME']
        password = POSTGRES_CONFIG[host_name]['PASSWORD']

        database_node = self.create_pipeline_object(
                    object_class=PostgresDatabase,
                    region=region,
                    rds_instance_id=rds_instance_id,
                    username=user,
                    password=password,
        )

        input_node = self.create_pipeline_object(
            object_class=PostgresNode,
            schedule=self.schedule,
            database=database_node,
            table=table,
            username=user,
            password=password,
            select_query=sql,
            insert_query=None,
            host=rds_instance_id,
        )

#         if self.input and not no_input:
#             input_nodes = [self.input]
#         else:
#             input_nodes = []
# 
#         self._output = base_output_node

        s3_format = self.create_pipeline_object(
            object_class=PipelineObject,
            type='TSV'
        )

        intermediate_node = self.create_s3_data_node(
            self.get_output_s3_path(get_modified_s3_path(intermediate_path)) 
            ,format=s3_format)

        self.create_pipeline_object(
            object_class=CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            worker_group=self.worker_group,
            input_node=input_node,
            output_node=intermediate_node,
            depends_on=self.depends_on,
            max_retries=self.max_retries,
        )

        self._output = self.create_s3_data_node(
            self.get_output_s3_path(get_modified_s3_path(output_path)))

        # This shouldn't be necessary but -
        # AWS uses \\n as null, so we need to remove it
        command = ' '.join(["[[ -z $(find ${INPUT1_STAGING_DIR} -maxdepth 1 ! \
                           -path ${INPUT1_STAGING_DIR} -name '*' -size +0) ]] \
                           && touch ${OUTPUT1_STAGING_DIR}/part-0 ",
                           "|| cat",
                            "${INPUT1_STAGING_DIR}/*",
                            "| sed 's/\\\\\\\\n/NULL/g'",  # replace \\n
                            # get rid of control characters
                            "| tr -d '\\\\000'",
                            # split into `splits` number of equal sized files
                            ("| split -a 4 -d -l $((($(cat ${{INPUT1_STAGING_DIR}}/* | wc -l) + \
                            {splits} - 1) / {splits})) - ${{OUTPUT1_STAGING_DIR}}/part-"
                                .format(splits=splits))])

        self.create_pipeline_object(
            object_class=ShellCommandActivity,
            input_node=intermediate_node,
            output_node=self.output,
            command=command,
            max_retries=self.max_retries,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args = cls.pop_inputs(input_args)
        step_args = cls.base_arguments_processor(etl, input_args)

        return step_args
