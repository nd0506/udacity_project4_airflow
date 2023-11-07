from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks import aws_hook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 copy_json_option='',
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region

    def execute(self, context):
        self.log.info('Getting aws credentials...')
        aws_hook = aws_hook.AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_conn = PostgresHook.PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Dropping table {tbl_name} if existed...'.format(tbl_name=self.table))
        redshift_conn.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Copying data from S3 source to our Redshift...')
        key = self.s3_key.format(**context)
        s3_folder_path = 's3://{}/{}'.format(self.s3_bucket, key)
        cmd = StageToRedshiftOperator.command_copying.format(
            tbl_redshift=self.table,
            s3_folder_path=s3_folder_path,
            aws_access_key_id=aws_credentials.access_key,
            aws_secret_key_id=aws_credentials.secret_key,
            s3_region=self.region,
            s3_json_format=self.copy_json_option
        )
        redshift_conn.run(cmd)
        self.log.info('The copying has been completed.')




