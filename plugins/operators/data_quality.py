from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_check=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if data_quality_check is None:
            data_quality_check = []
        self.redshift_conn_id = redshift_conn_id
        self.validate = data_quality_check

    def execute(self, context):
        self.log.info('Getting aws credentials...')
        redshift_conn = PostgresHook.PostgresHook('redshift')
        
        self.log.info('Validating the data...')
        for c in self.validate:
            check_query = c['data_quality_check']
            expected = c['expectation']
            records = redshift_conn.get_records(check_query)
            
            n_records = records[0][0]
            if n_records == expected:
                self.log.info('Pass for SQL {} with {} records'.format(check_query, n_records))
            else:
                raise ValueError("Fail for SQL {}. Expected {} | Actual {}".format(check_query, expected, n_records))
