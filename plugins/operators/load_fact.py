from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 truncate_table=False,
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.truncate_table=truncate_table
        self.query=query

    def execute(self, context):
        self.log.info('Getting aws credentials...')
        redshift_conn = PostgresHook.PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Deleting if the table existed')
        if self.truncate_table:
            self.log.info('Deleting the table {}'.format(self.table))
            redshift_conn.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Executing query: {}'.format(self.query))
        redshift_conn.run('INSERT INTO {} {}'.format(self.table, self.query))
        self.log.info('The loading has been completed.')
