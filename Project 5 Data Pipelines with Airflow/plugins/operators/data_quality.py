import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        """
        This function connects to Redshift, and then checks tables which data was inserted into to make sure they contain values,
        and raising error messages if they have no results or cantain no rows.
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records = redshift_hook.get_records(sql)[0]
    
            error_count = 0
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
            if error_count > 0:
                self.log.info('SQL Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            
            if error_count == 0:
                self.log.info('SQL Tests Passed')