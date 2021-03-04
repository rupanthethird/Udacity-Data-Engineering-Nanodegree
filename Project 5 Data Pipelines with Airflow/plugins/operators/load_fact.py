from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql_query="",
                 append_data='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.sql_query=sql_query
        self.append_data=append_data

    def execute(self, context):
        """
        This function connects to Redshift, and then run the SQL query to either delete rows of tables and inserting data into them against Redshift.
        or it runs the SQL query to insert data into tables against Redshift, depending on the boolean value of 'append_data'.
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_query)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)

            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_query)
            redshift.run(sql_statement)
        
        
        
        
