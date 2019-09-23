from mysql_merge.config import ExecutionMode
import MySQLdb


class CursorWrapper(object):
    _cursor = None
    _logger = None
    _dry_run = False
    _NOOP = 'SELECT 1;'

    def __init__(self, connection, logger, dry_run=False):
        if not connection:
            raise Exception("You must specify connection")
        self._cursor = connection.cursor()
        self._logger = logger
        self._dry_run = dry_run

    def __del__(self):
        if self._cursor:
            self._cursor.close()

    def execute(self, query):
        query_for_execution = None
        if not self._dry_run:
            query_for_execution = query
        else:
            self._logger.log("----> %s" % query)
            if self._is_dml_query(query):
                query_for_execution = self._convert_dml_query_to_dry_run(query)
            else:
                query_for_execution = None
        if query_for_execution:
            self._logger.qs = query_for_execution
            self._cursor.execute(self._logger.qs)
        else:
            self._logger.log("---> skip query")
            self._cursor.execute(self._NOOP)
            self._cursor.fetchone()

        return self._cursor

    def _is_dml_query(self, query):
        query_in_lower_case = query.lower()
        return ('alter' not in query_in_lower_case) and ('insert' in query_in_lower_case or 'select' in query_in_lower_case or 'update' in query_in_lower_case or 'delete' in query_in_lower_case)

    def _convert_dml_query_to_dry_run(self, query):
        return query if query.lower().startswith('select') else "EXPLAIN %s" % query
