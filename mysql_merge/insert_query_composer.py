class InsertQueryComposer(object):
    _insert_part = None
    _values_part_template = None
    _values_part = None
    values_count = None

    def __init__(self, table_name, columns):
        columns_clause = ""
        values_clause = ""
        for column in columns:
            columns_clause += "`{column_name}`, ".format(
                column_name=column['Field'])
            val = "{{0[{column_name}]}}, "
            values_clause += val.format(column_name=column['Field'])
        columns_clause = columns_clause.rstrip(", ")
        values_clause = values_clause.rstrip(", ")
        self._insert_part = "INSERT INTO `" + table_name + "` (" + columns_clause + ") VALUES"
        self._values_part_template = "(" + values_clause + "),"
        self._values_part = ""
        self.values_count = 0

    def add_value(self, record):
        self._values_part += self._values_part_template.format(record)
        self.values_count += 1

    def get_query(self):
        if self.values_count == 0:
            raise Exception("No values provided to InsertQueryComposer")
        self._values_part = self._values_part.rstrip(",")
        return "{insert}{values};".format(insert=self._insert_part, values=self._values_part)

    def reset(self):
        self.values_count = 0
        self._values_part = ""