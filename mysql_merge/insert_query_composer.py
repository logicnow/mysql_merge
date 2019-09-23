def compose_insert_query_template(table_name, columns):
    columns_clause = ""
    values_clause = ""
    for column in columns:
        columns_clause += "`{column_name}`, ".format(
            column_name=column['Field'])
        val = "{{0[{column_name}]}}, " if column['is_int'] else "\'{{0[{column_name}]}}\', "
        values_clause += val.format(column_name=column['Field'])
    columns_clause = columns_clause.rstrip(", ")
    values_clause = values_clause.rstrip(", ")
    return "INSERT IGNORE INTO `" + table_name + "` (" + columns_clause + ") VALUES(" + values_clause + ");"
