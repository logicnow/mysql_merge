from mysql_merge.config import ExecutionMode
from mysql_merge.cursor_wrapper import CursorWrapper
from mysql_merge.insert_query_composer import InsertQueryComposer
from mysql_merge.patch_file_helper import PatchFileHelper
from mysql_merge.utils import MiniLogger, create_connection, handle_exception
from mysql_merge.mysql_mapper import Mapper
from mysql_merge.utils import map_fks, lists_diff
import MySQLdb
import warnings


class Merger(object):
    CONST_STEPS = 9
    _conn = None
    _cursor = None

    _source_mapper = None
    _db_map = None
    _destination_db_map = None

    _config = None
    _logger = None
    _counter = 0

    _destination_db = None
    _source_db = None
    _orphaned_rows_update_values = {}
    _execution_mode = ExecutionMode.DEFAULT
    _patch_file = None
    _self_referencing_tables = {}

    def __init__(self, execution_mode, destination_db_map, source_db, destination_db, config, counter, logger):

        self._execution_mode = execution_mode
        self._destination_db_map = destination_db_map

        self._orphaned_rows_update_values = config.orphaned_rows_update_values
        self._source_db = source_db
        self._destination_db = destination_db
        self._config = config
        self._counter = counter
        self._logger = logger

        self._step = 1
        self._total_steps = self.CONST_STEPS
        if self._config.skip_ids_decrement:
            self._total_steps -= 1

        self._conn = create_connection(self._source_db)
        self._cursor = CursorWrapper(self._conn, self._logger, execution_mode == ExecutionMode.DRY_RUN)

        self.prepare_db()

        self._logger.log("Processing database '%s'..." % self._source_db['db'])
        # Indexes may be named differently in each database, therefore we need
        # to remap them

        self._logger.log(" -> 1/2 Re-mapping database")
        self._source_mapper = Mapper(self._conn, source_db['db'], MiniLogger(), verbose=False)
        db_map = self._source_mapper.map_db()

        self._logger.log(" -> 2/2 Re-applying FKs mapping to current database schema - in case execution broke before")
        map_fks(db_map, False)

        self._db_map = db_map

        # Remove from the map tables that are missing in destination db
        # for table in lists_diff(self._destination_db, self.get_overlapping_tables()):
        #  del self._destination_db[table]

    def prepare_db(self):
        self._cursor.execute("set names utf8")
        warnings.filterwarnings('error', category=MySQLdb.Warning)

    def _fk_checks(self, enable):
        self._cursor.execute("set foreign_key_checks=%d" % (enable))

    def __del__(self):
        if self._conn:
            self._conn.close()

    def _log_step(self, message):
        self._logger.log(" -> %d/%d %s" % (self._step, self._total_steps, message))
        self._step += 1

    def merge(self):
        self._conn.begin()

        self._logger.log(" ")
        self._logger.log("Processing database '%s'..." % self._source_db['db'])

        self._fk_checks(False)
        self._step = 1

        self._log_step("Executing preprocess_queries (specified in config)")
        self.execute_preprocess_queries()

        self._log_step("Converting tables to InnoDb")
        self.convert_tables_to_innodb()

        self._log_step("Converting FKs to UPDATE CASCADE")
        self.convert_fks_to_update_cascade()

        self._log_step("Converting mapped FKs to real FKs")
        self.convert_mapped_fks_to_real_fks()

        self._log_step("Nulling orphaned FKs")
        self.null_orphaned_fks()

        self._fk_checks(True)

        self._log_step("Incrementing PKs")
        self.change_pks()

        self._fk_checks(False)

        self._log_step("Copying data to the destination db")
        self.copy_data_to_target()

        self._fk_checks(True)

        if not self._config.skip_ids_decrement:
            self._log_step("Decrementing PKs")
            self.change_pks(-1)

        self._log_step("Committing changes")
        self._conn.commit()
        self._logger.log("----------------------------------------")

    def execute_preprocess_queries(self):
        for q in self._config.preprocess_queries:
            try:
                self._cursor.execute(q)
            except Exception, e:
                handle_exception(
                    "There was an error while executing preprocess_queries\nPlease fix your config and try again",
                    e, self._conn)

    def convert_tables_to_innodb(self):
        # Convert all tables to InnoDB
        for table_name in self._db_map.keys():
            try:
                rows = self._cursor.execute(
                    "select engine from information_schema.TABLES where table_schema = '%s' and table_name = '%s'" % (
                        self._source_db['db'], table_name))
                data = rows.fetchone()
                if data and data['engine'].lower() == 'innodb':
                    continue

                self._cursor.execute("alter table `%s` engine InnoDB" % (table_name))
            # except _mysql_exceptions.OperationalError,e:
            except Exception, e:
                handle_exception(
                    "There was an error while converting table `%s` to InnoDB\nPlease fix your schema and try again" % table_name,
                    e, self._conn)

    def convert_fks_to_update_cascade(self):
        # Convert FK to on update cascade
        for table_name, table_map in self._db_map.items():
            for col_name, fk_data in table_map['fk_host'].items():
                try:
                    self._cursor.execute("alter table `%s` drop foreign key `%s`" % (
                        table_name, fk_data['constraint_name']))
                    if table_name == fk_data['parent']:
                        self._logger.log("---> self referencing table {table} FK: {fk}".format(
                            table=table_name, fk=fk_data['constraint_name']))
                        if table_name not in self._self_referencing_tables:
                            self._self_referencing_tables[table_name] = set()
                        self._self_referencing_tables[table_name].add(col_name)
                        self._logger.log(self._self_referencing_tables)
                    else:
                        self._cursor.execute(
                            "alter table `%s` add foreign key `%s` (`%s`) references `%s` (`%s`) on update cascade" % (
                                table_name, fk_data['constraint_name'], col_name, fk_data['parent'],
                                fk_data['parent_col']))
                except Exception, e:
                    handle_exception("There was an error while converting FK `%s` on `%s`.`%s` to ON UPDATE CASCADE" % (
                        fk_data['constraint_name'], table_name, col_name), e, self._conn)

    def convert_mapped_fks_to_real_fks(self):
        # Convert mapped FKs to real FKs
        for table_name, table_map in self._db_map.items():
            for col_name, fk_data in table_map['fk_create'].items():
                constraint_name = ""
                try:
                    constraint_name = "%s_%s_dbmerge" % (
                        table_name[0:25], col_name[0:25])  # max length of constraint name is 64
                    self._cursor.execute(
                        "alter table `%s` add foreign key `%s` (`%s`) references `%s` (`%s`) on update cascade" % (
                            table_name, constraint_name, col_name, fk_data['parent'], fk_data['parent_col']))

                    self._db_map[table_name]['fk_host'][col_name] = fk_data
                    del self._db_map[table_name]['fk_create'][col_name]
                except Exception, e:
                    handle_exception("There was an error while creating new FK `%s` on `%s`.`%s`" % (
                        constraint_name, table_name, col_name), e, self._conn)

    def null_orphaned_fks(self):
        mapping = self._orphaned_rows_update_values.get('columns', {})

        for table_name, table_map in self._db_map.items():
            for col_name, fk_data in table_map['fk_host'].items():
                params = {
                    'child': table_name,
                    'child_col': col_name,
                    'parent': fk_data['parent'],
                    'parent_col': fk_data['parent_col'],
                    'value': mapping[col_name] if mapping.has_key(col_name) else "null"
                }
                try:
                    try:
                        self._cursor.execute(
                            "UPDATE `%(child)s` c left join `%(parent)s` p on c.`%(child_col)s`=p.`%(parent_col)s` set c.`%(child_col)s`=%(value)s WHERE p.`%(parent_col)s` is null" % params)
                    except (MySQLdb.Warning, MySQLdb.IntegrityError), e:
                        # If nulling failed, let's delete problematic rows
                        self._cursor.execute(
                            "DELETE FROM `%(child)s` WHERE not exists (select * from `%(parent)s` p where p.`%(parent_col)s`=`%(child)s`.`%(child_col)s` limit 1)" % params)
                except Exception, e:
                    handle_exception(
                        "There was an error while nulling orphaned FK on `%s`.`%s`" % (table_name, col_name), e,
                        self._conn)

    def change_pks(self, order=1):
        # Update all numeric PKs to ID + (1 000 000) * order
        for table_name, table_map in self._db_map.items():
            ignored_ids_clause = self._config.ids_to_ignore.get(table_name, [])
            where_clause = "WHERE `Id` not in (%s)" % ", ".join(str(e) for e in ignored_ids_clause) \
                if len(ignored_ids_clause) else ""
            for col_name in table_map['primary'].keys():
                # If current col is also a Foreign Key, we do not touch it
                if table_map['fk_host'].has_key(col_name):
                    continue

                increment_value = self.get_increment_value(table_name) * order
                try:
                    if table_name in self._self_referencing_tables:
                        set_clause_add = ""
                        set_clause = "`{c}` = `{c}` + {step}".format(
                            c=col_name, step=increment_value)
                        self._cursor.execute("UPDATE `{table}` SET {set_clause} {where}".format(
                            table=table_name, set_clause=set_clause, where=where_clause))
                        self._fk_checks(False)
                        for self_col_name in self._self_referencing_tables[table_name]:
                            set_clause_add += "`{c}` = `{c}` + {step}, ".format(
                                c=self_col_name, step=increment_value)
                        self._logger.log("UPDATE `{table}` SET {set_clause_add}".format(
                            table=table_name, set_clause_add=set_clause_add[:-2]))
                        self._cursor.execute("UPDATE `{table}` SET {set_clause_add}".format(
                            table=table_name, set_clause_add=set_clause_add[:-2]))
                        self._fk_checks(True)
                    else:
                        self._logger.log(
                            "UPDATE `%(table)s` SET `%(pk)s` = `%(pk)s` + %(step)d %(where)s" % {"table": table_name,
                                                                                                "pk": col_name,
                                                                                                "step": increment_value,
                                                                                                "where": where_clause})
                        self._cursor.execute(
                            "UPDATE `%(table)s` SET `%(pk)s` = `%(pk)s` + %(step)d %(where)s" % {"table": table_name,
                                                                                                "pk": col_name,
                                                                                                "step": increment_value,
                                                                                                "where": where_clause})
                except Exception, e:
                    if table_name in self._self_referencing_tables:
                        self._fk_checks(True)
                    handle_exception("There was an error while updating PK `%s`.`%s` to %d + pk_value" % (
                        table_name, col_name, increment_value), e, self._conn)

    def map_pks_to_target_on_unique_conflict(self):
        # Update all the PKs in the source db to the value from destination db
        # if there's unique value collidinb with target database
        if self._execution_mode == ExecutionMode.DEFAULT:
            update_cur = CursorWrapper(self._conn, self._logger, self._execution_mode == ExecutionMode.DRY_RUN)
            for table_name, table_map in self._db_map.items():
                pks = table_map['primary'].keys()
                if len(pks) != 1:
                    continue

                pk_col = pks[0]
                for index_name, columns in table_map['indexes'].items():
                    new_pk = old_pk = ""
                    try:
                        # Get all rows that have the same unique value as our destination table
                        rows = self._cursor.execute("SELECT t1.`%(pk_col)s` as old_pk, t2.`%(pk_col)s` as new_pk " \
                                                    "FROM `%(table)s` t1 " \
                                                    "LEFT JOIN `%(destination_db)s`.`%(table)s` t2 ON (%(join)s) " \
                                                    "WHERE t2.`%(pk_col)s` is not null" % {
                                                        'destination_db': self._destination_db['db'],
                                                        "table": table_name,
                                                        "pk_col": pk_col,
                                                        "join": " AND ".join([
                                                            "(t1.`%(column)s` = t2.`%(column)s` AND t2.`%(column)s` is not null)" % {
                                                                'column': column} for column in columns])
                                                    })

                        # Update all those rows PKs - to trigger the CASCADE on all pointers
                        while True:
                            row = rows.fetchone()
                            if not row:
                                break

                            new_pk, old_pk = row['new_pk'], row['old_pk']
                            update_cur.execute(
                                "UPDATE `%(table)s` set `%(pk_col)s`=%(new_pk)s where `%(pk_col)s`=%(old_pk)s" % {
                                    'table': table_name,
                                    'pk_col': pk_col,
                                    'new_pk': row['new_pk'],
                                    'old_pk': row['old_pk'],
                                })
                            self._db_map[table_name]['pk_changed_to_resolve_unique_conficts'].append(str(row['new_pk']))

                    except Exception, e:
                        handle_exception(
                            "There was an error while normalizing unique index `%s`.`%s` from values '%s' to value '%s'" % (
                                table_name, index_name, old_pk, new_pk), e, self._conn)

    def copy_data_to_target(self):
        diff_tables = self._source_mapper.get_non_overlapping_tables(self._destination_db_map)
        for k, v in diff_tables.items():
            if len(v):
                self._logger.log("----> Skipping some missing tables in %s database: %s; " % (k, v))

        patch = self.get_patch_file()
        patch.write_line('SET FOREIGN_KEY_CHECKS = 0;')

        # Copy all the data to destination table
        for table_name, table_map in self._db_map.items():
            if any([table_name in v for k, v in diff_tables.items()]):
                continue
            try:
                ignored_records = self._config.ids_to_ignore.get(table_name, [])
                ignored_records += table_map['pk_changed_to_resolve_unique_conficts']
                where = "" if not len(ignored_records) else "WHERE %(pk_col)s NOT IN (%(ids)s)" % {
                        'pk_col': table_map['primary'].keys()[0],
                        'ids': ",".join(map(str, ignored_records))
                    }

                diff_columns = self._source_mapper.get_non_overlapping_columns(self._destination_db_map, table_name)
                for k, v in diff_columns.items():
                    if len(v):
                        self._logger.log(
                            "----> Skipping some missing columns in %s database in table %s: %s; " % (k, table_name, v))

                columns = self._source_mapper.get_overlapping_columns(self._destination_db_map, table_name)
                select_from_src_query = "SELECT %(columns)s FROM `%(source_db)s`.`%(table)s` %(where)s;" % {
                        'source_db': self._source_db['db'],
                        'table': table_name,
                        'where': where,
                        'columns': "`%s`" % ("`,`".join(columns))
                    }
                if not len(columns):
                    raise Exception("Table %s have no intersecting column in merged database and destination database")

                if self._execution_mode == ExecutionMode.IMPORT_FILE:
                    columns_descriptor = [
                        table_map['columns'][x] for x in columns]
                    query_composer = InsertQueryComposer(table_name, columns_descriptor)
                    rows = self._cursor.execute(select_from_src_query)
                    while True:
                        row = rows.fetchone()
                        if not row:
                            if query_composer.values_count:
                                patch.write_line(query_composer.get_query())
                                query_composer.reset()
                            break
                        for key, value in row.items():
                            row[key] = self._conn.escape(value, self._conn.encoders)
                        query_composer.add_value(row)
                        if query_composer.values_count == self._config.batch_size:
                            patch.write_line(query_composer.get_query())
                            query_composer.reset()
                else:
                    self._cursor.execute(
                        "INSERT INTO `%(destination_db)s`.`%(table)s` (%(columns)s) %(select)s" % {
                            'destination_db': self._destination_db['db'],
                            'source_db': self._source_db['db'],
                            'table': table_name,
                            'select': select_from_src_query,
                            'columns': "`%s`" % ("`,`".join(columns))
                        })
            except Exception, e:
                hint = "--> HINT: Looks like you runned this script twice on the same database\n" if "Duplicate" in "%s" % e else ""
                handle_exception(
                    ("There was an error while moving data between databases. Table: `%s`.\n" + hint) % (table_name), e,
                    self._conn)
        patch.write_line('SET FOREIGN_KEY_CHECKS = 1;')

    def get_increment_value(self, table_name):
        if table_name in self._config.increment_step:
            increment_step = self._config.increment_step[table_name]
        else:
            increment_step = self._config.increment_step['default']
        return self._counter * increment_step

    def get_patch_file(self):
        if not self._patch_file:
            self._patch_file = PatchFileHelper(self._counter)
        return self._patch_file
