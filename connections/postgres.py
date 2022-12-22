import asyncpg
from quart import current_app as app
from .utils import parse_and_publish_dashboard_data, publish_dashboard_data

class Postgres:
    def __init__(self):
        self._pool = None
        self._enable_read_replica = None,
        self._read_pool = None
        self.publish_dashboard_events = False

    async def connect(self, database: str, user: str, password: str, host: str, port: int, enable_read_replica = False,read_replica_host:str = None,read_replica_port:int = None,use_pool: bool=True,enable_ssl: bool=False, minsize=10, maxsize=100, keepalives_idle=5, keepalives_interval=4, echo=False,max_inactive_connection_lifetime=90.0, **kwargs):
        """
        Sets connection parameters.
        """
        self._pool = await asyncpg.create_pool(user=user, password=password, database=database, host=host, port=port, max_inactive_connection_lifetime=max_inactive_connection_lifetime, min_size=minsize, max_size=maxsize)
        self.publish_dashboard_events = kwargs.get("publish_dashboard_events", False)
        self._enable_read_replica = enable_read_replica
        if self._enable_read_replica:
            if not read_replica_host or not read_replica_port:
                raise "Invalid read replica connection details"
            self._read_pool = await asyncpg.create_pool(
                user=user,
                password=password,
                database=database,
                host=read_replica_host,
                port=read_replica_port,
                max_inactive_connection_lifetime=max_inactive_connection_lifetime,
                min_size=minsize,
                max_size=maxsize,
            )


    def get_where_string(self, where: dict) -> str:
        """
        Returns a string representing the where condition.
        """
        c = 0
        where_condition = ""
        if where:
            if len(where) == 1:
                if list(where.values())[0] == '':
                    where_condition = list(where.keys())[0]
                else:
                    where_condition = ' and '.join(list(where.keys()))
                    where_condition = where_condition %tuple(where.values())
            else:
                for condition in where:
                    if c == 0:
                        if where[condition] == '':
                            where_condition += condition
                            c+=1
                        else:
                            where_condition += condition %where[condition]
                            c+=1
                    else:
                        if where[condition] == '':
                            where_condition += ' AND ' + condition
                        else:
                            where_condition += ' AND ' + condition %where[condition]
        return where_condition


    async def select(self, table: str, columns: list, where: dict={}, group_by: str=None, having: dict={}, order_by: str=None, offset: int=0, limit: int=0) -> list:
        """
        Creates a select query for selective columns with where keys
        Supports multiple where claus with and or or both
        Args:
            table: A string indicating the name of the table
            columns: List of columns to select
            where_keys: Dictionary of conditions
                Example of where keys: {"name > '%s'": "cip", "amount = %s", 50, "active = %s": True}
                items within dictionary gets 'AND'-ed
            group_by: A string indicating column names to group the results on
            order_by: A string indicating column name to order the results on
            offset: Offset on the results
            limit: The limit on the number of results
        Returns:
            A list of dictionaries with each dictionary representing a row.
        """
        select_base_query = "SELECT {} FROM {}"
        columns = ', '.join(columns)
        c = 0
        if where:
            where_condition = self.get_where_string(where)
            select_base_query = select_base_query + " WHERE ({})".format(where_condition)
        if group_by:
            select_base_query = select_base_query + " GROUP BY {}".format(group_by)
        if having:
            having_condition = self.get_where_string(having)
            select_base_query = select_base_query + " HAVING ({})".format(having_condition)
        if order_by:
            select_base_query = select_base_query + " ORDER BY {}".format(order_by)
        if offset:
            select_base_query = select_base_query + " OFFSET {}".format(offset)
        if limit:
            select_base_query = select_base_query + " LIMIT {}".format(limit)
        query = select_base_query.format(columns, table)
        query = query + ';'
        pool = self._get_read_pool()
        async with pool.acquire() as conn:
            try:
                app.logger.debug(f"select-query:: {query}")
                result = await conn.fetch(query)
                return list(map(dict, result))
            except Exception as error:
                app.logger.error(f"Select Query Error:: {query} => {error}")
                raise error


    async def insert(self, table: str, values: dict, ignore_on_conflict: bool = False, unique_constraint_columns: list = [], update_on_conflict: str = '') -> int:
        """
        Creates an insert statement with only chosen fields.
        Args:
            table: A string indicating the name of the table
            values: A dictionary of column names as keys and column values as value  to be inserted
        Returns:
            A binary integer
        """
        insert_base_string = "INSERT INTO {} ({}) VALUES ({});"
        unique_constraint_columns = '(' + ', '.join(unique_constraint_columns) + ')' if unique_constraint_columns else ''
        if(ignore_on_conflict):
            insert_base_string = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT %s DO NOTHING;" %(unique_constraint_columns)
        elif(update_on_conflict):
            insert_base_string = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT %s %s;" %(unique_constraint_columns, update_on_conflict)
        keys = ', '.join(values.keys())
        value_place_holder = ''
        for i in range(1, len(values)+1):
            value_place_holder += ' $' + str(i) + ','
        query = insert_base_string.format(table, keys, value_place_holder[:-1])
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("insert-query:: %s", query)
                app.logger.debug("insert-values:: %s", values)
                result = await conn.execute(query, *tuple(values.values()))
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await publish_dashboard_data(**{
                    "company_id" : values.get('company_id', None),
                    "module" : table,
                    "data" : values,
                    "operation_type": "insert"
                })
                return int(float(result.split(' ')[-1]))
            except Exception as error:
                app.logger.error(f"Insert Query Error:: {query} => {error} ")
                raise error


    async def update(self, table: str, values: dict, where: dict) -> int:
        """
        Creates an update query with only chosen fields
        Supports only a single field where clause
        Args:
            table: A string indicating the name of the table
            values: A dict of column names as keys and column values as values to be updated
            where_keys: dictionary of conditions
                Example of where keys: {"name > '%s'": "cip", "amount = %s", 50, "active = %s": True}
                Items within dictionary gets 'AND'-ed
        Returns:
            A binary integer
        """
        update_base_string = "UPDATE {} SET ({}) = ({}) WHERE ({});"
        if len(values) == 1:
            update_base_string = "UPDATE {} SET ({}) = ROW({}) WHERE ({});"
        keys = ', '.join(values.keys())
        value_place_holder = ''
        for i in range(1, len(values) + 1):
            value_place_holder += ' $' + str(i) + ','
        where_condition = ' and '.join(list(where.keys()))
        where_condition = where_condition %tuple(where.values())
        query = update_base_string.format(table, keys, value_place_holder[:-1], where_condition)
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("update-query:: %s", query)
                app.logger.debug("update-values:: %s", values)
                result = await conn.execute(query, *tuple(values.values()))
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await publish_dashboard_data(**{
                    "company_id" : values.get('company_id', None),
                    "module" : table,
                    "data" : values,
                    "where": where,
                    "operation_type": "update"
                })
                return int(float(result.split(' ')[-1]))
            except Exception as error:
                app.logger.error(f"Update Query Error:: {query} => {error} ")
                raise error


    async def delete(self, table: str, where: dict) -> int:
        """
        Executes a delete query only with choosen fields
        Args:
            table: A string indicating the name of the table
            where: dictionary of conditions
                Example of where keys: {"name > '%s'": "cip", "amount = %s", 50, "active = %s": True}
                Items within dictionary gets 'AND'-ed
        Returns:
            A binary integer
        """
        delete_base_string = "DELETE FROM {} WHERE ({})"
        where_condition = self.get_where_string(where)
        query = delete_base_string.format(table, where_condition)
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("delete-query:: %s", query)
                result = await conn.execute(query)
                return int(float(result.split(' ')[-1]))
            except Exception as error:
                app.logger.error(f"Delete Query Error :: {query} => {error}")
                raise error


    async def insert_and_update(self, statements: list) -> int:
        """
        Executes multiple insert and update statements in a single call.
        Args:
            statements: A list of lists with each inner list representing a single statement.
                Example: [['update', table_name, dict_of_update_values, dict_of_where_condition], ['insert', table_name, dict_of_insert_values]]
        Returns:
            An integer
        """
        insert_base_string = "INSERT INTO {} ({}) VALUES {};"
        update_base_string = "UPDATE {} SET ({}) = {} WHERE ({});"
        single_update_base_string = "UPDATE {} SET {} = '{}' WHERE ({});"
        queries = []
        for statement in statements:
            if statement[0] == 'insert':
                keys = ', '.join(statement[2].keys())
                query = insert_base_string.format(statement[1], keys, tuple(statement[2].values()))
            elif statement[0] == 'update':
                keys = ', '.join(statement[2].keys())
                where_condition = ' and '.join(list(statement[3].keys()))
                where_condition = where_condition %tuple(statement[3].values())
                if len(tuple(statement[2].values())) == 1:
                    query = single_update_base_string.format(statement[1], keys, tuple(statement[2].values())[0], where_condition)
                else:
                    query = update_base_string.format(statement[1], keys, tuple(statement[2].values()), where_condition)
            queries.append(query)
        final_query = ' '.join(queries)
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("insert-and-update-query:: %s", final_query)
                result = await conn.execute(final_query)
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await parse_and_publish_dashboard_data(final_query)
                return int(float(result.split(' ')[-1]))
            except Exception as error:
                app.logger.error(f"Insert And Update Query Error:: {final_query} => {error}")
                raise error


    async def execute_raw_select_query(self, query) -> list:
        """
        Executes a raw select query.
        Args:
            query: A string indicating the raw SQL statement which will get executed.
        Returns:
            A list of dictionaries with each dictionary represented a row.
        """
        pool = self._get_read_pool()
        async with pool.acquire() as conn:
            try:
                app.logger.debug("raw-select-query:: %s", query)
                result = await conn.fetch(query)
                return list(map(dict, result))
            except Exception as error:
                app.logger.error(f"Raw Select Query Error:: {query} => {error}")
                raise error


    # TODO: parser to publish data
    async def execute_raw_insert_query(self, query) -> int:
        """
        Executes a raw insert statement.
        Args:
            query: A string indicating the raw SQL statement which will get executed.
        Returns:
            An integer
        """
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("raw-insert-query:: %s", query)
                result = await conn.execute(query)
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await parse_and_publish_dashboard_data(query)
                return int(float(result.split(' ')[-1]))
            except Exception as error:
                app.logger.error(f"Raw Insert Query Error  {query} => {error}")
                raise error


    # TODO: parser to publish data
    async def execute_raw_update_query(self, query) -> int:
        """
        Executes a raw update statement.
        Args:
            query: A string indicating the raw SQL statement which will get executed.
        Returns:
            An integer
        """
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("raw-update-query:: %s", query)
                result = await conn.execute(query)
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await parse_and_publish_dashboard_data(query)
                return int(float(result.split(' ')[-1]))
            except Exception as error:
                app.logger.error(f"Raw Update Query Error :: {query} => {error}")
                raise error


    async def execute_raw_transaction_query(self, query) -> int:
        """
        Executes a raw transaction statement.
        Args:
            query: A string indicating the raw SQL statement which will get executed.
        Returns:
            COMMIT on success and error string on failure
        """
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                result = await conn.execute(query)
                if result != 'COMMIT':
                    raise result
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await parse_and_publish_dashboard_data(query)
                return result
            except Exception as error:
                app.logger.error(f"Raw Transaction Query Error ::  {query} => {error}")
                raise error


    async def close(self):
        """
        Closes the connection pool.
        """
        app.logger.debug("Closing connection pool")
        await self._pool.close()

    def _get_read_pool(self):
        return self._read_pool if self._enable_read_replica else self._pool

    async def insert_with_returning(self, table: str, values: dict, returning: str):
        """
        Same as insert but with RETURNING support
        """
        insert_base_string = "INSERT INTO {} ({}) VALUES ({}) RETURNING {};"
        keys = ", ".join(values.keys())
        value_place_holder = ""
        for i in range(1, len(values) + 1):
            value_place_holder += " $" + str(i) + ","
        query = insert_base_string.format(
            table, keys, value_place_holder[:-1], returning
        )

        async with self._pool.acquire() as conn:
            try:
                app.logger.debug("insert-with-returning-query:: %s", query)
                app.logger.debug("values: %s", values)
                result = await conn.fetchval(query, *tuple(values.values()))
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await publish_dashboard_data(**{
                    "company_id" : values.get('company_id', None),
                    "module" : table,
                    "data" : values,
                    "operation_type": "insert"
                })
                return result
            except Exception as error:
                app.logger.error(f"error in insert_with_returning::  {query} => {error}")
                raise error

    async def execute_insert_or_update_query(self, query) -> int:
        """
        Executes a raw insert or update statement.
        Args:
            query: A string indicating the raw SQL statement which will get executed.
        Returns:
            An integer
        """
        async with self._pool.acquire() as conn, conn.transaction():
            try:
                app.logger.debug("insert-or-update-query:: %s", query)
                result = await conn.execute(query)
                app.logger.debug(f"Publish dashboard events :: {self.publish_dashboard_events}")
                if self.publish_dashboard_events:
                    await parse_and_publish_dashboard_data(query)
                return int(float(result.split(" ")[-1]))
            except Exception as error:
                app.logger.error(f"error in execute_insert_or_update_query:: {query} => {error}")
                raise error