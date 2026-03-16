import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
import time

# Normal PostgresClient
class PostgresClient:

    def __init__(self, 
                 user, 
                 password, 
                 host, 
                 port, 
                 db_name,
                 table_name = None,
                 autocommit = True,
                 initialize_on_construction = True):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.table_name = table_name
        self.autocommit=autocommit
        self.conn = None

        if initialize_on_construction:

            # Automatically connect on initialization
            self.connect(self.db_name, autocommit=self.autocommit)

            # If table is provided, try to set it
            if table_name:
                self.set_table(table_name)

    # ---------- Connection Management ----------
    def connect(self, db_name, autocommit=True):
        """
        Establish a connection.
        """

        # Always start with a fresh connection
        if self.conn:
            self.close()

        self.db_name = db_name or self.db_name

        self.conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            sslmode="require"
        )
        self.conn.autocommit = autocommit

    def switch_db(self, new_db, autocommit=True):
        """
        Switch to another database.
        """
        self.connect(new_db, autocommit=autocommit)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # ---------- Connection Health ----------
    def _connection_is_alive(self):
        """Check if the current connection is alive."""
        if not self.conn:
            return False
        try:
            self.conn.poll()
            return True
        except Exception:
            return False

    def _reconnect(self, retries=5, delay=30):
        """Attempt to reconnect to the database."""
        for attempt in range(1, retries + 1):
            try:
                print(f"Reconnection attempt {attempt}/{retries}...")
                self.connect(self.db_name, autocommit=self.autocommit)
                print("Reconnected successfully.")
                return True
            except Exception as e:
                print(f"Reconnect attempt {attempt} failed: {e}")
                if attempt < retries:
                    time.sleep(delay)
        print(f"All {retries} reconnection attempts failed.")
        return False

    # ---------- Context Manager (Connection Scope) ----------
    def __enter__(self):
        self.connect(self.db_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ---------- Transaction Management ----------
    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    @contextmanager
    def transaction(self):
        """
        Context-managed transaction.
        """
        if not self.conn:
            raise RuntimeError("No database selected.")

        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False

        try:
            yield
            self.commit()
        except Exception:
            self.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    # ---------- Inspect Tables ----------
    def get_tables_in_db(self, print_results = False):
        """
        Get all table names in the 'public' schema of the currently connected database.

        Parameters:
        ----------
        print_results : If True print all the tables found (default False)

        Returns:
            set: A set of table names (strings) present in 'public'.
                Returns an empty set if no database is selected or an error occurs.
        """
        if not self.conn:
            print("No database selected. Connect to a database first.")
            return set()

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE';
                    """
                )

                tables = {row[0] for row in cursor.fetchall()}
                db_name = self.conn.get_dsn_parameters().get('dbname', 'unknown')

                if print_results:
                    print(f"Tables in database '{db_name}':")
                    for t in sorted(tables):
                        print(f" - {t}")

                return tables


            except Exception as e:
                print("Error fetching tables:", e)
                return set()

    # ---------- Table Assignment ----------
    def set_table(self, table_name):
        """
        Set the default table for operations, only if it exists in the connected database.
        """
        if not self.conn:
            print("No database connected. Cannot set table.")
            return None

        tables_in_db = self.get_tables_in_db()
        if table_name in tables_in_db:
            self.table_name = table_name
            print(f"Default table set to '{table_name}'.")
        else:
            print(f"Warning: Table '{table_name}' does not exist in database '{self.db_name}'. Default table not changed.")

    # ---------- Permission Checking ----------
    def get_table_permissions(self, table = None, verbose = False):
        """
        Return table permissions for the current user.
        """
        if not self.conn:
            raise RuntimeError("No database selected.")
        
        # Default to self.table_name if table is None 
        table = table or self.table_name

        if not table or not isinstance(table, str):  # Ensure table is a string
            raise RuntimeError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT privilege_type
                FROM information_schema.role_table_grants
                WHERE table_schema = 'public'
                  AND table_name = %s
                  AND grantee = current_user;
            """, (table,))

            privileges = {row[0] for row in cursor.fetchall()}

        if verbose:
            print(
                f"Permissions for user '{self.user}' "
                f"on table '{self.table_name}': {privileges or 'NONE'}"
            )

        return privileges
    
    def send_notification(self, 
                          channel = "ml_tasks", 
                          message="Check - ml_jobs_table"):
        
        """Send a notification to the database."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"NOTIFY {channel}, %s;", (message,))
            print("\n".join(["Notification sent!", 
                             f"channel: {channel}", 
                             f"message: {message}"]))

    def has_table_permission(self, table, privilege):
        """
        Check if current user has a specific privilege on a table.

        privilege examples:
        - SELECT
        - INSERT
        - UPDATE
        - DELETE
        """
        user_privileges = self.get_table_permissions(table)
        return privilege.upper() in user_privileges

    # ---------- Permission Enforcement ----------
    def require_table_permission(self, table, permission):
        """
        Ensure the current user has a specific permission on a table.
        Raises PermissionError if missing.
        """
        if not self.has_table_permission(table, permission):
            user = self.conn.get_dsn_parameters().get("user", "unknown")
            raise PermissionError(
                f"User '{user}' does NOT have {permission} permission on table '{table}'."
            )

    # ---------- Inspect Columns ----------
    def return_table_columns(self, table = None, show_id = False):
        """
        Return user-defined column names for a table in the public schema.
        Ignores columns with defaults or auto-increment identity (e.g., SERIAL, timestamps).

        Parameters:
        ----------
        table : Optional Table name, default is self.table_name
        show_id : return column defaults

        Returns: 
        -------
        Set : Column names that require user-supplied values

        Raises:
        -------
        RuntimeError : If the query fails or connection is missing.
        """

        if not self.conn:
            raise RuntimeError("No database selected.")
        
        # Default to self.table_name if table is absent
        table = table or self.table_name

        # Ensure table is a valid string
        if not table or not isinstance(table, str) or table.strip() == "":
            raise ValueError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        try:
            with self.conn.cursor() as cursor:

                base_query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND table_schema = 'public'
                """

                # Hide default / identity columns unless explicitly requested
                if not show_id:
                    base_query += """
                        AND column_default IS NULL
                        AND is_identity = 'NO'
                    """

                base_query += " ORDER BY ordinal_position;"

                cursor.execute(base_query, (table,))
                columns = [row[0] for row in cursor.fetchall()]

                return columns

        except Exception as e:
            raise RuntimeError(f"Error fetching columns for '{table}': {e}")   
             
  # ---------- Input Validation of Data to be Written ----------
    def check_input_format_against_table_schema(self, input_values_dict, table = None):
        """
        Validate that the keys in input_values_dict correspond to actual columns in the table.

        Parameters:
        ----------
        input_values_dict : Dictionary of values to insert
        table : Table name (If None defaults to self.table_name)

        Returns:
        -------
        True if all keys match table columns

        Raises:
        ------
        ValueError : If extra or missing columns are detected
        RuntimeError : If query fails
        """

        if not self.conn:
            raise RuntimeError("No database selected.")
        
        # Default to self.table_name 
        table = table or self.table_name

        # Ensure table is a valid string
        if not table or not isinstance(table, str) or table.strip() == "":
            raise ValueError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        try:
            db_columns = set(self.return_table_columns(table))
            input_columns = set(input_values_dict.keys())

            # Extra keys not in table
            extra_columns = input_columns - db_columns
            if extra_columns:
                raise ValueError(
                    f"These columns in input dictionary NOT in table '{table}': {extra_columns}"
                )

            # Missing keys
            missing_columns = db_columns - input_columns
            if missing_columns:
                raise ValueError(
                    f"These columns in table are MISSING from input dictionary: {missing_columns}"
                )

            return True

        except Exception as e:
            raise RuntimeError(f"Error validating dictionary keys against table '{table}': {e}")

    # ---------- Input Execution ----------
    def insert_values_into_table(self, 
                                data_as_dicts, 
                                table = None, 
                                return_last_rows=False, 
                                last_n=5,
                                custom_message = None):
        """
        Insert one or more dictionaries of values into PostgreSQL set table safely.

        Parameters:
        ----------
        data_as_dicts : Single dict or list of dicts to insert.
        table : Name of table being inserted into within DB
        return_last_rows : Return last `last_n` rows after insert. (Bool, optional)
        last_n : Number of rows to return. (int, optional)
        custom_message : Message for display after insertion

        Returns:
        -------
        list of tuples : Last `last_n` rows if requested, else empty list.
        """

        if not self.conn:
            raise RuntimeError("No database selected.")
        
        # Default to self.table_name if table is None
        table = table or self.table_name

        # Ensure table is a valid string
        if not table or not isinstance(table, str) or table.strip() == "":
            raise ValueError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        # Permission check
        self.require_table_permission(table, "INSERT")

        # Normalize input to list of dicts
        if isinstance(data_as_dicts, dict):
            data_as_dicts = [data_as_dicts]
        elif not isinstance(data_as_dicts, list) or not all(isinstance(d, dict) for d in data_as_dicts):
            raise ValueError("Input must be a dict or list of dicts.")

        if not data_as_dicts:
            raise ValueError("No data provided for insertion.")

        try:
            with self.transaction():
                with self.conn.cursor() as cursor:

                    # Get user-supplied columns
                    column_names = self.return_table_columns(table)

                    # Validate schema and consistent keys
                    expected_keys = set(column_names)
                    for row in data_as_dicts:
                        self.check_input_format_against_table_schema(row, table)
                        if set(row.keys()) != expected_keys:
                            raise ValueError("All dictionaries must have identical keys.")

                    # Build VALUES list
                    values = [
                        [row[col] for col in column_names]
                        for row in data_as_dicts
                    ]

                    # Build INSERT query
                    insert_query = sql.SQL("""
                        INSERT INTO {table} ({cols})
                        VALUES ({placeholders})
                    """).format(
                        table=sql.Identifier(table),
                        cols=sql.SQL(', ').join(map(sql.Identifier, column_names)),
                        placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in column_names)
                    )

                    cursor.executemany(insert_query, values)

                    if custom_message:
                        print(custom_message)
                    else:
                        print("Data insertion successful!")

                    # Optionally fetch last rows if requested
                    if return_last_rows:
                        last_rows = self.fetch_n_rows_from_table(table, n = last_n)
                        for row in last_rows:
                            print(", ".join([f"{col}: {val}" for col, val in zip(self.return_table_columns(table), row)]))

                        return last_rows

        except Exception as e:
            raise RuntimeError(f"Error inserting values into '{table}': {e}")

    # ---------- Update Table Values -----------

    def update_row_values(
        self,
        table,
        set_values,
        where_conditions,
        return_updated_rows=False
    ):
        """
        Update row values in PostgreSQL table safely.

        Parameters:
        ----------
        table : table to be updated (default to self.table_name)
        set_values : Column-value pairs to update. (as a dict)
        where_conditions : Column-value pairs for WHERE clause. (as a dict)
        return_updated_rows : If True, Return updated rows.

        Returns:
        -------
        list of tuples : If True, return updated rows, else empty list.

        Example:
        -------

        self.update_row_values(
            set_values={"status": "Processed"},
            where_conditions={"file_path": "image-blob | Batch-1-0000010-Person.zip"},
            return_updated_rows = True
        )

        """

        # Assign self.table to table 
        table = table or self.table_name

        # Ensure table is a valid string
        if not table or not isinstance(table, str) or table.strip() == "":
            raise ValueError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        if not self.conn:
            raise RuntimeError("No database selected.")
        
        # Permission check
        self.require_table_permission(table, "UPDATE")

        if not set_values or not isinstance(set_values, dict):
            raise ValueError("set_values must be a non-empty dict.")

        if not where_conditions or not isinstance(where_conditions, dict):
            raise ValueError("where_conditions must be a non-empty dict.")

        try:
            with self.transaction():
                with self.conn.cursor() as cursor:

                    # Validate columns
                    valid_columns = set(self.return_table_columns(table))

                    if not set(set_values.keys()).issubset(valid_columns):
                        raise ValueError("Invalid columns in set_values.")

                    if not set(where_conditions.keys()).issubset(valid_columns):
                        raise ValueError("Invalid columns in where_conditions.")

                    # Build SET clause
                    set_clause = sql.SQL(", ").join(
                        sql.SQL("{} = {}").format(
                            sql.Identifier(col),
                            sql.Placeholder()
                        )
                        for col in set_values
                    )

                    # Build WHERE clause
                    where_clause = sql.SQL(" AND ").join(
                        sql.SQL("{} = {}").format(
                            sql.Identifier(col),
                            sql.Placeholder()
                        )
                        for col in where_conditions
                    )

                    # Combine into a single query
                    query = sql.SQL("""
                        UPDATE {table}
                        SET {set_clause}
                        WHERE {where_clause}
                    """).format(
                        table=sql.Identifier(table),
                        set_clause=set_clause,
                        where_clause=where_clause
                    )

                    # the values that will be updated subject to where clause
                    values = list(set_values.values()) + list(where_conditions.values())

                    if return_updated_rows:
                        query += sql.SQL(" RETURNING *")

                    cursor.execute(query, values)
                    rows = cursor.fetchall() if return_updated_rows else []

            return rows

        except Exception as e:
            raise RuntimeError(f"Error updating rows in '{table}': {e}")

    # ---------- Examine Table Values ----------
    def fetch_n_rows_from_table(self, 
                                table = None,
                                where_conditions = None, 
                                n=5,
                                return_results_formatted = False,
                                print_results = False,
                                order_by=None, 
                                descending=True,
                                show_id=False):
        
        """
        Fetch the last `n` rows from table safely, returning only selected columns. 

        Parameters:
        ----------
        table : If None then default for table isself.table_name)
        where_conditions : WHERE clause aka filter condition. (optional, as a dict, key is col, value is = condition) 
        n : Number of rows to return (optional n = 5)
        order_by : column(s) to sort return values by (default is None)
        descending : If True, return by descending values (default is True)
        show_id : If True, return id and created_date columns (default is false)

        Returns:
        -------
        list of tuples : If True, return updated rows, else empty list.

        """

        table = table or self.table_name

        # Ensure table is a valid string
        if not table or not isinstance(table, str) or table.strip() == "":
            raise ValueError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        if not self.conn:
            raise RuntimeError("No database selected.")
        
        if order_by:
            order_by_clause = sql.SQL("ORDER BY {col} {direction}").format(
                col=sql.Identifier(order_by),
                direction=sql.SQL("DESC") if descending else sql.SQL("ASC")
            )
        else:
            order_by_clause = sql.SQL("ORDER BY id DESC")

        try:
            with self.conn.cursor() as cursor:

                values = []
                where_clause = sql.SQL("")

                # Build WHERE clause
                if where_conditions:
                    where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(
                        sql.SQL("{} = {}").format(
                            sql.Identifier(col),
                            sql.Placeholder()
                        )
                        for col in where_conditions
                    )
                    values.extend(where_conditions.values())

                query = sql.SQL("""
                    SELECT *
                    FROM {table}
                    {where_clause}
                    {order_by_clause}
                    LIMIT %s
                """).format(
                    table=sql.Identifier(table),
                    where_clause=where_clause,
                    order_by_clause=order_by_clause
                )

                values.append(n)
                cursor.execute(query, values)

                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]

                # Optionally remove default columns like id / created_at
                if not show_id:
                    filtered_indices = [
                        i for i, col in enumerate(column_names)
                        if col not in {"id", "created_at"}
                    ]

                    column_names = [column_names[i] for i in filtered_indices]
                    rows = [[row[i] for i in filtered_indices] for row in rows]

                if print_results:
                    print(" | ".join(column_names))
                    print("-" * (len(column_names) * 15))
                    for row in rows:
                        print(" | ".join(str(v) for v in row))

                if return_results_formatted:
                    new_rows = []
                    for row in rows:
                        new_rows += [{k: v for k, v in zip(column_names, list(row))}]
                    rows = new_rows

                return rows

        except Exception as e:
            raise RuntimeError(f"Error fetching rows from table '{table}': {e}")
        
    # ---------- Quick check of User Access ----------
    def test_user_access_for_assigned_table(self, table = None):
        """
        Test if the current connection user can read from table (default to self.table_name).

        Returns
        -------
        bool
            True if the current user can read from the table, False otherwise.
        """
        table = table or self.table_name

        # Ensure table is a valid string
        if not table or not isinstance(table, str) or table.strip() == "":
            raise ValueError(f"A valid table name must be provided.\n This value is not applicable: {table}")

        if not self.conn:
            print("No database selected. Connect to a database first.")
            return False

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT * FROM {} ORDER BY ID DESC LIMIT 5").format(
                        sql.Identifier(table)
                    )
                )
                row = cursor.fetchone()
                user = self.conn.get_dsn_parameters().get("user", "unknown")
                db_name = self.conn.get_dsn_parameters().get("dbname", "unknown")
                print(f"User '{user}' can read from table '{table}' in database '{db_name}'. Sample row:", row)
                return True

        except Exception as e:
            user = self.conn.get_dsn_parameters().get("user", "unknown")
            print(f"User '{user}' cannot access table '{table}': {e}")
            return False