import psycopg2
import os
import subprocess
from psycopg2 import sql
from collections import OrderedDict
from contextlib import contextmanager

class PostgresAdmin:
    def __init__(self, user, password, host, port, db_name="postgres"):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name

        self.conn = None

    # ---------- Connection Management ----------

    def connect(self, db_name=None, autocommit=True):
        """
        Connect to a PostgreSQL database.
        """
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
        Switch connection to another database.
        """
        self.connect(new_db, autocommit=autocommit)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # ---------- Context Manager ----------

    def __enter__(self):
        self.connect(self.db_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ---------- Transaction Context Manager ----------

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    @contextmanager
    def transaction(self):

        # Check to see that we do have a connection
        if not self.conn:
            raise RuntimeError("No active connection")

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

    # ---------- DB Creation ----------

    def list_databases(self):
        """
        List all non-template databases.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false;"
            )
            databases = [row[0] for row in cursor.fetchall()]

        print("Existing databases:")
        for db in databases:
            print(f" - {db}")

        return databases

    def create_database_if_it_does_not_exists(self, db_name):
        """
        Create database if it does not exist.
        Must be connected to 'postgres'.
        """
        if not self.conn:
            raise RuntimeError("No active connection")

        # Save old autocommit
        old_autocommit = self.conn.autocommit
        try:
            # Enable autocommit for CREATE DATABASE
            self.conn.autocommit = True

            with self.conn.cursor() as cursor:
                # Check if database exists
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s;",
                    (db_name,)
                )

                if cursor.fetchone() is None:
                    print(f"Creating database '{db_name}'...")
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(db_name)
                        )
                    )
                    return True

                print(f"Database '{db_name}' already exists.")
                return False
        finally:
            # Restore original autocommit state
            self.conn.autocommit = old_autocommit

    def copy_database(self, source_db, target_db):
        """
        Copy a database using PostgreSQL TEMPLATE.
        Works on Azure PostgreSQL.
        """
        if not self.conn:
            raise RuntimeError("No active connection")

        # Must not be inside a transaction
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = True

        try:
            existing_dbs = self.list_databases()
            if target_db in existing_dbs:
                raise RuntimeError(f"Target database '{target_db}' already exists.")

            with self.conn.cursor() as cursor:
                # Terminate connections to source DB (Azure-safe)
                cursor.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s
                    AND pid <> pg_backend_pid();
                """, (source_db,))

                print(f"Copying database '{source_db}' -> '{target_db}'")

                cursor.execute(
                    sql.SQL("CREATE DATABASE {} WITH TEMPLATE {};").format(
                        sql.Identifier(target_db),
                        sql.Identifier(source_db),
                    )
                )

            print(f"Database '{source_db}' successfully copied to '{target_db}'")

        finally:
            self.conn.autocommit = old_autocommit

    def drop_database(self, db_to_drop, force=False, save_prod = True):
        """
        Drop a DB from postgres. Must be connected to the 'postgres' database.

        Parameters:
        ----------
        db_name : Name of the DB 
        force : If True, terminates active connections
        save_prod : If True, stops a deletion of any db starting with prod
        """

        # This line exists mostly to prevent weird issues where conn is None
        if not self.conn:
            raise RuntimeError("No active connection")

        # If connected to the DB we're dropping, disconnect and reconnect to postgres
        if db_to_drop == self.db_name:
            print(f"Disconnecting from '{db_to_drop}'...")
            self.close()
            self.connect("postgres")

        if db_to_drop.lower().startswith("prod") and save_prod == True:
            raise RuntimeError("Refusing to delete production database")

        old_autocommit = self.conn.autocommit
        self.conn.autocommit = True

        try:
            with self.conn.cursor() as cursor:
                # Check if DB exists
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s;",
                    (db_to_drop,)
                )
                if cursor.fetchone() is None:
                    print(f"Database '{db_to_drop}' does not exist.")
                    return False

                if force:
                    print(f"Terminating active connections to '{db_to_drop}'...")
                    cursor.execute("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = %s
                        AND pid <> pg_backend_pid();
                    """, (db_to_drop,))

                print(f"Dropping database '{db_to_drop}'...")
                cursor.execute(
                    sql.SQL("DROP DATABASE {};").format(
                        sql.Identifier(db_to_drop)
                    )
                )

            print(f"Database '{db_to_drop}' deleted successfully.")
            return True

        finally:
            self.conn.autocommit = old_autocommit

    # ---------- Table Inspection ---------- 
    def list_tables_in_db(self, db_name = None, print_results = False):
        """
        Get all table names in the 'public' schema of the currently connected database.

        Parameters:
        ----------
        db_name = If None, it'll be set to the db that's connected
        print_results : If True print all the tables found (default False)

        Returns:
            set: A set of table names (strings) present in 'public'.
                Returns an empty set if no database is selected or an error occurs.
        """
        if not self.conn:
            print("No database selected. Connect to a database first.")
            return set()
        
        if db_name is None:
            db_name = self.db_name

        # Switch to new_db but retain old one to return to it
        original_db = self.db_name

        with self.conn.cursor() as cursor:
            try:

                if db_name and db_name != self.db_name:
                    self.switch_db(db_name)
            
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

            # Return to the original DB            
            finally:
                if db_name and self.db_name != original_db:
                    self.switch_db(original_db)
            
    # ---------- Table Creation ---------- 

    def create_table_if_it_does_not_exists(
            self,
            table_name,
            table_definition,
            db_name=None,
            list_tables=False
        ):
        """
        Create a table in the public schema if it does not exist.
        Optionally switch to db_name temporarily.

        Automatically ensures:
        - id BIGSERIAL PRIMARY KEY
        - created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP

        table_definition: {column_name: sql_definition} dictionary
        """
        # ---- Enforce required columns (non-destructive) ----
        enforced_columns = OrderedDict()

        if "id" not in table_definition:
            enforced_columns["id"] = "BIGSERIAL PRIMARY KEY"

        if "created_at" not in table_definition:
            enforced_columns["created_at"] = "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"

        # Merge enforced + user-defined columns
        full_definition = OrderedDict(
            list(enforced_columns.items()) +
            list(table_definition.items())
        )

        original_db = self.db_name

        try:
            # Switch to target database if provided
            if db_name and db_name != self.db_name:
                self.switch_db(db_name)

            # ---- Use transaction context manager ----
            with self.transaction():
                with self.conn.cursor() as cursor:
                    # Check if table exists
                    cursor.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = %s;
                        """,
                        (table_name,)
                    )

                    if cursor.fetchone():
                        print(f"Table '{table_name}' already exists in database '{self.db_name}'.")
                        return

                    print(f"Creating table '{table_name}' in database '{self.db_name}'...")

                    columns_sql = sql.SQL(", ").join(
                        sql.SQL("{} {}").format(
                            sql.Identifier(col),
                            sql.SQL(definition)
                        )
                        for col, definition in full_definition.items()
                    )

                    query = sql.SQL("CREATE TABLE {} ({})").format(
                        sql.Identifier(table_name),
                        columns_sql
                    )

                    cursor.execute(query)
                    print(f"Table '{table_name}' created successfully in database '{self.db_name}'.")

                    if list_tables:
                        cursor.execute(
                            """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_type = 'BASE TABLE'
                            ORDER BY table_name;
                            """
                        )
                        tables = cursor.fetchall()
                        print(f"Tables in public schema of database '{self.db_name}':")
                        for t in tables:
                            print(f" - {t[0]}")

        finally:
            # Restore original database if switched
            if db_name and self.db_name != original_db:
                self.switch_db(original_db)

    # ---------- Table Modification ---------- 

    # Rename the table 
    def rename_table(self, old_name, new_name):
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}")
                .format(
                    sql.Identifier(old_name),
                    sql.Identifier(new_name)
                )
            )

    # -----------------------------
    # Add index for ML jobs table
    # -----------------------------
    def add_ml_jobs_index(self, table_name="ml_jobs"):
        """
        Adds an index on (status, created_at) for fast worker queries.
        Idempotent — won't fail if index already exists.
        """
        with self.transaction():
            with self.conn.cursor() as cur:
                index_name = f"idx_{table_name}_pending"
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {table_name} (status, created_at);
                """)
        self.conn.commit()
        print(f"Index '{index_name}' ensured on '{table_name}'.")

    # -----------------------------
    # Add LISTEN / NOTIFY trigger
    # -----------------------------
    def add_ml_jobs_notify_trigger(self, table_name="ml_jobs"):
        """
        Creates a trigger that NOTIFYs workers on new jobs.
        Idempotent — won't recreate trigger if it exists.
        """
        with self.transaction():
            with self.conn.cursor() as cur:
                # Create trigger function
                cur.execute(f"""
                    CREATE OR REPLACE FUNCTION notify_new_job()
                    RETURNS trigger AS $$
                    BEGIN
                        PERFORM pg_notify('new_job', NEW.id::text);
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                # Attach trigger to table only if it doesn't exist
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_trigger WHERE tgname = 'new_job_trigger'
                        ) THEN
                            CREATE TRIGGER new_job_trigger
                            AFTER INSERT ON {table_name}
                            FOR EACH ROW
                            EXECUTE FUNCTION notify_new_job();
                        END IF;
                    END
                    $$;
                """)
        self.conn.commit()
        print(f"Trigger 'new_job_trigger' ensured on '{table_name}'.")

    # ---------- Table Column Modification ---------- 

    def column_exists(self, table, column):
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name = %s;
            """, (table, column))
            return cursor.fetchone() is not None

    def add_column(
        self,
        table_name,
        column_name,
        column_definition = "VARCHAR(100)",
        default_sql=None
    ):
        
        """
        Add a column to a table if it does not exist.

        Parameters:
        - table_name: str, table name
        - column_name: str, column name
        - column_definition: str, type + constraints, e.g. "INT NOT NULL"
        - default_sql: str or None, raw SQL for default value, e.g. "0", "'unknown'", "CURRENT_TIMESTAMP"
        """

        # Check if the column already exists:
        if self.column_exists(table_name, column_name):
            print(f"Columns {column_name} already exists in {table_name}...")
            return None 
        
        # If a default is provided, append to the column definition
        full_definition = column_definition

        if default_sql is not None:
            full_definition += f" DEFAULT {default_sql}"

        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    ALTER TABLE {}
                    ADD COLUMN IF NOT EXISTS {} {}
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(column_name),
                    sql.SQL(full_definition)
                )
            )

        print(f"Column '{column_name}' ensured on table '{table_name}' with default '{default_sql}'.")

    def drop_column(
        self,
        table_name,
        column_name,
        cascade=False
    ):
        
        # Check if the column does not exist:
        if not self.column_exists(table_name, column_name):
            print(f"Column {column_name} does NOT exist in {table_name}...")
            return None 
        
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("ALTER TABLE {} DROP COLUMN {} {}").format(
                    sql.Identifier(table_name),
                    sql.Identifier(column_name),
                    sql.SQL("CASCADE" if cascade else "RESTRICT")
                )
            )

        print(f"Column '{column_name}' dropped from '{table_name}'.")

    def alter_column_type(
        self,
        table_name,
        column_name,
        new_type,
        default_sql=None
    ):
        """
        Change the data type of a column and optionally set a default value.

        Parameters:
        - table_name: str, table name
        - column_name: str, column name
        - new_type: str, new SQL type, e.g. "INT", "VARCHAR(100)", "TIMESTAMPTZ"
        - default_sql: str or None, raw SQL default, e.g. "0", "'unknown'", "CURRENT_TIMESTAMP"
        """        

        # Check if the column does not exist:
        if not self.column_exists(table_name, column_name):
            print(f"Column {column_name} does NOT exist in {table_name}...")
            return None 

        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    ALTER TABLE {}
                    ALTER COLUMN {} TYPE {}
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(column_name),
                    sql.SQL(new_type)
                )
            )

            # Step 2: Optionally set default
            if default_sql is not None:
                cursor.execute(
                    sql.SQL("""
                        ALTER TABLE {}
                        ALTER COLUMN {} SET DEFAULT {}
                    """).format(
                        sql.Identifier(table_name),
                        sql.Identifier(column_name),
                        sql.SQL(default_sql)
                    )
                )

                print(
                    f"Column '{column_name}' in '{table_name}' changed to type {new_type} "
                    f"with default {default_sql}."
                )

            else:
                print(
                    f"Column '{column_name}' in '{table_name}' changed to type {new_type}."
                )

    # Create indices (usually for things that auto increment)
    def create_index(self, table, column, unique=False):
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    CREATE {unique} INDEX IF NOT EXISTS
                    ON {} ({})
                """).format(
                    sql.SQL("UNIQUE") if unique else sql.SQL(""),
                    sql.Identifier(table),
                    sql.Identifier(column)
                )
            )

    def rename_column(self, table_name, old_column_name, new_column_name):

        """
        Rename a column.

        Parameters:
        - table_name: str, table name
        - old_column_name: str, column being changed
        - new_column_name: str, name it is being changed to.\
        """        

        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("ALTER TABLE {} RENAME COLUMN {} TO {}")
                .format(
                    sql.Identifier(table_name),
                    sql.Identifier(old_column_name),
                    sql.Identifier(new_column_name)
                )
            )

    # ---------- Drop Table ----------

    def drop_table(
        self,
        table_name,
        cascade=False,
        if_exists=True,
        confirm=True
    ):
        
        """
        Drop a table from the public schema.

        Parameters:
        ----------
        confirm : If True, prompt for confirmation before dropping
        """
        if not self.conn:
            raise RuntimeError("No database selected. Connect to a database first.")

        if confirm:
            response = input(
                f"Are you sure you want to drop table '{table_name}'? "
                f"{'(CASCADE)' if cascade else ''} [y/N]: "
            ).strip().lower()

            if response not in {"y", "yes"}:
                print("Drop table aborted.")
                return

        # ---- Build query safely ----
        drop_parts = ["DROP TABLE"]
        if if_exists:
            drop_parts.append("IF EXISTS")
        
        drop_parts.append(sql.Identifier(table_name).as_string(self.conn))
        drop_parts.append("CASCADE" if cascade else "RESTRICT")

        query = " ".join(drop_parts)

        # ---- Execute inside transaction ----
        with self.transaction():
            with self.conn.cursor() as cursor:
                cursor.execute(query)

        print(f"Table '{table_name}' dropped.")


    # ---------- User Management ----------
    def create_user_if_not_exists(self, new_user, password):
        """
        Create a PostgreSQL role with LOGIN if it does not exist.
        Must be run with autocommit=True.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s;",
                (new_user,)
            )

            if cursor.fetchone() is None:
                print(f"Creating user '{new_user}'...")
                cursor.execute(
                    sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s")
                       .format(sql.Identifier(new_user)),
                    (password,)
                )
            else:
                print(f"User '{new_user}' already exists.")

    # ---------- Row Deletion from Table ----------
    def delete_rows_from_table(
        self,
        table,
        where_clause=None,
        params=None,
        allow_full_delete=False,
    ):
        """
        Delete rows from a table.

        Parameters
        ----------
        table : str
            Table name.
        where_clause : str | None
            SQL WHERE clause without the 'WHERE' keyword.
            Example: "status = %s AND created_at < %s"
        params : tuple | list | None
            Parameters for the WHERE clause.
            Example: ("inactive", "2024-01-01") Fits w/ where clause example
        allow_full_delete : bool
            If False, prevents deleting all rows accidentally.

        Returns
        -------
        int
            Number of rows deleted.
        """
        if not self.conn:
            raise RuntimeError("No active connection")

        if where_clause is None and not allow_full_delete:
            raise RuntimeError(
                "Refusing to delete all rows. "
                "Provide a WHERE clause or set allow_full_delete=True."
            )

        params = params or ()

        with self.transaction():
            with self.conn.cursor() as cursor:
                if where_clause:
                    query = sql.SQL("DELETE FROM {} WHERE {}").format(
                        sql.Identifier(table),
                        sql.SQL(where_clause),
                    )
                else:
                    query = sql.SQL("DELETE FROM {}").format(
                        sql.Identifier(table)
                    )

                cursor.execute(query, params)
                deleted = cursor.rowcount

        print(f"Deleted {deleted} row(s) from '{table}'.")
        return deleted

    # ---------- Check a table's values  ----------
    def check_table_in_db(self, 
                          table, limit = 5, 
                          count_total_rows = True, 
                          db_name = None):
        """
        Test if the current connection user can read from a specific table, and what is there.

        Parameters
        ----------
        table : str, Table to attempt reading from
        limit : int, how many rows to print out
        count_total_rows : If True, print a count of total rows in the table
        db_name : If None, use current table connection

        Returns
        -------
        bool
            True if the current user can read from the table, False otherwise.
        """
        if not self.conn:
            print("No database selected. Connect to a database first.")
            return False
        
        original_db = self.db_name
    
        try:
            if db_name and db_name != self.db_name:
                self.switch_db(db_name)
        
            with self.conn.cursor() as cursor:

                if count_total_rows:

                    # Get total row count
                    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                    total_rows = cursor.fetchone()[0]

                    print(f"\nTotal rows in table '{table}': {total_rows}")

                # Get column names
                cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(sql.Identifier(table)))
                col_names = [desc[0] for desc in cursor.description]

                # Fetch rows up to limit 
                query = sql.SQL(
                    "SELECT * FROM {} ORDER BY id DESC LIMIT %s"
                ).format(
                    sql.Identifier(table)
                )

                # Execute query and collect results
                cursor.execute(query, (limit+1,))
                rows = cursor.fetchall()

                user = self.conn.get_dsn_parameters().get("user", "unknown")
                db_name = self.conn.get_dsn_parameters().get("dbname", "unknown")

                print(f"User '{user}' can read from table '{table}' in database '{db_name}'.")

                if not rows:
                    print("No rows found.")
                    return True

                # Print column headers
                print(f" For table {table} in DB {db_name}, the following are returned: ")
                print(" | ".join(col_names))
                print("-" * (len(col_names) * 15))  # simple separator

                # Print each row with columns
                for row in rows:
                    row_str = " | ".join(str(value) for value in row)
                    print(row_str)

                return True

        except Exception as e:
            user = self.conn.get_dsn_parameters().get("user", "unknown")
            print(f"User '{user}' cannot access table '{table}': {e}")
            return False
        
        finally:
            if db_name and self.db_name != original_db:
                self.switch_db(original_db)

    # ---------- Permissions ----------
    def grant_permissions_to_user_to_db(self, 
                                        new_user, 
                                        db_name = None,
                                        grant_select=True,
                                        grant_insert=True,
                                        grant_update=True):
        """
        Grant schema, table, and sequence permissions (existing + future).
        """
        if not self.conn:
            raise RuntimeError("No database selected. Connect to a database first.")
        
        if db_name is None:
            db_name = self.conn.get_dsn_parameters()["dbname"]

        if db_name != self.conn.get_dsn_parameters()["dbname"]:
            self.switch_db(db_name)

        # Build the privileges string based on the flags
        privileges = []
        if grant_select:
            privileges.append("SELECT")
        if grant_insert:
            privileges.append("INSERT")
        if grant_update:
            privileges.append("UPDATE")

        if not privileges:
            raise ValueError("At least one privilege (SELECT, INSERT, UPDATE) must be True.")
        
        priv_str = ", ".join(privileges)

        # Implement user privileges 
        with self.conn.cursor() as cursor:
            # Schema usage
            cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}")
                   .format(sql.Identifier(new_user))
            )

            # Existing tables
            cursor.execute(
                sql.SQL(f"GRANT {priv_str} ON ALL TABLES IN SCHEMA public TO {{}}")
                .format(sql.Identifier(new_user))
            )

            # Existing sequences
            cursor.execute(
                sql.SQL("""
                    GRANT USAGE, SELECT
                    ON ALL SEQUENCES IN SCHEMA public
                    TO {}
                """).format(sql.Identifier(new_user))
            )

            # Future tables
            cursor.execute(
                sql.SQL(f"""
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    GRANT {priv_str} ON TABLES TO {{}}
                """).format(sql.Identifier(new_user))
            )

            # Future sequences
            cursor.execute(
                sql.SQL("""
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    GRANT USAGE, SELECT ON SEQUENCES TO {}
                """).format(sql.Identifier(new_user))
            )

        print(f"Permissions ({priv_str}) granted to '{new_user}' on database '{db_name}'.")

    # ---------- Simplify user creation and access ----------
    def create_user_and_grant_permissions(self, new_user, password, grant_permissions=True):
        """
        Create user and grant permissions atomically.
        """
        with self.transaction():
            self.create_user_if_not_exists(new_user, password)
            if grant_permissions:
                self.grant_permissions_to_user_to_db(new_user)

    # ---------- Permission Revocation ----------
    def revoke_permissions_from_user(self, user):
        """
        Revoke all table and sequence privileges (existing + future).
        """
        db_name = self.conn.get_dsn_parameters()["dbname"]

        with self.conn.cursor() as cursor:
            # Existing tables
            cursor.execute(
                sql.SQL("""
                    REVOKE ALL PRIVILEGES
                    ON ALL TABLES IN SCHEMA public
                    FROM {}
                """).format(sql.Identifier(user))
            )

            # Existing sequences
            cursor.execute(
                sql.SQL("""
                    REVOKE ALL PRIVILEGES
                    ON ALL SEQUENCES IN SCHEMA public
                    FROM {}
                """).format(sql.Identifier(user))
            )

            # Future tables
            cursor.execute(
                sql.SQL("""
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    REVOKE ALL ON TABLES FROM {}
                """).format(sql.Identifier(user))
            )

            # Future sequences
            cursor.execute(
                sql.SQL("""
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    REVOKE ALL ON SEQUENCES FROM {}
                """).format(sql.Identifier(user))
            )

        print(f"All privileges revoked from '{user}' on '{db_name}'.")

    # ---------- Lock or Unlock User ----------

    def lock_or_unlock_user(self, user, lock = True):

        action = sql.SQL("NOLOGIN") if lock else sql.SQL("LOGIN")
        state = "locked" if lock else "unlocked"

        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("ALTER ROLE {} %s")
                .format(sql.Identifier(user), action)
            )
            
        print(f"User '{user}' unlocked ({state} enabled).")

    # Check if they are locked or unlocked
    def is_user_locked(self, user):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s;",
                (user,)
            )
            row = cursor.fetchone()
            return row is not None and not row[0]

    # ---------- Change User Password ----------

    def set_user_password(self, user, password):
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("ALTER ROLE {} PASSWORD %s")
                .format(sql.Identifier(user)),
                (password,)
            )

    # ---------- Drop User ----------

    def drop_user(
        self,
        user,
        reassign_owned_to=None,
        drop_owned=False
    ):
        """
        Remove a PostgreSQL role safely.

        Parameters:
        ----------
        user : str
            Role to drop
        reassign_owned_to : str | None
            Reassign owned objects to this role
        drop_owned : bool
            Drop all objects owned by the user
        """
        with self.conn.cursor() as cursor:
            # Check existence
            cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s;",
                (user,)
            )

            if cursor.fetchone() is None:
                print(f"User '{user}' does not exist.")
                return

            # Reassign owned objects
            if reassign_owned_to:
                cursor.execute(
                    sql.SQL("REASSIGN OWNED BY {} TO {}")
                       .format(
                           sql.Identifier(user),
                           sql.Identifier(reassign_owned_to)
                       )
                )

            # Drop owned objects
            if drop_owned:
                cursor.execute(
                    sql.SQL("DROP OWNED BY {}")
                       .format(sql.Identifier(user))
                )

            # Drop role
            cursor.execute(
                sql.SQL("DROP ROLE {}")
                   .format(sql.Identifier(user))
            )

        print(f"User '{user}' has been dropped.")

    # ---------- List Users and their permissions ----------

    def list_users_and_permissions(self, ignore_system_users=True):
        """
        List all PostgreSQL users/roles and their permissions in the current database.

        Parameters:
        ----------
        ignore_system_users : bool
            If True, only include users that can actually log in (ignore system/service roles).

        Returns:
            dict: {username: {"roles": {...}, "table_privileges": {...}}}
        """
        if not self.conn:
            print("No database selected. Connect to a database first.")
            return {}

        users_info = {}

        with self.conn.cursor() as cursor:
            try:
                # ----- Get all roles/users -----
                cursor.execute("""
                    SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin
                    FROM pg_roles;
                """)

                roles = cursor.fetchall()

                for rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin in roles:
                    
                    # Skip system/service roles if requested
                    if ignore_system_users and not rolcanlogin:
                        continue
                    if ignore_system_users and rolname.startswith(("pg_", "azure", "rds_", "replication")):
                        continue

                    users_info[rolname] = {
                        "roles": {
                            "superuser": rolsuper,
                            "inherit": rolinherit,
                            "create_role": rolcreaterole,
                            "create_db": rolcreatedb,
                            "can_login": rolcanlogin
                        },
                        "table_privileges": {}
                    }

                # ----- Get table privileges -----
                cursor.execute("""
                    SELECT grantee, table_name, privilege_type
                    FROM information_schema.role_table_grants
                    WHERE table_schema='public';
                """)
                for grantee, table_name, privilege in cursor.fetchall():
                    users_info.setdefault(grantee, {"roles": {}, "table_privileges": {}})
                    users_info[grantee]["table_privileges"].setdefault(table_name, set()).add(privilege)

                # ----- Print summary -----
                for user, info in users_info.items():
                    print(f"\nUser: {user}")
                    print(" Roles:", info["roles"])
                    if info["table_privileges"]:
                        print(" Table privileges:")
                        for tbl, privs in info["table_privileges"].items():
                            print(f"  - {tbl}: {', '.join(sorted(privs))}")

                return users_info

            except Exception as e:
                print("Error fetching users and permissions:", e)
                return {}



"""
admin_user="mlpgsql_admin"
admin_password="FFAzure3#"
host="ff-ml-training.postgres.database.azure.com"
port=5432
database="postgres"
sslmode="require"
new_db = "ml_training"

table_def = {
    "id": "SERIAL PRIMARY KEY",
    "model_name": "VARCHAR(100) NOT NULL",
    "epoch": "INT",
    "batchsize": "INT",
    "accuracy": "REAL",
    "loss": "REAL",
    "timestamp": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
}

ml_writer_user = "ml_writer"
ml_writer_pw = "APWforMLWriter5%"

with PostgresAdmin(
    user=admin_user,
    password=admin_password,
    host=host,
    port=port
) as admin:

    # Ensure the database exists
    admin.create_database_if_not_exists(new_db)
    admin.switch_db(new_db)

    # Create a table and drop it
    admin.create_table_if_it_does_not_exists(
            table_name="test_log",
            table_definition=table_def,
            list_tables=True
        )

    admin.drop_table(
            table_name="test_log",
            confirm=False
        )

    # Create a user and grant permissions
    admin.create_user_and_grant_permissions(
            new_user=ml_writer_user,
            password=ml_writer_pw
        )

    # Use transaction for altering table columns
    with admin.transaction():
        admin.alter_column_type("training_log", "accuracy", "FLOAT")
        admin.alter_column_type("training_log", "epoch", "INT", default_sql="0")
        admin.alter_column_type("training_log", "epoch", "INT")

    users = admin.list_users_and_permissions()
"""