import os

from dagster_duckdb import DuckDBResource

from foneplatform.resources.io_manager import DuckDBDuckDBIOManager

from .dbt_resource import get_dbt_resource


def get_database_path() -> str:
    """
    Determines the database path based on the environment.

    Returns:
        str: The database path or connection string.

    Raises:
        ValueError: If required environment variables are not set.
    """
    environment = os.environ.get("ENVIRONMENT", "dev").lower()

    if environment == "dev":
        data_dir = os.environ.get("DATA_DIR")
        if not data_dir:
            raise ValueError("DATA_DIR environment variable is not set in dev mode.")

        # set variables for dbt
        os.environ["ENVIRONMENT"] = environment
        os.environ["DATA_DIR"] = os.path.abspath(data_dir)
        return os.path.join(os.environ["DATA_DIR"], "f1.duckdb")

    elif environment == "prod":
        connection_string = os.environ.get("MOTHERDUCK_CONNECTION_STRING")
        if not connection_string:
            raise ValueError(
                "MOTHERDUCK_CONNECTION_STRING environment variable is not set in prod mode."
            )
        return connection_string

    else:
        raise ValueError("'ENVIRONMENT' environment variable is not set to 'dev' or 'prod'.")


database = get_database_path()

default_resources = {
    "io_manager": DuckDBDuckDBIOManager(database=database),
    "dbt": get_dbt_resource(),
    "duckdb_resource": DuckDBResource(database=database),
}
