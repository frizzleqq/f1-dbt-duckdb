import os

from dagster_duckdb import DuckDBResource

from foneplatform.resources.io_manager import DuckDBDuckDBIOManager

from .dbt_resource import get_dbt_resource


def get_database_path() -> tuple[str, dict]:
    """
    Determines the database path based on the environment.

    Returns:
        str: The database path or connection string.

    Raises:
        ValueError: If required environment variables are not set.
    """
    environment = os.environ.get("ENVIRONMENT", "dev").lower()

    if environment == "dev":
        data_dir = os.environ.get("DATA_DIR", "data")

        # set variables for dbt
        os.environ["ENVIRONMENT"] = environment
        os.environ["DATA_DIR"] = os.path.abspath(data_dir)
        return os.path.join(os.environ["DATA_DIR"], "f1.duckdb"), {}

    elif environment == "prod":
        motherduck_token = os.environ.get("MOTHERDUCK_TOKEN")
        if not motherduck_token:
            raise ValueError("MOTHERDUCK_TOKEN environment variable is not set in prod mode.")
        return "md:f1", {"motherduck_token": motherduck_token}

    else:
        raise ValueError("'ENVIRONMENT' environment variable is not set to 'dev' or 'prod'.")


database, config = get_database_path()

default_resources = {
    "io_manager": DuckDBDuckDBIOManager(database=database, connection_config=config),
    "dbt": get_dbt_resource(),
    "duckdb_resource": DuckDBResource(database=database, connection_config=config),
}
