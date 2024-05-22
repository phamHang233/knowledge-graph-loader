import os

from dotenv import load_dotenv

load_dotenv()


class ArangoDBConfig:
    HOST = os.environ.get("ARANGODB_HOST", '0.0.0.0')
    PORT = os.environ.get("ARANGODB_PORT", '8529')
    USERNAME = os.environ.get("ARANGODB_USERNAME", "root")
    PASSWORD = os.environ.get("ARANGODB_PASSWORD", "dev123")
    CONNECTION_URL = os.getenv("ARANGODB_CONNECTION_URL") or f"arangodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"
    DATABASE = os.getenv('ARANGODB_DATABASE', 'klg_database')
    GRAPH = 'KnowledgeGraph'


class MongoDBConfig:
    HOST = os.environ.get("MONGODB_HOST", '0.0.0.0')
    PORT = os.environ.get("MONGODB_PORT", '8529')
    USERNAME = os.environ.get("MONGODB_USERNAME", "root")
    PASSWORD = os.environ.get("MONGODB_PASSWORD", "dev123")
    CONNECTION_URL = os.getenv("MONGODB_CONNECTION_URL") or f"mongodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"
    DATABASE = os.getenv('MONGODB_DATABASE', 'klg_database')


class ArangoDBOldConfig:
    CONNECTION_URL = os.getenv("ARANGODB_OLD_CONNECTION_URL")
    DATABASE = os.getenv('ARANGODB_OLD_DATABASE', 'klg_database')
    GRAPH = 'KnowledgeGraph'


class ArangoDBLendingConfig:
    HOST = os.environ.get("ARANGODB_LENDING_HOST", '0.0.0.0')
    PORT = os.environ.get("ARANGODB_LENDING_PORT", '8529')
    USERNAME = os.environ.get("ARANGODB_LENDING_USERNAME", "root")
    PASSWORD = os.environ.get("ARANGODB_LENDING_PASSWORD", "dev123")

    CONNECTION_URL = os.getenv("ARANGODB_LENDING_CONNECTION_URL") \
                     or f"arangodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"

    DATABASE = 'klg_database'
    GRAPH = 'knowledge_graph'


class PostgresDBConfig:
    SCHEMA = os.environ.get("POSTGRES_SCHEMA", "public")
    WRAPPED_TOKEN = os.environ.get("POSTGRES_WRAPPED_TOKEN_EVENT_TABLE", "wrapped_token")
    CONNECTION_URL = os.environ.get("POSTGRES_CONNECTION_URL", "postgresql://user:password@localhost:5432/database")
    TRANSFER_EVENT_TABLE = os.environ.get("POSTGRES_TRANSFER_EVENT_TABLE", "transfer_event")
    DAPP_INTERACTION_TABLE = os.environ.get("POSTGRES_DAPP_INTERACTION_TABLE", "dapp_interaction")


class BlockchainETLConfig:
    HOST = os.getenv("BLOCKCHAIN_ETL_HOST")
    PORT = os.getenv("BLOCKCHAIN_ETL_PORT")
    USERNAME = os.getenv("BLOCKCHAIN_ETL_USERNAME")
    PASSWORD = os.getenv("BLOCKCHAIN_ETL_PASSWORD")

    CONNECTION_URL = os.getenv("BLOCKCHAIN_ETL_CONNECTION_URL") or f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"
    DATABASE = 'blockchain_etl'
    DB_PREFIX = os.getenv("DB_PREFIX")


class MongoLendingConfig:
    HOST = os.getenv("MONGO_LENDING_HOST")
    PORT = os.getenv("MONGO_LENDING_PORT")
    USERNAME = os.getenv("MONGO_LENDING_USERNAME")
    PASSWORD = os.getenv("MONGO_LENDING_PASSWORD")

    CONNECTION_URL = os.getenv("MONGO_LENDING_CONNECTION_URL") or f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"
    DATABASE = 'LendingPools'


class TestBlockchainETLConfig:
    CONNECTION_URL = os.getenv("TEST_BLOCKCHAIN_ETL_CONNECTION_URL")
    DATABASE = 'blockchain_etl'


class TokenDatabaseConfig:
    CONNECTION_URL = os.getenv('MONGO_TOKEN_URL')
    DATABASE = 'TokenDatabase'


class FirebaseConfig:
    SECRET_FILE = ".data/secret/firebase.json"
    BUCKET_NAME = "centic-45a8e.appspot.com"


class MonitoringConfig:
    MONITOR_ROOT_PATH = os.getenv("MONITOR_ROOT_PATH", "/home/monitor/.log/")


class MongoDBDexConfig:
    CONNECTION_URL = os.getenv("MONGODB_DEX_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_DEX_DATABASE', 'dex')


class MongoDBSCLabelConfig:
    CONNECTION_URL = os.getenv("MONGODB_SC_LABEL_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_SC_LABEL_DATABASE', 'SmartContractLabel')


class MongoDBdYdXConfig:
    CONNECTION_URL = os.getenv("MONGODB_DYDX_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_DYDX_DATABASE', 'dYdXDatabase')


class MongoCDPConfig:
    CONNECTION_URL = os.getenv("CDP_CONNECTION_URL")
    DATABASE = os.getenv('CDP_DATABASE', 'cdp_database')

class MongoComConfig:
    CONNECTION_URL = os.getenv("COMMUNITY_CONNECTION_URL")
    DATABASE = os.getenv('COMMUNITY_DATABASE', 'CommunityDatabase')

class BigQueryConfig:
    CONNECTION_URL = os.getenv("BIGQUERY_CREDENTIALS")

class ElasticsearchConfig:
    ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_CONNECTION_URL")
    ELASTICSEARCH_PASS = os.getenv("ELASTICSEARCH_PASSWORD")