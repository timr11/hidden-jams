import logging
from neo4j import GraphDatabase
from neo4j.work.transaction import Transaction
from hidden_jams.entity import EntityType, Entity, sanitize_entity
from hidden_jams.settings import settings

from typing import List

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class GraphDB:
    def __init__(self):
        logger.info("Initializing GraphDB with URI: %s", settings.neo4j_uri)
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
        )
        assert self.driver, "Could not connect to Neo4j"
        logger.info("Successfully connected to Neo4j")

    def close(self):
        assert self.driver, "Could not connect to Neo4j"
        logger.info("Closing connection to Neo4j")
        self.driver.close()
        logger.info("Connection to Neo4j closed")

    def upload_entities(self, entity_type: EntityType, entities: List[Entity]):
        sanitized_entities = [sanitize_entity(e) for e in entities]
        query = f"""
        UNWIND $batch as entity
        CREATE (e:{entity_type.value})
        SET e += entity
        """
        self.run_query(query, {"batch": sanitized_entities})

    def run_query(self, query: str, parameters: dict):
        assert self.driver, "Could not connect to Neo4j"
        logger.debug("Running query: %s with parameters: %s", query, parameters)
        with self.driver.session() as session:
            result = session.write_transaction(self._execute_query, query, parameters)
        logger.debug("Query execution complete")
        return result

    def delete_all_of_entity_type(self, entity_type: EntityType):
        query = f"MATCH (n:{entity_type.value}) DETACH DELETE n"
        self.run_query(query, {})

    @staticmethod
    def _execute_query(tx: Transaction, query: str, parameters: dict):
        return tx.run(query, parameters or {}).data()
