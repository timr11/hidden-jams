from enum import Enum
import json
import logging
from typing import Generator, List

import musicbrainzngs
from neo4j import GraphDatabase
from neo4j.work.transaction import Transaction

musicbrainzngs.set_useragent("Hidden Jams", "0.1", "timromanski@gmail.com")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

type Entity = dict


class EntityType(Enum):
    ARTIST = "Artist"
    LABEL = "Label"
    RELEASE = "Release"


class Downloader:
    def __init__(self, uri: str, user: str, password: str):
        logger.info("Initializing Downloader with URI: %s", uri)
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        assert self.driver, "Could not connect to Neo4j"
        logger.info("Successfully connected to Neo4j")

    def close(self):
        assert self.driver, "Could not connect to Neo4j"
        logger.info("Closing connection to Neo4j")
        self.driver.close()
        logger.info("Connection to Neo4j closed")

    def fetch_entities(
        self, entity_type: EntityType, query: str, limit=100, pages=10
    ) -> Generator[Entity, None, None]:
        logger.info("Fetching entities of type %s with query '%s'", entity_type, query)
        offset = 0
        page = 1
        browse_method_name = f"search_{entity_type.value.lower()}s"

        if not hasattr(musicbrainzngs, browse_method_name):
            logger.error(
                "Method %s does not exist in musicbrainzngs", browse_method_name
            )
            raise AttributeError(
                f"Method {browse_method_name} does not exist in musicbrainzngs"
            )

        browse_method = getattr(musicbrainzngs, browse_method_name)

        while page <= pages:
            logger.info("Fetching page %d/%d for %s", page, pages, entity_type)
            result = browse_method(tag=query, limit=limit, offset=offset)
            page_entities = result.get(f"{entity_type.value.lower()}-list", [])
            if not page_entities:
                logger.info("No more entities found for %s", entity_type)
                break
            for entity in page_entities:
                yield entity
            offset += limit
            page += 1

    def _upload_batch(self, entity_type: EntityType, batch: List[Entity]):
        assert self.driver, "Could not connect to Neo4j"
        logger.info(
            "Uploading batch of %d entities of type %s", len(batch), entity_type
        )
        with self.driver.session() as session:
            session.write_transaction(self._create_entities, entity_type, batch)
        logger.info("Batch upload complete")

    @staticmethod
    def _create_entities(tx: Transaction, entity_type: EntityType, batch: List[Entity]):
        for entity in batch:

            def sanitize_key(key):
                return key.replace("-", "_").replace(":", "")

            def sanitize_value(value):
                if isinstance(value, dict):
                    return {
                        sanitize_key(k): sanitize_value(v) for k, v in value.items()
                    }
                elif isinstance(value, list):
                    return [sanitize_value(i) for i in value]
                else:
                    return value

            def sanitize_dict(d):
                if isinstance(d, dict):
                    return {sanitize_key(k): sanitize_value(v) for k, v in d.items()}
                else:
                    return d

            sanitized_entity: dict = sanitize_dict(entity)
            properties = ", ".join(
                [f"{key}: ${key}" for key in sanitized_entity.keys()]
            )
            create_query = f"""
            CREATE (e:{entity_type.value} {{{properties}}})
            """
            logger.debug(
                "Running query: %s with properties: %s", create_query, sanitized_entity
            )
            tx.run(create_query, **sanitized_entity)

    def download(self, entity_type: EntityType, query: str, limit=100, batch_size=100):
        logger.info("Starting download for %s with query '%s'", entity_type, query)
        batch = []
        for entity in self.fetch_entities(entity_type, query, limit, pages=1):
            logger.debug(
                "Fetched entity: %.200s, batch size: %d / %d",
                json.dumps(entity, indent=2),
                len(batch),
                batch_size,
            )
            batch.append(entity)
            if len(batch) >= batch_size:
                logger.info("Batch size reached, uploading batch")
                self._upload_batch(entity_type, batch)
                batch = []
        if batch:
            logger.info("Uploading final batch of size %d", len(batch))
            self._upload_batch(entity_type, batch)
        logger.info("Download complete for %s", entity_type)


if __name__ == "__main__":
    # Connect to Neo4j instance
    logger.info("Connecting to Neo4j")
    uri = "bolt://localhost:7687"
    user = ""
    password = ""
    downloader = Downloader(uri, user, password)
    try:
        downloader.download(EntityType.ARTIST, "jazz", limit=100, batch_size=100)
    except Exception as e:
        logger.error("An error occurred: %s", e)
    finally:
        downloader.close()
