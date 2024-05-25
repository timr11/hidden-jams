import json
import logging
from typing import Generator

import musicbrainzngs
from hidden_jams.entity import Entity, EntityType

from hidden_jams.memgraph import GraphDB

musicbrainzngs.set_useragent("Hidden Jams", "0.1", "timromanski@gmail.com")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Downloader:
    def __init__(self):
        self.db = GraphDB()

    def cleanup(self):
        self.db.close()

    def fetch_entities(
        self, entity_type: EntityType, limit=100, **kwargs
    ) -> Generator[Entity, None, None]:
        logger.info(
            "Fetching entities of type %s with query '%s'", entity_type, **kwargs
        )
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

        while True:
            logger.info("Fetching page %d for %s", page, entity_type)
            result = browse_method(limit=limit, offset=offset, **kwargs)
            page_entities = result.get(f"{entity_type.value.lower()}-list", [])
            if not page_entities:
                logger.info("No more entities found for %s", entity_type)
                break
            for entity in page_entities:
                yield entity
            offset += limit
            page += 1

    def download(self, entity_type: EntityType, tag: str, limit=100, batch_size=100):
        logger.info("Starting download for %s with query '%s'", entity_type, tag)
        batch = []
        for entity in self.fetch_entities(entity_type, limit, tag=tag):
            logger.debug(
                "Fetched entity: %.200s, batch size: %d / %d",
                json.dumps(entity, indent=2),
                len(batch),
                batch_size,
            )
            batch.append(entity)
            if len(batch) >= batch_size:
                logger.info("Batch size reached, uploading batch")
                self.db.upload_entities(entity_type, batch)
                batch = []
        if batch:
            logger.info("Uploading final batch of size %d", len(batch))
            self.db.upload_entities(entity_type, batch)
        logger.info("Download complete for %s", entity_type)


if __name__ == "__main__":
    # Connect to Neo4j instance
    downloader = Downloader()
    try:
        downloader.download(EntityType.ARTIST, "jazz")
    except Exception as e:
        logger.error("An error occurred: %s", e)
    finally:
        downloader.cleanup()
