from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    neo4j_uri: str = Field(alias="NEO4J_URI", default="bolt://localhost:7687")
    neo4j_user: str = Field(alias="NEO4J_USER", default="")
    neo4j_password: str = Field(alias="NEO4J_PASSWORD", default="")

    class Config:
        env_file = ".env"


settings = Settings()
