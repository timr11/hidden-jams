from enum import Enum

type Entity = dict


class EntityType(Enum):
    ARTIST = "Artist"
    LABEL = "Label"
    RELEASE = "Release"


def sanitize_key(key):
    return key.replace("-", "_").replace(":", "")


def sanitize_value(value):
    if isinstance(value, dict):
        return {sanitize_key(k): sanitize_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [sanitize_value(i) for i in value]
    else:
        return value


def sanitize_entity(d):
    if isinstance(d, dict):
        return {sanitize_key(k): sanitize_value(v) for k, v in d.items()}
    else:
        return d
