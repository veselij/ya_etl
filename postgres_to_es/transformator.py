from dataclasses import dataclass, field
from typing import ClassVar

from settings import IndexsEnum


@dataclass
class BaseDoc:
    index_name: ClassVar[str] = IndexsEnum.movies.value
    uuid: str


@dataclass
class Doc(BaseDoc):
    """Class of elasticsearch movies index document."""

    index_name: ClassVar[str] = IndexsEnum.movies.value
    imdb_rating: float
    title: str
    description: str
    directors: list[dict] = field(default_factory=list)
    genre: list[dict] = field(default_factory=list)
    subscription: list[dict] = field(default_factory=list)
    actors: list[dict] = field(default_factory=list)
    writers: list[dict] = field(default_factory=list)


@dataclass
class GenreDoc(BaseDoc):
    """Class of elasticsearch genre index documet."""

    index_name: ClassVar[str] = IndexsEnum.genres.value
    name: str
    description: str


@dataclass
class PersonDoc(BaseDoc):
    """Class of elasticsearch person index documet."""

    index_name: ClassVar[str] = IndexsEnum.persons.value
    full_name: str
    role: str
    film_ids: list[str] = field(default_factory=list)


def transform_movies(bacth: list) -> dict[str, BaseDoc]:
    """Transform list for rows to dictionary of elasticsearch prepared documents for index movies.

    Args:
        bacth: list rows

    Returns:
        dict: prepared documents.
    """
    all_objects = {}
    for row in bacth:
        doc_id = row['fw_id']
        if doc_id not in all_objects:
            all_objects[doc_id] = Doc(
                uuid=doc_id,
                imdb_rating=row['rating'],
                title=row['title'],
                description=row['description'],
            )
        doc = all_objects[doc_id]
        genre_item ={'uuid': row['genre_id'], 'name': row['genre_name']} 
        subscription_item ={'uuid': row['subscription_id'], 'name': row['subscription_name']} 
        if genre_item not in doc.genre:
            doc.genre.append(genre_item)
        if subscription_item not in doc.subscription:
            doc.subscription.append(subscription_item)
        add_roles(doc, row['role'], row['id'], row['full_name'])
    return all_objects


def add_roles(doc: Doc, role: str, role_id: str, full_name: str) -> None:
    """Add roles details to documet.

    Args:
        doc: Doc document to add roles
        role: str role
        role_id: str role id UUID
        full_name: str full name
    """
    if not role:
        return
    nested_field = getattr(doc, "{0}s".format(role), None)
    if nested_field is not None:
        new_value = {"uuid": role_id, "name": full_name}
        if new_value not in nested_field:
            nested_field.append(new_value)


def transform_genre(batch: list) -> dict[str, BaseDoc]:
    """Transform list for rows to dictionary of elasticsearch prepared documents for index genre.

    Args:
        batch: list rows

    Returns:
        dict: prepared documents.
    """
    all_objects = {}
    for row in batch:
        all_objects[row["id"]] = GenreDoc(uuid=row["id"], name=row["name"], description=row["description"])
    return all_objects


# TODO подумать как быть с тем, что один и тот же персон может быть в разных ролях, тогда индекс надо делать состовной для документа
def transform_person(batch: list) -> dict[str, BaseDoc]:
    """Transform list for rows to dictionary of elasticsearch prepared documents for index genre.

    Args:
        batch: list rows

    Returns:
        dict: prepared documents.
    """
    all_objects = {}
    for row in batch:
        doc_id = row["id"]
        if doc_id not in all_objects:
            all_objects[doc_id] = PersonDoc(
                uuid=doc_id,
                full_name=row["full_name"],
                role=row["role"],
            )
        doc = all_objects[doc_id]
        if row['film_work_id'] not in doc.film_ids:
            doc.film_ids.append(row['film_work_id'])
    return all_objects


handlers = {
    IndexsEnum.movies.value: transform_movies,
    IndexsEnum.genres.value: transform_genre,
    IndexsEnum.persons.value: transform_person,
}


def transform_lists_to_dc(bacth: tuple[str, list]) -> dict[str, BaseDoc]:
    """Transform data from database output to elistic prepared format.

    Args:
        bacth: tuple[str, list] input from database producer.

    Returns:
        dict[str, BaseDoc] return elasticsearch preapred dict of documents,
    """
    index, index_data = bacth
    return handlers[index](index_data)
