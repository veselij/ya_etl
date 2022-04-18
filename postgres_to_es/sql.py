# SQL to track and update changes in separate FILM_WORKs index
SQL_PERSON_GET_TRACKED_IDs = """
SELECT id, modified
FROM content.person
WHERE modified > '{last_tracked}'
ORDER BY modified
LIMIT 100;
"""

SQL_GENRE_GET_TRACKED_IDs = """
SELECT id, modified
FROM content.genre
WHERE modified > '{last_tracked}'
ORDER BY modified
LIMIT 100;
"""

SQL_MOVIE_GET_TRACKED_IDs = """
SELECT id, modified
FROM content.film_work
WHERE modified > '{last_tracked}'
ORDER BY modified
LIMIT 100;
"""

SQL_PERSON_GET_FILM_IDs = """
SELECT distinct fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
WHERE pfw.person_id IN ({tracked_ids}) and modified > '{last_tracked}'
ORDER BY fw.modified
LIMIT 100;
"""

SQL_GENRE_GET_FILM_IDs = """
SELECT distinct fw.id, fw.modified
FROM content.genre fw
LEFT JOIN content.genre_film_work pfw ON pfw.genre_id = fw.id
WHERE pfw.genre_id IN ({tracked_ids}) and modified > '{last_tracked}'
ORDER BY fw.modified
LIMIT 100;
"""

SQL_GET_FILMs = """
SELECT distinct
    fw.id as fw_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created, 
    fw.modified, 
    pfw.role, 
    p.id, 
    p.full_name,
    g.name,
    g.id as genre_id
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id IN ({film_ids})
ORDER BY fw.id;
"""

# SQL to track and update changes in separate GENREs index
SQL_STANDALONE_GENRE_GET_TRACKED_IDs = """
SELECT id, modified
FROM content.genre
WHERE modified > '{last_tracked}'
ORDER BY modified
LIMIT 100;
"""

SQL_GET_GENREs = """
SELECT id, name, description
FROM content.genre 
WHERE id in ({film_ids});
"""

# SQL to track and update changes in separate PERSONs index
SQL_STANDALONE_PERSON_GET_TRACKED_IDs = """
SELECT id, modified
FROM content.person
WHERE modified > '{last_tracked}'
ORDER BY modified
LIMIT 100;
"""

SQL_GET_PERSONs = """
SELECT p.id, p.full_name, pfw.role, pfw.film_work_id
FROM content.person p 
LEFT JOIN content.person_film_work pfw on pfw.person_id = p.id
WHERE p.id IN ({film_ids})
ORDER BY p.id;
"""
