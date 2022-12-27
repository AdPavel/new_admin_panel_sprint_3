def get_query(query_name):
    film_work = """
    SELECT
       fw.id,
       fw.title,
       fw.description,
       fw.rating,
       fw.type,
       fw.created,
       fw.modified,
       COALESCE (
           json_agg(
               DISTINCT jsonb_build_object(
                   'person_role', pfw.role,
                   'person_id', p.id,
                   'person_name', p.full_name
               )
           ) FILTER (WHERE p.id is not null),
           '[]'
       ) as persons,
       array_agg(DISTINCT g.name) as genres
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified > '{0}'
    GROUP BY fw.id
    ORDER BY fw.modified
    LIMIT 100;
    """
    person = """
    SELECT
       fw.id,
       fw.title,
       fw.description,
       fw.rating,
       fw.type,
       fw.created,
       fw.modified,
       COALESCE (
           json_agg(
               DISTINCT jsonb_build_object(
                   'person_role', pfw.role,
                   'person_id', p.id,
                   'person_name', p.full_name
               )
           ) FILTER (WHERE p.id is not null),
           '[]'
       ) as persons,
       array_agg(DISTINCT g.name) as genres,
       p.modified as person_modified
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    
    WHERE p.modified > '{0}'
    GROUP BY p.modified, fw.id
    ORDER BY p.modified
    LIMIT 100;
    """
    genre = """
    SELECT
       fw.id,
       fw.title,
       fw.description,
       fw.rating,
       fw.type,
       fw.created,
       fw.modified,
       COALESCE (
           json_agg(
               DISTINCT jsonb_build_object(
                   'person_role', pfw.role,
                   'person_id', p.id,
                   'person_name', p.full_name
               )
           ) FILTER (WHERE p.id is not null),
           '[]'
       ) as persons,
       array_agg(DISTINCT g.name) as genres,
       g.modified as genre_modified
    FROM content.film_work fw
    JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    JOIN content.genre g ON g.id = gfw.genre_id
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE g.modified > '{0}'
    GROUP BY fw.id, g.modified
    ORDER BY g.modified
    LIMIT 100;
    """
    dict_queries = {'film_work': film_work, 'person': person, 'genre': genre}
    return dict_queries[query_name]
