def get_query():
    query = """
    SELECT
       fw.id,
       fw.title,
       fw.description,
       fw.rating,
       fw.type,
       fw.created,
       fw.modified as fw_modified,
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
       {modified_field}
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE {modified_field} > '{modified_date}'
    GROUP BY fw.id, {modified_field}
    ORDER BY {modified_field}
    LIMIT 100;
    """
    return query
