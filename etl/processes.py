import json
import logging.config
from queries import queries
import movies_idx
from decorate import backoff

logging.config.fileConfig('config/log_config')
log = logging.getLogger(__name__)


class PgExtractor:
    def __init__(self, pg_conn, state):
        self.conn = pg_conn
        self.state = state
        self.table = (self.extract_filmwork, self.extract_person, self.extract_genre)

    @backoff()
    def extract_filmwork(self) -> tuple:
        curs = self.conn.cursor()
        filmwork_state = self.state.get_state('fw')
        query = queries.get_query('film_work').format(str(filmwork_state))
        curs.execute(query)
        data = curs.fetchall()
        curs.close()
        return 'fw', data

    @backoff()
    def extract_person(self) -> tuple:
        curs = self.conn.cursor()
        person_state = self.state.get_state('pr')
        curs.execute(queries.get_query('person').format(person_state))
        data = curs.fetchall()
        curs.close()
        return 'pr', data

    @backoff()
    def extract_genre(self) -> tuple:
        curs = self.conn.cursor()
        genre_state = self.state.get_state('gn')
        curs.execute(queries.get_query('genre').format(genre_state))
        data = curs.fetchall()
        curs.close()
        return 'gn', data

    def extract(self) -> tuple:
        for extractor in self.table:
            key, data = extractor()
            if data:
                return data, key
            else:
                return None, None


class Transform:
    @staticmethod
    def transforming(data: dict) -> list:
        es_dict = [{'id': row['id'], 'imdb_rating': row['rating'],
                    'genre': row['genres'], 'title': row['title'], 'description': row['description'],
                    'director': [person['person_name'] for person in row['persons'] if
                                 person['person_role'] == 'director'],
                    'writers_names': [person['person_name'] for person in row['persons'] if
                                      person['person_role'] == 'writer'],
                    'actors_names': [person['person_name'] for person in row['persons'] if
                                     person['person_role'] == 'actor'],
                    'actors': [{'id': person['person_id'], 'name': person['person_name']}
                               for person in row['persons'] if person['person_role'] == 'actor'],
                    'writers': [{'id': person['person_id'], 'name': person['person_name']}
                                for person in row['persons'] if person['person_role'] == 'writer']
                    } for row in data]
        return es_dict


class EsLoader:
    def __init__(self, es_conn, index_name='movies'):
        self.es_conn = es_conn
        self.index_name = index_name

    @backoff()
    def create_index(self):
        settings = movies_idx.body
        connect = self.es_conn
        if not connect.indices.exists(index=self.index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            connect.indices.create(index=self.index_name, ignore=400, body=settings)

    def bulk_update(self, datas: list) -> bool:
        connect = self.es_conn
        body = ''
        for data in datas:
            index = {'index': {'_index': self.index_name, '_id': data['id']}}
            body += json.dumps(index) + '\n' + json.dumps(data) + '\n'

        results = connect.bulk(body=body)
        if results['errors']:
            error = [result['index'] for result in results['items'] if result['index']['status'] != 200]
            log.debug(results['took'])
            log.debug(results['errors'])
            log.debug(error)
            return None
        return True

    def make_load(self, datas: list) -> bool:
        self.create_index()
        return self.bulk_update(datas)
