from collections import OrderedDict
import json
# import OrderedDict as OrderedDict

from queries import queries
import movies_idx

class PgExtractor:
    def __init__(self, pg_conn, state):
        self.conn = pg_conn
        self.state = state
        self.table = (self.extract_filmwork, self.extract_person, self.extract_genre)

    def extract_filmwork(self):
        curs = self.conn.cursor()
        filmwork_state = self.state.get_state('fw')
        query = queries.get_query('film_work').format(str(filmwork_state))
        curs.execute(query)
        data = curs.fetchall()
        curs.close()
        return 'fw', data

    def extract_person(self):
        curs = self.conn.cursor()
        person_state = self.state.get_state('pr')
        curs.execute(queries.get_query('person').format(person_state))
        data = curs.fetchall()
        curs.close()
        return 'pr', data

    def extract_genre(self):
        curs = self.conn.cursor()
        genre_state = self.state.get_state('gn')
        curs.execute(queries.get_query('genre').format(genre_state))
        data = curs.fetchall()
        curs.close()
        return 'gn', data

    def extract(self):
        for extractor in self.table:
            key, data = extractor()
            if data:
                return data, key
            else:
                return None, None


class Transform:
    # def __init__(self):
    #     pass

    @staticmethod
    def transforming(data):
        # film_key = ['id', 'title', 'description', 'imdb_rating', 'type', 'created', 'modified', 'persons', 'genre']
        # film_dict = [dict(zip(film_key, row)) for row in data]

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

    def create_index(self, settings=movies_idx.body):
        created = False
        # index settings
        settings ={
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
                        },
                        "english_stemmer": {
                            "type": "stemmer",
                            "language": "english"
                        },
                        "english_possessive_stemmer": {
                            "type": "stemmer",
                            "language": "possessive_english"
                        },
                        "russian_stop": {
                            "type": "stop",
                            "stopwords": "_russian_"
                        },
                        "russian_stemmer": {
                            "type": "stemmer",
                            "language": "russian"
                        }
                    },
                    "analyzer": {
                        "ru_en": {
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "english_stop",
                                "english_stemmer",
                                "english_possessive_stemmer",
                                "russian_stop",
                                "russian_stemmer"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "imdb_rating": {
                        "type": "float"
                    },
                    "genre": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "ru_en",
                        "fields": {
                            "raw": {
                                "type": "keyword"
                            }
                        }
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "director": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "writers_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    },
                    "writers": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    }
                }
            }
        }
        connect = self.es_conn
        try:
            if not connect.indices.exists(index=self.index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                connect.indices.create(index=self.index_name, ignore=400, body=settings)
                print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        # finally:
        #     return created

    def bulk_update(self, datas) -> bool:
        connect = self.es_conn
        if datas == []:
            # logger.warning('No more data to update in elastic')
            return None
        body = ''
        for data in datas:
            index = {'index': {'_index': self.index_name, '_id': data['id']}}
            body += json.dumps(index) + '\n' + json.dumps(data) + '\n'

        results = connect.bulk(body=body)
        if results['errors']:
            error = [result['index'] for result in results['items'] if result['index']['status'] != 200]
            # logger.debug(results['took'])
            # logger.debug(results['errors'])
            # logger.debug(error)
            print(results['took'])
            print(results['errors'])
            print(error)
            return None
        return True

    def make_load(self, datas):
        self.create_index()
        self.bulk_update(datas)
