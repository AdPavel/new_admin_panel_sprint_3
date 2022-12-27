import psycopg2
import time
import logging.config
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch

from decorate import backoff
from config.config import settings
from state import state
from processes import PgExtractor, Transform, EsLoader

logging.config.fileConfig('config/log_config')
log = logging.getLogger(__name__)


@backoff()
def pg_connect(dsl):
    pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    return pg_conn


@backoff()
def es_connect(es_dsl):
    elastic = Elasticsearch([es_dsl],)
    if not elastic.ping():
        raise ValueError('Elasticsearch connection failed')
    else:
        return elastic


def etl_process(pg_conn: _connection, es_conn: Elasticsearch, state: state.State):

    param_dict = {'fw': 'modified',
                  'pr': 'person_modified',
                  'gn': 'genre_modified'}

    postgres_extractor = PgExtractor(pg_conn, state)
    els = EsLoader(es_conn)

    data, key = postgres_extractor.extract()
    if data:
        transform_data = Transform().transforming(data)
        if not els.make_load(transform_data):
            return None
        state.set_state(key, data[-1][param_dict[key]])
    else:
        return None


if __name__ == '__main__':
    dsl = {'dbname': settings.db_name, 'user': settings.db_user,
           'password': settings.db_password, 'host': settings.host,
           'port': settings.port}

    es_dsl = {'host': settings.es_host, 'port': settings.es_port,
              'scheme': settings.es_scheme}

    storage = state.JsonFileStorage('store.json')
    state = state.State(storage)

    while True:
        with pg_connect(dsl) as pg_conn, es_connect(es_dsl) as es_conn:
            if not etl_process(pg_conn, es_conn, state):
                log.warning('No data to update')
                time.sleep(30)
