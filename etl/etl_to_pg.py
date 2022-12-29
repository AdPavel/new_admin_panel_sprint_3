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
    postgres_extractor = PgExtractor(pg_conn, state)
    els = EsLoader(es_conn)

    try:
        data, key = postgres_extractor.extract()
        if data:
            transform_data = Transform().transforming(data)
            if not els.make_load(transform_data):
                return False, None
            state.set_state(key, data[-1]['modified'])
            return key, len(data)
    except:
        return False, None


if __name__ == '__main__':
    dsl = {'dbname': settings.db_name, 'user': settings.db_user,
           'password': settings.db_password, 'host': settings.host,
           'port': settings.port}

    es_dsl = {'host': settings.es_host, 'port': settings.es_port,
              'scheme': settings.es_scheme}

    storage = state.JsonFileStorage(settings.storage_file)
    state = state.State(storage)

    while True:
        with pg_connect(dsl) as pg_conn, es_connect(es_dsl) as es_conn:
            key, transfer_count = etl_process(pg_conn, es_conn, state)
            if key:
                log.info(f'Для таблицы "{key.split("_")[1]}" в ElasticSearch перенесена пачка из {transfer_count} '
                         f'записей.')
            else:
                log.info('Нет данных для переноса в ElasticSearch.')
                time.sleep(settings.iteration_pause)
