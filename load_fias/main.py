import argparse
import signal
import sys
from config import config
import load_full
import psycopg2
import logging
from db import connection
import load_rel


logging.basicConfig(filename='load_fias',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Загрузчик данных из ГАР")
    parser.add_argument("--truncate", action="store_true", help="Удалить все данные из таблицы перед закгрузкой в неё")
    parser.add_argument("--keep-indexes", action="store_true", help="Сохранить индексы перед загрузкой")
    parser.add_argument("--regions", '-r', type=int, nargs='+', help="Номера регионов для загрузки. 01-99")
    parser.add_argument("--gar-path", '-p', help="Путь к архиву с ГАР")
    parser.add_argument("--full-gar", action='store_true', help="Загрузить как в ГАР")
    parser.add_argument("--help-tables", action='store_true', help="Загрузить справочные таблицы")

    return parser.parse_args()

def signal_handler(sig, frame):
    print('Завершение работы программы')
    sys.exit(0)


def load_gar(conn, args):
    regions = args.regions if args.regions else config['default']['regions']
    regions = list(map(lambda x: f'{x:02d}', regions))
    gar_path = args.gar_path if args.gar_path else config['default']['gar_path']
    full_gar = args.full_gar

    if args.truncate:
        logger.info('Удаление содержимого таблиц')

        if full_gar:
            connection.drop_full(conn)
        else:
            connection.drop_rel(conn)

    if not args.truncate and not args.keep_indexes:
        logger.info('Удаление индексов')

        if full_gar:
            connection.drop_indices_full(conn)
        else:
            connection.drop_indices_rel(conn)
    
    tables = []
    if args.full_gar or args.help_tables:
        if args.full_gar:
            tables += ['AS_ADDR_OBJ', 'AS_ADM_HIERARCHY']
        else:
            connection.truncate_help(conn)
            tables += ['AS_OBJECT_LEVELS', 'AS_ADDR_OBJ_TYPES']
        load_full.import_files(conn, gar_path, tables, regions)
    else:
        load_rel.import_files(conn, gar_path, regions)

    logger.info('Заполнены все таблицы')

    if not args.keep_indexes:
        logger.info('Построение индексов')

        if full_gar:
            connection.create_indices_full(conn)
        if not args.help_tables:
            connection.create_indices_rel(conn)

    if args.full_gar:
        logger.info('Построение цепочек с полными адресами')
        for region in regions:
            connection.fill_fts_fields(conn, int(region))
            logger.info(f'Построены цепочки с полными адресами для региона {region}')

        logger.info('Построение индекса для полнотекстового поиска')
        connection.create_index_full_gin(conn)

    conn.close()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    args = parse_args()

    conn = connection.get_connection(config)
    if not conn:
        return
    
    try:
        load_gar(conn, args)
    except Exception as e:
        logger.fatal(e)
    finally:
        conn.close()
    

if __name__ == '__main__':
    main()