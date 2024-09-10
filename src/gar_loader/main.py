import argparse
import signal
import sys
import logging
import os

from config import config
from src.gar_loader.full_loader import import_files as import_full
from src.gar_loader.rel_loader import import_files as import_rel
from src.db import connection

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log_file_path = os.path.join(root, 'load_fias.log')

logging.basicConfig(filename=log_file_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Загрузчик данных из ГАР")
    parser.add_argument("--truncate", action="store_true", help="Удалить все данные из таблицы перед загрузкой в неё")
    parser.add_argument("--keep-indexes", action="store_true", help="Сохранить индексы перед загрузкой")
    parser.add_argument("--regions", '-r', type=int, nargs='+', help="Номера регионов для загрузки. 1-99")
    parser.add_argument("--gar-path", '-p', help="Путь к архиву с ГАР")
    parser.add_argument("--rel-gar", action='store_true', help="Загрузить с разделением по иерархиям")
    parser.add_argument("--full-gar", action='store_true', help="Загрузить как в ГАР")
    parser.add_argument("--help-tables", action='store_true', help="Загрузить справочные таблицы")

    return parser.parse_args()

def signal_handler(sig, frame):
    print('Завершение работы программы')
    sys.exit(0)

def load_full(conn, args):
    regions = args.regions if args.regions else config['load']['regions']
    regions = list(map(lambda x: f'{x:02d}', regions))
    gar_path = args.gar_path if args.gar_path else config['load']['gar_path']

    logger.info('Загрузка ГАР')
    if args.truncate and args.full_gar:
        logger.info('Удаление содержимого таблиц')
        connection.drop_full(conn)

    if not args.truncate and not args.keep_indexes and args.full_gar:
        logger.info('Удаление индексов')
        connection.drop_indexes_full(conn)
    
    tables = []
    if args.full_gar:
        tables += ['AS_ADDR_OBJ', 'AS_ADM_HIERARCHY']
    if args.help_tables:
        connection.truncate_help(conn)
        tables += ['AS_OBJECT_LEVELS', 'AS_ADDR_OBJ_TYPES']
    
    import_full(conn, gar_path, tables, regions)

    logger.info('Заполнены все таблицы')

    if not args.keep_indexes and args.full_gar:
        logger.info('Построение индексов')
        connection.create_indexes_full(conn)

    if args.full_gar:
        logger.info('Построение цепочек с полными адресами')
        if not args.regions:
            regions = [i for i in range(1, 100)]
        for region in regions:
            connection.fill_fts_fields(conn, int(region))
            logger.info(f'Построены цепочки с полными адресами для региона {region}')

        logger.info('Построение индекса для полнотекстового поиска')
        connection.create_index_full_gin(conn)

def load_rel(conn, args):
    regions = args.regions if args.regions else config['load']['regions']
    regions = list(map(lambda x: f'{x:02d}', regions))
    gar_path = args.gar_path if args.gar_path else config['load']['gar_path']

    logger.info('Загрузка ГАР с разделением по уровням')
    if args.truncate:
        logger.info('Удаление содержимого таблиц')
        connection.drop_rel(conn)

    if not args.truncate and not args.keep_indexes:
        connection.drop_indexes_rel(conn)
    
    import_rel(conn, gar_path, regions)

    logger.info('Заполнены все таблицы')

    if not args.keep_indexes:
        logger.info('Построение индексов')
        connection.create_indexes_rel(conn)


def load_gar(conn, args):
    if not (args.rel_gar or args.full_gar or args.help_tables):
        logger.error('Необходимо указать способ загрузки')
        return

    if args.rel_gar:
        load_rel(conn, args)
    if args.full_gar or args.help_tables:

        load_full(conn, args)

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