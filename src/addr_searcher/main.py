import sys
import argparse
import time
import logging

from src.addr_searcher import utils
from config import config
from src.addr_searcher.rel_searcher import search_address as search_rel
from src.tokenizer.tokens import get_addr_toks
from src.db.connection import get_connection
from src.addr_searcher.full_searcher import search_address as search_full
from src.addr_searcher.fts_searcher import search_address_plain, search_address_phrase

logfname = f'address_data{time.strftime('%Y%m%d%H%M%S')}.csv'
logging.basicConfig(level=logging.INFO)

def signal_handler(sig, frame):
    print('Завершение работы программы')
    sys.exit(0)

def extract_addr_parts(rows):
    addrs = []
    for row in rows:
            addr_part = row.split(' дом ')
            addrs.append(addr_part[0])

def average(conn, addr_toks, algo, count=1):
    avg = 0
    for _ in range(count):
        start_t = time.time()
        res = algo(conn, addr_toks)
        end_t = time.time()
        avg += end_t - start_t
    return res, avg / count

def parse_args():
    parser = argparse.ArgumentParser(description="Поиск адресов из указанного файла")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--test', default=1, type=int, help='Тестирование всех алгоритмов')
    group.add_argument('--rel', action='store_true', help='Использовать разделение по уровням')
    group.add_argument('--full', action='store_true', help='Поиск по пути path')
    group.add_argument('--fts-plain', action='store_true', help='Полнотекстовый поиск (plain)')
    group.add_argument('--fts-phrase', action='store_true', help='Полнотекстовый поиск (phrase)')
    parser.add_argument('--path', '-p', default=config['search']['source_path'], help='Путь к файлу')
    parser.add_argument('--avg', type=int, help='Вычислить среднее время поиска')
    return parser.parse_args()   

def test(conn, addrs, count=1):
    utils.log_address_data(logfname, ['i', 'address', 'rel', 'time', 'full', 'time','fts_plain','time', 'fts_phrase','time'])
    
    addr_objs = [get_addr_toks(addr, False) for addr in addrs]
    addr_toks = [get_addr_toks(addr, True) for addr in addrs]
    for i in range(len(addrs)):
        algs = [search_rel, search_full, search_address_plain, search_address_phrase]
        out = [i, addrs[i]]
        for alg in algs:
            avg = 0
            for _ in range(count):
                start_t = time.time()
                if alg == search_rel:
                    res = alg(conn, addr_objs[i]) if addr_objs[i] else False
                else:
                    res = alg(conn, addr_toks[i])
                end_t = time.time()
                avg += end_t - start_t
            out += [res, avg/count]
        utils.log_address_data(logfname, out)

def main():
    args = parse_args()
    addrs = utils.load_data(args.path)
    conn = get_connection(config)

    if args.test:
        test(conn, addrs, args.test)
        return

    get_list = False if args.rel else True
    addr_toks = [get_addr_toks(addr, get_list) for addr in addrs]
    if args.rel:
        search = search_rel
    elif args.full:
        search = search_full
    elif args.fts_plain:
        search = search_address_plain
    else:
        search = search_address_phrase

    if args.avg:
        for i, addr in enumerate(addr_toks):
            res, avg = average(conn, addr, search, args.avg)
            utils.log_address_data(logfname, [i, addrs[i], res, avg])
    else:
        for i, addr in enumerate(addr_toks):    
            start = time.time()
            res = search(conn, addr)
            #res = search(conn, addr[0])
            #if len(addr) > 1 and not res[0]:
            #    res = search(conn, addr[0])

            end = time.time()
            utils.log_address_data(logfname, [i, addrs[i], res, end - start])

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    utils.log_address_data(logfname, [f'Программа выполнена за {end - start} с.'])
