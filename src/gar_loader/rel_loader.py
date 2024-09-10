import os
import zipfile
import re
import lxml.etree as ET
import logging

from src.db import connection

logger = logging.getLogger(__name__)

def import_files(conn, xml_path, regions=[]):
    xml_names = ['AS_ADDR_OBJ', 'AS_ADM_HIERARCHY']
    xml_r = '|'.join(t for t in xml_names)
    if regions:
        file_pattern = re.compile(r'^'+r'('+"|".join(c for c in regions)+r')'+r'/(' + xml_r + r')_[0-9]{8}.+.XML$')
    else:
        file_pattern = re.compile(r'^([0-9]{2}/)?(' + xml_r + r')_[0-9]{8}.+.XML$')
    with zipfile.ZipFile(xml_path, mode='r', allowZip64=True) as zip_file:
        fnames = zip_file.namelist()
        file_names = [fn for fn in fnames if file_pattern.match(fn)]
        folders = {}
        for file_name in file_names:
            dir_name = os.path.dirname(file_name)
            if not dir_name:
                continue
            if dir_name not in folders:
                folders[dir_name] = []

            folders[dir_name].append(file_name)

        for folder in folders:
            process_files(conn, zip_file, folder, folders[folder])

def process_files(conn, zip_file, dir, files):
    dir = int(dir)

    for file_name in files:
        xml_file =  zip_file.open(file_name, 'r')
        logging.info(f'Загрузка {file_name}')
        try:
            doc = ET.iterparse(xml_file, events=('start', 'end',))
        except ET.ParseError as e:
            logging.error(f"Не получилось прочитать XML: {str(e)}")
            return

        _, root = next(doc)
        root_tag = root.tag
        if root_tag == 'ADDRESSOBJECTS':
            regions, areas, cities, plans, streets = parse_addr_objs(doc, dir)
        elif root_tag == 'ITEMS':
            hiers = parse_hier(doc)

    try:
        connection.fill_rel_tables(conn, regions, areas, cities, plans, streets, hiers)
    except Exception as e:
        conn.rollback()
        logging.error(f'Не удалось заполнить регион {dir}: {e}')

    logger.info(f'Загружен {files}')


def parse_addr_objs(doc, dir):
    regions = []
    areas = []
    cities = []
    plans = []
    streets = []

    city_names = ['г', 'гфз', 'п', 'пос', 'тер']
    street_names = ['ул', 'пер', 'б-р', 'вал', 'б-г', 'берег', 'ал', 'аллея', 'въезд', 'взд']

    for event, element in doc:
        if event != 'end':
            continue
        if element.tag != 'OBJECT' or element.get("ISACTIVE") != '1' or element.get("ISACTUAL") != '1':
            element.clear()
            continue

        object_id, level, name, type_name = element.get('OBJECTID'),  element.get('LEVEL'), element.get('NAME'), element.get('TYPENAME')

        type_name = type_name.replace('.', '').lower()
        name = name.replace('.', '').lower()

        if level not in ['1', '2', '5', '6', '7', '8']:
            element.clear()
            continue
        if level == '1' and type_name not in ['г', 'гфз']:
            regions.append([object_id, dir, name, type_name])
        elif level == '2' and type_name not in city_names:
            areas.append([object_id, dir, name, type_name])
        elif level == '5' or level == '6' or type_name in city_names and level in ('1', '2'):
            cities.append([object_id, dir,  name, type_name])
        elif level == '7' and type_name not in street_names:
            plans.append([object_id, dir, name, type_name])
        elif level == '8' or level == '7':# or type_name in street_names:
            streets.append([object_id, dir, name, type_name])

        element.clear()
    return regions, areas, cities, plans, streets

def parse_hier(doc):
    hier = []

    for event, element in doc:
        if event != 'end' or element.tag != 'ITEM':
            continue
        if element.get("ISACTIVE") != '1':
            element.clear()
            continue

        hier.append([element.get('OBJECTID'),element.get('PARENTOBJID')])

        element.clear()

    return hier
