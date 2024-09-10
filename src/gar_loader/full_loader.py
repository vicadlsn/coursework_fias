import re
import zipfile
import logging
from lxml import etree as ET

from src.db import connection

logger = logging.getLogger(__name__)

def import_files(conn, xml_path, xml_names, regions=[]):
    xml_r = '|'.join(t for t in xml_names)
    if regions:
        file_pattern = re.compile(r'^(('+"|".join(c for c in regions)+r')/)?(' + xml_r + r')_[0-9]{8}.+.XML$')
    else:
        file_pattern = re.compile(r'^([0-9]{2}/)?(' + xml_r + r')_[0-9]{8}.+.XML$')
    
    with zipfile.ZipFile(xml_path, mode='r', allowZip64=True) as zip_file:
        fnames = zip_file.namelist()
        files = [fn for fn in fnames if file_pattern.match(fn)]
        for f in files:
            process_file(conn, zip_file, f)



def process_file(conn, zip_file, f):
    dir = f[0:2] 
    with zip_file.open(f, 'r') as file:
        logger.info(f'Зарузка {f}')

        try:
            table_name, fields, data = parse_xml(file, dir)
        except ET.ParseError as e:
            logger.error(f"Не получилось прочитать XML: {str(e)}")
            return
        try:
            connection.insert_into_table(conn, table_name, fields, data)
        except Exception as e:
            logger.error(f'Не удалось вставить значения в таблицу {table_name}: {str(e)}')

        logger.info(f'Загружен {f}')


tags = {
    "ADDRESSOBJECTS": ("OBJECT", "addr_obj"),
    "ITEMS": ("ITEM", "hierarchy"),
    "ADDRESSOBJECTTYPES": ("ADDRESSOBJECTTYPE", "obj_types"),
    "OBJECTLEVELS": ("OBJECTLEVEL", "level_types"),
}


table_fields = {
    "addr_obj": {
        #"ID": "id",
        "OBJECTID": "object_id",
        "NAME": "name",
        "TYPENAME": "type_name",
        "LEVEL": "level"
    },
    "hierarchy": {
        #"ID": "id",
        "OBJECTID": "object_id",
        "PARENTOBJID": "parent_object_id",
        "PATH": "path",
    },
    "obj_types": {
        "ID": "id",
        "LEVEL": "level",
        "NAME": "name",
        "ISACTIVE": "is_active",
    },
    "level_types": {
        "LEVEL": "level",
        "NAME": "name",
        "ISACTIVE": "is_active",
    },
}

def parse_xml(xml_file, dir):
    doc = ET.iterparse(xml_file, events=('start', 'end'))

    _, root = next(doc)
    root_tag = root.tag
    tag, table_name = tags[root_tag]

    fields = [item for _, item in table_fields[table_name].items()]

    if root_tag == "ADDRESSOBJECTS" or root_tag == 'ITEMS':
        fields.append("region_code")

    data = []
    for event, element in doc:
        if event == 'end' and element.tag == tag:
            if tag == 'OBJECT' and (element.get('ISACTIVE') != '1' or element.get('ISACTUAL') != '1'):
                element.clear()
                continue

            if tag == 'ITEM' and element.get('ISACTIVE') != '1':
                element.clear()
                continue

            if tag == "OBJECT":
                type_name = element.get('TYPENAME').replace('.', '').lower()
                name = element.get("NAME").replace('.', '').lower()
                values = [element.get('OBJECTID'), name, type_name, element.get("LEVEL"), dir]
            else:
                values = [element.get(field) for field in table_fields[table_name]]

            if tag == "ITEM":
                values.append(dir)

            data.append(values)
            element.clear()
            
    return table_name, fields, data