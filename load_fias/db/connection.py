import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

def get_connection(config):
    return psycopg2.connect(
            dbname=config['database']['dbname'],
            host=config['database']['host'],
            port=config['database']['port'],
            user=config['database']['user'],
            password=config['database']['password']
    )

def fill_rel_tables(conn, regions, areas, cities, plans, streets, hiers):
    with conn.cursor() as cur:
        cur.execute('create table fias.hier_temp (object_id bigint primary key, parent_object_id bigint);')
        
        execute_values(cur, f'INSERT INTO fias.hier_temp (object_id, parent_object_id) values %s', hiers)
        logger.info('Заполнена временная таблица с иерархией')

        if regions:
            execute_values(cur, f'INSERT INTO fias.Region (object_id, region_code, name, type_name) values %s ON CONFLICT (object_id) DO UPDATE SET region_code=excluded.region_code, name=excluded.name, type_name=excluded.type_name', regions)
        if areas:
            execute_values(cur, f'INSERT INTO fias.Area (object_id, region_code, name, type_name) values %s ON CONFLICT (object_id) DO UPDATE SET region_code=excluded.region_code, name=excluded.name, type_name=excluded.type_name', areas)
        if cities:
            execute_values(cur, f'INSERT INTO fias.City_Settlement (object_id, region_code, name, type_name) values %s ON CONFLICT (object_id) DO UPDATE SET region_code=excluded.region_code, name=excluded.name, type_name=excluded.type_name', cities)
        if plans:
            execute_values(cur, f'INSERT INTO fias.Plan (object_id, region_code, name, type_name)values %s ON CONFLICT (object_id) DO UPDATE SET region_code=excluded.region_code, name=excluded.name, type_name=excluded.type_name', plans)
        if streets:
            execute_values(cur, f'INSERT INTO fias.Street  (object_id, region_code, name, type_name) values %s ON CONFLICT (object_id) DO UPDATE SET region_code=excluded.region_code, name=excluded.name, type_name=excluded.type_name', streets)

                
        logger.info('Заполнены таблицы с адресными объектами')

        cur.execute('''update fias.Area as t
                                set region_id = r.id 
                                from fias.Region as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')
        logger.info('Связи в Area')

        cur.execute('''update fias.City_Settlement as t
                                set region_id = r.id
                                from fias.Region as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')
                
        cur.execute('''update fias.City_Settlement as t
                                set area_id = r.id,
                                    region_id = r.region_id
                                from fias.Area as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')
                
        cur.execute('''update fias.City_Settlement as t
                                set area_id = r.area_id,
                                    region_id = r.region_id,
                                    dop_id = r.id
                                from fias.City_Settlement as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')
        logger.info('Построены связи в City_Settlement')

        cur.execute('''update fias.Plan as t
                                set city_id = r.id
                                from fias.City_Settlement as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')
                
        logger.info('Построены связи в Plan')

        cur.execute('''update fias.Street as t
                                set city_id = r.id
                                from fias.City_Settlement as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')
                
        cur.execute('''update fias.Street as t
                                set city_id = r.city_id,
                                    plan_id = r.id
                                from fias.Plan as r
                                join fias.hier_temp as h on h.parent_object_id  = r.object_id
                                where h.object_id = t.object_id''')

        cur.execute('drop table fias.hier_temp;')
        logger.info('Построены связи в Street')
        conn.commit()

def fill_fts_fields(conn, region_code):
    with conn.cursor() as cur:
        cur.execute(f'call make_full_address({region_code});')
        conn.commit()


def truncate_full(conn):
    with conn.cursor() as cur:
        cur.execute(f'truncate table fias.addr_obj;')
        cur.execute('truncate table fias.hierarchy;')
        conn.commit()

def truncate_rel(conn):
    with conn.cursor() as cur:
        cur.execute(f'truncate table fias.Region cascade;')
        conn.commit()

def truncate_help(conn):
    with conn.cursor() as cur:
        cur.execute(f'truncate table fias.obj_types;')
        cur.execute('truncate table fias.level_types;')
        conn.commit()

def drop_indices_full(conn):
    with conn.cursor() as cur:
        cur.execute(f'drop index if exists idx_addr_obj_type_name;')
        cur.execute('drop index if exists idx_addr_obj_object_id;')
        cur.execute('drop index if exists idx_addr_obj_full_name;')
        cur.execute('drop index if exists idx_addr_obj_region_code;')
        cur.execute('drop index if exists idx_addr_obj_full_text_gin;')
        cur.execute('drop index if exists idx_hier_object_id;')
        conn.commit()

def drop_indices_rel(conn):
    with conn.cursor() as cur:
        cur.execute(f'drop index if exists idx_region_type_name;')
        cur.execute('drop index if exists idx_area_type_name;')
        cur.execute('drop index if exists idx_city_type_name;')
        cur.execute('drop index if exists idx_plan_type_name;')
        cur.execute('drop index if exists idx_street_type_name;')
        conn.commit()

def create_indices_full(conn):
    with conn.cursor() as cur:
        cur.execute('create index if not exists idx_addr_obj_type_name on fias.addr_obj(name, type_name);')
        cur.execute('create index if not exists idx_addr_obj_object_id on fias.addr_obj(object_id);')
        cur.execute('create index if not exists idx_addr_obj_region_code on fias.addr_obj(region_code);')
        cur.execute('create index if not exists idx_hierarchy_object_id on fias.hierarchy(object_id);')
        conn.commit()

def create_indices_rel(conn):
    with conn.cursor() as cur:
        cur.execute('create index if not exists idx_region_type_name on fias.Region(name, type_name);')
        cur.execute('create index if not exists idx_area_type_name on fias.Area(name, type_name);')
        cur.execute('create index if not exists idx_city_type_name on fias.City_Settlement(name, type_name);')
        cur.execute('create index if not exists idx_plan_type_name on fias.Plan(name, type_name);')
        cur.execute('create index if not exists idx_street_type_name on fias.Street(name, type_name);')
        conn.commit()

def create_index_full_gin(conn):
    with conn.cursor() as cur:
        cur.execute('create index if not exists idx_addr_obj_full_text_gin on fias.addr_obj using gin(full_text);')
        conn.commit()

def insert_into_table(conn, table_name, fields, values):
    update_fields = ', '.join(f"{field} = excluded.{field}" for field in fields)
    with conn.cursor() as cur:
        if 'object_id' in fields:
            execute_values(cur, f'insert into fias.{table_name} ({", ".join(fields)}) values %s on conflict(object_id) do update set {update_fields}', (values))
        else:
            execute_values(cur, f'insert into fias.{table_name} ({", ".join(fields)}) values %s', (values))
        conn.commit()

def drop_full(conn):
    with conn.cursor() as cur:
        with open('db/sql/create_tables_full.sql', 'r') as sql:
            cur.execute(sql.read())

def drop_rel(conn):
    with conn.cursor() as cur:
        with open('db/sql/create_tables_rel.sql', 'r') as sql:
            cur.execute(sql.read())
        conn.commit()