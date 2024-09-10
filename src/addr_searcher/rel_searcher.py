def search_address(conn, addr):
    if not addr:
        return False
    
    join_parts = []
    where_parts = []
    values = []

    if addr.region:
        set_region_query(addr, join_parts, where_parts, values)
    if addr.area:
        set_area_query(addr, join_parts, where_parts, values)
    if addr.city_dop:
        set_city_dop_query(addr, join_parts, where_parts, values)
    if addr.city:
        set_city_query(addr, join_parts, where_parts, values)
    if addr.plan:
        set_plan_query(addr, join_parts, where_parts, values)
    if addr.street:
        set_street_query(addr, join_parts, where_parts, values)

    query = f"SELECT count(1) FROM {' '.join(join_parts)} WHERE {' AND '.join(where_parts)}"

    print(query % tuple(values))
    with conn.cursor() as cur:
        cur.execute(query, values)
        res_count = cur.fetchall()[0][0]
        return res_count > 0


def set_region_query(addr, join_parts, where_parts, values):
    join_parts.append('fias.Region AS r')
    where_parts.append('r.name = %s AND r.type_name = %s')
    values.extend([addr.region.name, addr.region.type])


def set_area_query(addr, join_parts, where_parts, values):
    if not addr.region:
        join_parts.append("fias.Area AS a")
    else:
        join_parts.append("JOIN fias.Area AS a ON r.id = a.region_id")
    where_parts.append("a.name = %s AND a.type_name = %s")
    values.extend([addr.area.name, addr.area.type])


def set_city_dop_query(addr, join_parts, where_parts, values):
    if not addr.region and not addr.area:
        join_parts.append("fias.City_Settlement AS cs_dop")
    elif not addr.region:
        join_parts.append("JOIN fias.City_Settlement AS cs_dop ON a.id = cs_dop.area_id")
    elif not addr.area:
        join_parts.append("JOIN fias.City_Settlement AS cs_dop ON r.id = cs_dop.region_id")
    else:
        join_parts.append("JOIN fias.City_Settlement AS cs_dop ON r.id = cs_dop.region_id AND a.id = cs_dop.area_id")
    where_parts.append("cs_dop.name = %s AND cs_dop.type_name = %s")
    values.extend([addr.city_dop.name, addr.city_dop.type])


def set_city_query(addr, join_parts, where_parts, values):
    if addr.city_dop:
        join_parts.append("JOIN fias.City_Settlement AS cs ON cs_dop.id = cs.dop_id")
        where_parts.append("cs.name = %s AND cs.type_name = %s")
        values.extend([addr.city.name, addr.city.type])
        return

    if not addr.region and not addr.area:
        join_parts.append("fias.City_Settlement AS cs")
    elif not addr.region:
        join_parts.append("JOIN fias.City_Settlement AS cs ON a.id = cs.area_id")
    elif not addr.area:
        join_parts.append("JOIN fias.City_Settlement AS cs ON r.id = cs.region_id")
    else:
        join_parts.append("JOIN fias.City_Settlement AS cs ON r.id = cs.region_id AND a.id = cs.area_id")
    
    join_parts.append("LEFT JOIN fias.City_Settlement AS cs_dop ON cs_dop.id = cs.dop_id")
    where_parts.append("(cs.name = %s AND cs.type_name = %s OR cs_dop.name = %s AND cs_dop.type_name = %s)")
    values.extend([addr.city.name, addr.city.type, addr.city.name, addr.city.type])


def set_plan_query(addr, join_parts, where_parts, values):
    if not addr.region:
        if not addr.area:
            if not addr.city:
                join_parts.append("fias.Plan AS spd")
            else:
                join_parts.append("JOIN fias.Plan AS spd ON cs.id = spd.city_id")
        elif not addr.city:
            join_parts.append("JOIN fias.City_Settlement AS cs ON a.id = cs.area_id JOIN fias.Street AS spd ON cs.id = spd.city_id")
        else:
            join_parts.append("JOIN fias.Plan AS spd ON cs.id = spd.city_id")
    elif not addr.area and not addr.city:
        join_parts.append("JOIN fias.City_Settlement AS cs ON r.id = cs.region_id JOIN fias.Plan AS spd ON cs.id = spd.city_id")
    elif not addr.area and addr.city:
        join_parts.append("JOIN fias.Plan AS spd ON cs.id = spd.city_id")
    elif addr.city:
        join_parts.append("JOIN fias.Plan AS spd ON cs.id = spd.city_id")
    else:
        join_parts.append("JOIN fias.City_Settlement AS cs ON r.id = cs.region_id AND a.id = cs.area_id JOIN fias.Plan AS spd ON cs.id = spd.city_id")
    
    where_parts.append("spd.name = %s AND spd.type_name = %s")
    values.extend([addr.plan.name, addr.plan.type])


def set_street_query(addr, join_parts, where_parts, values):
    if addr.plan:
        join_parts.append("JOIN fias.Street AS sp ON spd.id = sp.plan_id")
        where_parts.append("sp.name = %s AND sp.type_name = %s")
        values.extend([addr.street.name, addr.street.type])
        return

    if not addr.region:
        if not addr.area:
            if not addr.city:
                join_parts.append("fias.Street AS sp")
            else:
                join_parts.append("JOIN fias.Street AS sp ON cs.id = sp.city_id")
        elif not addr.city:
            join_parts.append("JOIN fias.City_Settlement AS cs ON a.id = cs.area_id JOIN fias.Street AS sp ON cs.id = sp.city_id")
        else:
            join_parts.append("JOIN fias.Street AS sp ON cs.id = sp.city_id")
                
    elif not addr.area and not addr.city:
        join_parts.append("JOIN fias.City_Settlement AS cs ON r.id = cs.region_id JOIN fias.Street AS sp ON cs.id = sp.city_id")
    elif not addr.area and addr.city:
        join_parts.append("JOIN fias.Street AS sp ON cs.id = sp.city_id")
    elif addr.city:
        join_parts.append("JOIN fias.Street AS sp ON cs.id = sp.city_id")
    else:
        join_parts.append("JOIN fias.City_Settlement AS cs ON r.id = cs.region_id AND a.id = cs.area_id JOIN fias.Street AS sp ON cs.id = sp.city_id")
    
    where_parts.append("sp.name = %s AND sp.type_name = %s")
    values.extend([addr.street.name, addr.street.type])
