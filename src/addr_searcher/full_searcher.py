def search_address(conn, toks):
    with conn.cursor() as cur:
        if not len(toks):
            return False

        type, name = toks[-1]
        sql_get_path = """select h.path
            from fias.addr_obj as ao
            join fias.hierarchy h on ao.object_id = h.object_id
            where ao.name = %s and ao.type_name = %s;"""
        cur.execute(sql_get_path, (name, type,))
        rows = cur.fetchall()
        if not rows:
            return False
        
        for row in rows:
            path = row[0]
            path_objects = path.split('.')[:-1]
            tok_index = len(toks) - 2
            
            while tok_index >= 0:
                if not path_objects:
                    break
                path_object_id = path_objects.pop()
                cur.execute("select name, type_name from fias.addr_obj where object_id = %s", (path_object_id,))
                cur_row = cur.fetchone() 
                if not cur_row:
                    break
                
                path_name, path_type = cur_row
                tok_type, tok_name = toks[tok_index] 
                if path_name == tok_name and path_type == tok_type:
                        tok_index -= 1

            if tok_index < 0:
                 return True
                
        return False