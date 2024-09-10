def search_address_plain(conn, addr_toks):
    with conn.cursor() as cur:
        results = []
        search_str = "full_text @@ plainto_tsquery('russian', %s)"
        params = ' '.join(tok[0] + ' ' + tok[1] for tok in addr_toks)
        query = f'SELECT full_address FROM fias.addr_obj WHERE {search_str}'

        cur.execute(query, (params,))
        results = cur.fetchall()
        return len(results) != 0
    
        #return len(addr_toks) / min(len(res[0].split(',')) for res in results)

def search_address_phrase(conn, addr_toks):
    with conn.cursor() as cur:
        results = []
        search_str = ' and '.join(
                "full_text @@ phraseto_tsquery('russian', %s)"
                for _ in addr_toks
        )
        params = [' '.join(t for t in tok) for tok in addr_toks]

        query = f'SELECT full_address FROM fias.addr_obj WHERE {search_str}'

        cur.execute(query, params)
        results = cur.fetchall()
        return len(results) != 0
    
        #return len(addr_toks) / min(len(res[0].split(',')) for res in results)