from src.tokenizer.patterns import simple_types, composite_types, addr_elem_socrs
from src.tokenizer.address import Addr, AddrObject

import re

replacements = {f"{{COMP_{i}}}": comp_type for i, comp_type in enumerate(composite_types)}

def restore_replacements(addr_string):
    for replacement, comp_type in replacements.items():
        addr_string = addr_string.replace(replacement, comp_type)
    return addr_string

def get_addr_toks(address, get_list=False):
    addr = re.sub(r'[\.,;\"\']', '', address).lower()

    for placeholder, comp_type in replacements.items():
        addr = addr.replace(comp_type + ' ', placeholder + ' ')

    addr_split = addr.split()

    i = 0
    addr_obj = [] if get_list else Addr()

    while i < len(addr_split):
        token = addr_split[i]
        if token in simple_types or token in replacements:
            address_type = token if token in simple_types else replacements[token]
            name_tokens = []
            i += 1

            while i < len(addr_split) and (addr_split[i] not in simple_types and addr_split[i] not in replacements):
                name_tokens.append(addr_split[i])
                i += 1
            address_name = ' '.join(name_tokens)

            if get_list:
                addr_obj.append([address_type, address_name])
            else:
                addr_object = AddrObject(address_type, address_name, f'{address_type} {address_name}')
                addr = map_address_element(addr_obj, addr_object, address_type)
                if not addr:
                    return None
        else:
            i += 1

    return addr_obj

def map_address_element(addr, addr_object, address_type):
    match addr_elem_socrs.get(address_type):
        case "region":
            if addr.region:
                return None
            addr.region = addr_object
        case "area":
            if addr.area:
                return None
            addr.area = addr_object
        case "city":
            if addr.city:
                if addr.city_dop:
                    return None
                addr.city_dop = addr.city
            addr.city = addr_object
        case "plan":
            if addr.plan:
                return None
            addr.plan = addr_object
        case "street":
            if addr.street:
                    return None
            addr.street = addr_object
    return addr