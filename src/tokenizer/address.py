from dataclasses import dataclass

@dataclass
class AddrObject:
    type: str
    name: str
    full_name: str

@dataclass
class Addr:
    region: AddrObject = None
    area: AddrObject = None
    city_dop: AddrObject = None
    city: AddrObject = None
    plan: AddrObject = None
    street: AddrObject = None
