from typing import List, Optional

class LocationMapper:
    STATION_MAPPING = {
        'BELLEVILLE': 'FRM',
        'BIG BEND': 'BBC', 
        'BONDVILLE': 'BVL',
        'BROWNSTOWN': 'BRW',
        'CARBONDALE': 'SIU',
        'CHAMPAIGN': 'CMI',
        'DEKALB': 'DEK',
        'DIXON SPRINGS': 'DXS',
        'FAIRFIELD': 'FAI',
        'FREEPORT': 'FRE',
        'KILBOURNE': 'SFM',
        'MONMOUTH': 'MON',
        'OLNEY': 'OLN',
        'PEORIA': 'ICC',
        'PERRY': 'ORR',
        'REND LAKE': 'RND',
        'SNICARTE': 'SNI',
        'ST. CHARLES': 'STC',
        'SPRINGFIELD': 'LLC',
        'STELLE': 'STE'
    }

    @classmethod
    def get_station_code(cls, location: str) -> Optional[str]:
        return cls.STATION_MAPPING.get(location.upper())

    @classmethod
    def get_all_locations(cls) -> List[str]:
        return list(cls.STATION_MAPPING.keys())
    
    @staticmethod
    def get_all_mappings() -> dict:
        return dict(LocationMapper.STATION_MAPPING)