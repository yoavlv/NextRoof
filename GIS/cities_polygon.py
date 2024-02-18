from tqdm import tqdm
import httpx
from .sql_gis import read_cities_to_dict

def recursive_flip(coords):
    if isinstance(coords, list):
        return [recursive_flip(coord) for coord in coords[::-1]]
    else:
        return coords

def get_city_polygon():
    city_code = read_cities_to_dict()
    city_polygon = {}
    for key, city in tqdm(city_code.items(), desc="Fetching City Polygons"):
        url = f'https://nominatim.openstreetmap.org/search?format=json&country=israel&city={city}&polygon_geojson=1'
        response = httpx.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            polygon_geojson = data[0].get('geojson') if data else None
            if polygon_geojson:
                try:
                    cords_fliped = recursive_flip(polygon_geojson['coordinates'])
                    city_polygon[key] = cords_fliped
                except Exception as e:
                    print(f"Error {city} polygon {e}")
    return city_polygon


