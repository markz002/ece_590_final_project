from pystac_client import Client
from planetary_computer import sign
from typing import List, Dict, Any
import datetime

def get_mpc_landsat_pathrows(year: str = "2016", max_items: int = 10000) -> set:
    """
    Query available Landsat path/row combinations from Microsoft Planetary Computer (MPC) STAC API.
    
    Args:
        year: The target year in YYYY format (default: "2016")
        max_items: Maximum number of items to retrieve (default: 10000)
        
    Returns:
        A set of unique (path, row) tuples available on MPC for the specified year
    """
    # Connect to MPC STAC API
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    
    # Search parameters
    search = catalog.search(
        collections=["landsat-c2-l2"],
        datetime=f"{year}-01-01/{year}-12-31",
        max_items=max_items
    )
    
    # Extract unique path/row pairs
    path_row_set = set()
    for item in search.items():
        path = item.properties.get("landsat:wrs_path")
        row = item.properties.get("landsat:wrs_row")
        if path is not None and row is not None:
            path_row_set.add((int(path), int(row)))
            
    return path_row_set

def get_landsat_scenes(path: str, row: str, start_date: str = "2016-01-01", end_date: str = "2016-12-31", 
                       collection: str = "landsat-c2-l2", limit: int = 100) -> List[Dict[str, Any]]:
    """
    Retrieve Landsat scenes for specified path/row and date range from Microsoft Planetary Computer.
    Args:
        path: WRS-2 path number as string
        row: WRS-2 row number as string
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        collection: STAC collection name
        limit: Maximum number of items to return
        
    Returns:
        List of dictionaries containing scene metadata and signed asset URLs
    """
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    
    search = catalog.search(
        collections=[collection],
        query={
            "landsat:wrs_path": {"eq": int(path)},
            "landsat:wrs_row": {"eq": int(row)}
        },
        datetime=f"{start_date}/{end_date}",
        limit=limit
    )
    
    scenes = []
    for item in search.get_items():
        signed_item = sign(item)
        scene = {
            "id": item.id,
            "properties": dict(item.properties.items()),
            "assets": {key: asset.href for key, asset in signed_item.assets.items()}
        }
        scenes.append(scene)
    
    return scenes

def print_scene_info(scenes: List[Dict[str, Any]]) -> None:
    """Print formatted scene information"""
    print(f"\nFound {len(scenes)} records (Path {scenes[0]['properties']['landsat:wrs_path']}, " 
          f"Row {scenes[0]['properties']['landsat:wrs_row']})\n")
    
    for scene in scenes:
        print(f"\nScene ID: {scene['id']}")
        
        print("Properties:")
        for key, value in scene['properties'].items():
            print(f"  {key}: {value}")
            
        print("\nAssets:")
        for key, url in scene['assets'].items():
            print(f"  {key}: {url}")

if __name__ == "__main__":
    # Example usage
    year = "2016"
    pathrows = get_mpc_landsat_pathrows(year)
    
    # Print results
    print(f"Number of available Landsat Path/Row pairs on MPC for {year}: {len(pathrows)}\n")
    print("Path\tRow")
    for path, row in sorted(pathrows):
        print(f"{path:03d}\t{row:03d}")

    scenes = get_landsat_scenes(path="200", row="115", start_date="2016-01-01", end_date="2016-12-31") 
    print_scene_info(scenes)



