import fiona
from shapely.geometry import shape, box
from pyproj import Proj, Transformer
from datetime import datetime

def extract_min_bounding_box(geodatabase_path, layer_name, dest_crs='EPSG:4326'):
    """Extracts minimum bounding box from geodatabase"""
    
    # Initialize min and max values
    min_x, min_y, max_x, max_y = float('inf'), float('inf'), float('-inf'), float('-inf')

    # Open the file using Fiona
    with fiona.open(geodatabase_path, layer=layer_name) as layer:
        # Extract CRS from the layer
        if not layer.crs:
            raise ValueError("No CRS found in the layer")

        src_crs = layer.crs.to_string()
        
        # Create a transformer for coordinate conversion
        transformer = Transformer.from_crs(src_crs, dest_crs, always_xy=True)

        for feature in layer:
            geom = shape(feature['geometry'])

            # Update the bounding box
            bbox = geom.bounds # returns a tuple (minX, minY, maxX, maxY)
            min_x, min_y, max_x, max_y = min(min_x, bbox[0]), min(min_y, bbox[1]), max(max_x, bbox[2]), max(max_y, bbox[3])

    if min_x == float('inf'):
        raise ValueError("No valid geometries found in the layer")

    # Reproject the coordinates
    min_x, min_y = transformer.transform(min_x, min_y)
    max_x, max_y = transformer.transform(max_x, max_y)

    # Create a dictionary for the bounding box
    bounding_box = {
        'lonmin': min_x,
        'latmin': min_y,
        'lonmax': max_x,
        'latmax': max_y
    }

    return bounding_box

# Add month flag for filtering
def extract_month(product, start_month, end_month):
    """Adds a flag for whether a given product is within specified month range"""

    try:
        date_str = product.properties['startTimeFromAscendingNode']
        overpass_month = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ').month
        
        # Check if the month is within the specified range (inclusive)
        if start_month <= overpass_month <= end_month:
            return 1
        else:
            return 0
    except Exception as e:
        print(f"Error processing product: {e}")
        return -1  # Or any other value to indicate an error or non-match

