import os
import yaml
from eodag import setup_logging, EODataAccessGateway
import shapely
from datetime import datetime
from eodag_helpers import extract_min_bounding_box, extract_month

setup_logging(2)  # 3 for even more information

def fetch_landsat_candidates(dag, search_criteria, start_month, end_month): 
    """Gets landsat list according to specified search criteria using EODAG interface"""

    # Create gateway and get all possible images 
    all_products = dag.search_all(**search_criteria)

    # Add month as a filter property 
    for product in all_products:
        product.properties['in_range'] = extract_month(product, start_month, end_month)

    # Create shapely object for spatial filtering
    bbox = search_criteria["geom"]
    search_geometry = shapely.geometry.box(bbox["lonmin"], bbox["latmin"], bbox["lonmax"], bbox["latmax"])

    # Filter by cloud cover
    filtered_products = (
        all_products
        .filter_property(cloudCover=20, operator="lt")
        .filter_property(platformSerialIdentifier="LANDSAT_7", operator="ne")
        .filter_overlap(geometry=search_geometry, contains=True)
        .filter_property(in_range=1, operator='eq')
        )

    return(filtered_products)
    # filtered_prods_filepath = dag.serialize(filtered_products, filename=os.path.join(workspace, "filtered_products.geojson"))
    # restored_filtered_prods = dag.deserialize_and_register(filtered_prods_filepath)


workspace = 'landsat'
if not os.path.isdir(workspace):
    os.mkdir(workspace)

def read_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def main(config_path):
    config = read_config(config_path)

    bbox = extract_min_bounding_box(config['geodatabase_path'], config['layer_name'])
    search_criteria = {
        "productType": config['productType'],
        "start": config['start'],
        "end": config['end'],
        "geom": bbox
    }

    dag = EODataAccessGateway()
    filtered_products = fetch_landsat_candidates(dag, search_criteria, config['start_month'], config['end_month'])

    for product in filtered_products:
        product_path = dag.download(product)

if __name__ == "__main__":
    import sys
    config_file_path = sys.argv[1]  # Get config file path from command line argument
    main(config_file_path)

