import os
import subprocess

def download_loca2_files(model, region, grid_size, ensemble, ssp, var, start_year=2015, end_year=2044):
    base_url = "https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split"
    url = f"{base_url}/{model}/{region}/{grid_size}/{ensemble}/{ssp}/{var}/"
    
    file_pattern = f"{var}.{model}.{ssp}.{ensemble}.{start_year}-{end_year}.LOCA_16thdeg_v20220413.{region}.nc"
    
    wget_command = [
        "wget",
        "-r",  # recursive download
        "-np",  # no parent
        "-nd",  # no directories
        "-A", file_pattern,  # accept only files matching this pattern
        url
    ]
    
    try:
        subprocess.run(wget_command, check=True)
        print(f"Successfully downloaded files matching: {file_pattern}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while downloading: {e}")

# Usage example
download_loca2_files(
    model="EC-Earth3",
    region="cent",
    grid_size="0p0625deg",
    ensemble="r1i1p1f1",
    ssp="historical",
    var="tasmax",
    start_year=1950,
    end_year=2014
)