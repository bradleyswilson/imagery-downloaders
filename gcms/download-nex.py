import os
import subprocess
import logging
import pandas as pd
import numpy as np
import multiprocessing

def setup_logger(log_file='download_errors.log'):
    logger = logging.getLogger('nex_downloader')
    logger.setLevel(logging.ERROR)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.ERROR)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logger()

def download_nex_files(params):
    gcm, grid_size, ensemble, scenario, var, minx, maxx, miny, maxy, year = params
    base_url = "https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6"
    url = (f"{base_url}/{gcm}/{scenario}/"
                    f"{ensemble}/{var}/{var}_day_{gcm}_{scenario}"
                    f"_{ensemble}_{grid_size}_{year}.nc"
                    f"?var={var}&north={maxy}&west={minx}&east={maxx}&south={miny}&"
                    f"horizStride=1&time_start={year}-01-01T12:00:00Z&time_end={year}-12-31T12:00:00Z"
                    f"&&&accept=netcdf3&addLatLon=true"
                    )
    
    new_filename = f"{var}_day_{gcm}_{scenario}_{ensemble}_{grid_size}_{year}.nc"

    folder_path = os.path.join('data', gcm, scenario, var)
    os.makedirs(folder_path, exist_ok=True)
    
    full_file_path = os.path.join(folder_path, new_filename)
    
    # Check if file already exists
    if os.path.exists(full_file_path) and os.path.getsize(full_file_path) > 0:
        logger.info(f"File already exists: {full_file_path}")
        return

    wget_command = [
        "wget",
         "--wait=1", 
        "--random-wait",
        "--limit-rate=50k",
        "--timeout=30",
        "--waitretry=5",
        "--tries=2",
        "-O", 
        full_file_path,
        url
    ]
    
    try:
        subprocess.run(wget_command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        error_message = f"Error downloading {full_file_path}: {e}\nCommand output: {e.output}"
        logger.error(error_message)
        return

def find_nasanex_filename(gcm, scenario):
    """
    Load list of NASA-NEX files downloaded from their docs. We will use it to create
    the catalog of available datasets. Largely this is used to filter out the GCMs
    that don't have tasmax available.
    """
    template_filename = nasa_nex_runs_df[
        (nasa_nex_runs_df["GCM"] == gcm)
        & (nasa_nex_runs_df["scenario"] == scenario)
        & (nasa_nex_runs_df["variable"] == "tasmax")
    ]["file_name"].iloc[0]
    (
        _variable,
        _timestep,
        _gcm,
        _scenario,
        ensemble_member,
        grid_code,
        _yearnc,
    ) = template_filename.split("_")
    return ensemble_member, grid_code

def main():
    # Load the DataFrame
    df = pd.read_csv(
        "s3://carbonplan-climate-impacts/extreme-heat/v1.0/inputs/nex-gddp-cmip6-files.csv"
    )

    global nasa_nex_runs_df
    nasa_nex_runs_df = pd.DataFrame([run.split("/") for run in df[" fileURL"].values]).drop(
        [0, 1, 2, 3], axis=1
    )

    nasa_nex_runs_df.columns = [
        "GCM",
        "scenario",
        "ensemble_member",
        "variable",
        "file_name",
    ]

    variables = ["tasmax", "tasmin", "tas", "huss"]
    gcm_list = [
        "ACCESS-CM2",
        "ACCESS-ESM1-5",
        "BCC-CSM2-MR",
        "CanESM5",
        "CMCC-CM2-SR5",
        "CMCC-ESM2",
        "CNRM-CM6-1",
        "CNRM-ESM2-1",
        "EC-Earth3-Veg-LR",
        "EC-Earth3",
        "FGOALS-g3",
        "GFDL-CM4",
        "GFDL-ESM4",
        "GISS-E2-1-G",
        "HadGEM3-GC31-LL",
        "INM-CM4-8",
        "INM-CM5-0",
        "KACE-1-0-G",
        "KIOST-ESM",
        "MIROC-ES2L",
        "MPI-ESM1-2-HR",
        "MPI-ESM1-2-LR",
        "MRI-ESM2-0",
        "NorESM2-LM",
        "NorESM2-MM",
        "UKESM1-0-LL",
    ]
    scenario_years = {"historical": np.arange(1985, 2014),
                    "ssp245": np.arange(2015, 2100),
                    "ssp585": np.arange(2015, 2100)
                    }

    # Prepare parameters for parallel processing
    all_params = []
    for gcm in gcm_list:
        for variable in variables:
            for scenario, years in scenario_years.items():
                ensemble_member, grid_code = find_nasanex_filename(gcm, scenario)
                for year in years:
                    params = (gcm, grid_code, ensemble_member, scenario, variable, -86.6, -86.5, 39, 39.5, year)
                    all_params.append(params)

    # Set up multiprocessing pool
    ctx = multiprocessing.get_context('spawn')
    #num_cores = ctx.cpu_count()
    num_cores=1
    pool = ctx.Pool(processes=num_cores)

    # Run downloads in parallel
    pool.map(download_nex_files, all_params)

    # Close the pool
    pool.close()
    pool.join()

    print("All downloads completed.")

if __name__ == "__main__":
    main()
