import argparse
import os
import requests
from tqdm import tqdm
from urllib.parse import urlparse, urljoin

__version__ = "1.0.0"

# Dictionary mapping original file names to reader-friendly names
file_name_mapping = {
    "AWC_000_005_EV_N_P_AU_TRN_N_20210614.tif": "Available_Water_Capacity_0-5cm.tif",
    "CFG_000_005_EV_N_P_AU_TRN_N_20221006_Dominant_Class.tif": "Coarse_Fragments_0-5cm.tif",
    "CLY_000_005_EV_N_P_AU_TRN_N_20210902.tif": "Clay_Percentage_0-5cm.tif",
    "NTO_000_005_EV_N_P_AU_NAT_C_20140801.tif": "Total_Nitrogen_Percentage_0-5cm.tif",
    "pHc_000_005_EV_N_P_AU_NAT_C_20140801.tif": "pH_CaCl2_0-5cm.tif",
    "PTO_000_005_EV_N_P_AU_NAT_C_20140801.tif": "Total_Phosphorus_Percentage_0-5cm.tif",
    "SLT_000_005_EV_N_P_AU_TRN_N_20210902.tif": "Silt_Percentage_0-5cm.tif",
    "SND_000_005_EV_N_P_AU_TRN_N_20210902.tif": "Sand_Percentage_0-5cm.tif",
    "SOC_000_005_EV_N_P_AU_TRN_N_20220727.tif": "Organic_Carbon_Percentage_0-5cm.tif",
    "AVP_000_005_EV_N_P_AU_TRN_N_20220826.tif": "Available_Phosphorus_mg-kg_0-5cm.tif"
}

def download_raster(url, output_folder, api_key, timeout=300):
    try:
        # Construct the URL with the API key
        parsed_url = urlparse(url)
        auth_url = f"{parsed_url.scheme}://apikey:{api_key}@{parsed_url.netloc}{parsed_url.path}"
        
        # First, try to get the file size and follow redirects
        with requests.get(auth_url, stream=True, allow_redirects=True, timeout=timeout) as r:
            r.raise_for_status()
            file_size = int(r.headers.get('content-length', 0))
            final_url = r.url  # Get the final URL after redirects
            
            original_filename = os.path.basename(urlparse(final_url).path)
            friendly_filename = file_name_mapping.get(original_filename, original_filename)
            output_path = os.path.join(output_folder, friendly_filename)
            
            with open(output_path, 'wb') as f, tqdm(
                desc=friendly_filename,
                total=file_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as progress_bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    progress_bar.update(size)
        
        return friendly_filename
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def main():
    print(f"download_soil.py version {__version__}")
    parser = argparse.ArgumentParser(description="Download raster data from SGLA")
    parser.add_argument('--output', default='.', help='Output folder for downloaded files')
    parser.add_argument('--apikey', required=True, help='API key for TERN data access')
    args = parser.parse_args()

    base_url = "https://data.tern.org.au/landscapes/slga/NationalMaps/SoilAndLandscapeGrid/"
    rasters = list(file_name_mapping.keys())

    # Ensure output directory exists
    os.makedirs(args.output, exist_ok=True)

    # Download rasters
    for raster in rasters:
        url = urljoin(base_url, raster.split('_')[0] + '/' + raster)
        print(f"Attempting to download: {url}")
        filename = download_raster(url, args.output, args.apikey)
        if filename:
            print(f"Successfully downloaded: {filename}")
        else:
            print(f"Failed to download from: {url}")

if __name__ == "__main__":
    main()