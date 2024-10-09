__version__ = "1.0.0"

import requests
from datetime import datetime

def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")
    else:
        print(f"Failed to download: {url}")

def main():
    print(f"silo_netCDF version {__version__}")
    base_url = "https://s3-ap-southeast-2.amazonaws.com/silo-open-data/Official/annual"
    variables = ["monthly_rain", "max_temp", "min_temp", "radiation", "rh_tmax"]

    print("Available variables:", ", ".join(variables))
    selected_vars = input("Enter variables (comma-separated): ").split(",")
    selected_vars = [var.strip() for var in selected_vars if var.strip() in variables]

    current_year = datetime.now().year
    start_year = int(input(f"Enter start year (1889-{current_year}): "))
    end_year = int(input(f"Enter end year ({start_year}-{current_year}): "))

    for year in range(start_year, end_year + 1):
        for variable in selected_vars:
            url = f"{base_url}/{variable}/{year}.{variable}.nc"
            filename = f"{year}.{variable}.nc"
            download_file(url, filename)
            

if __name__ == "__main__":
    main()