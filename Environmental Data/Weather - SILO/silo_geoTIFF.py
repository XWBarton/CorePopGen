__version__ = "0.1.0"

import requests
import os
from datetime import datetime

def download_silo_data(variable, year, month):
    base_url = "https://s3-ap-southeast-2.amazonaws.com/silo-open-data/Official/monthly"
    filename = f"{year}{month:02d}.{variable}.tif"
    url = f"{base_url}/{variable}/{year}/{filename}"

    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)
        print(f"Successfully downloaded {filename}")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")

def get_year_range():
    while True:
        try:
            start_year = int(input("Enter the start year (YYYY): "))
            end_year = int(input("Enter the end year (YYYY): "))
            if start_year > end_year:
                print("Start year must be less than or equal to end year.")
                continue
            if len(str(start_year)) != 4 or len(str(end_year)) != 4:
                print("Years must be in YYYY format.")
                continue
            return list(range(start_year, end_year + 1))
        except ValueError:
            print("Invalid input. Please enter valid years.")

def get_months():
    while True:
        months_input = input("Enter the month(s) (1-12) separated by commas: ")
        months = [m.strip() for m in months_input.split(',')]
        if all(m.isdigit() and 1 <= int(m) <= 12 for m in months):
            return [int(m) for m in months]
        print("Invalid input. Please enter valid months (1-12).")

def main():
    print(f"silo_geoTIFF version {__version__}")
    variables = ["monthly_rain", "max_temp", "min_temp", "radiation", "rh_tmax"]
    
    print("Available variables:")
    for i, var in enumerate(variables, 1):
        print(f"{i}. {var}")
    
    while True:
        try:
            variable_choice = int(input("Enter the number of the variable you want to download: ")) - 1
            if 0 <= variable_choice < len(variables):
                variable = variables[variable_choice]
                break
            print("Invalid choice. Please select a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")
    
    years = get_year_range()
    months = get_months()
    
    for year in years:
        for month in months:
            download_silo_data(variable, str(year), month)

if __name__ == "__main__":
    main()