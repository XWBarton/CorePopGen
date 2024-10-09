import csv
import sys
import argparse
import requests

# Your What3Words API key
API_KEY = "YOP1YSTS"
BASE_URL = "https://api.what3words.com/v3"

def get_w3w_address(lat, lon):
    url = f"{BASE_URL}/convert-to-3wa"
    params = {
        "key": API_KEY,
        "coordinates": f"{lat},{lon}"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()['words']
    except requests.RequestException as e:
        return f"Error: {str(e)}"

def process_csv(input_file, output_file, lat_col, lon_col):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['What3Words Address']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for row in reader:
            try:
                lat = float(row[lat_col])
                lon = float(row[lon_col])
                w3w_address = get_w3w_address(lat, lon)
                row['What3Words Address'] = w3w_address
                writer.writerow(row)
                print(f"Processed: {lat}, {lon} -> {w3w_address}")
            except ValueError:
                print(f"Skipping row due to invalid lat/lon: {row}")
            except KeyError:
                print(f"Skipping row due to missing lat/lon columns: {row}")

def main():
    parser = argparse.ArgumentParser(description="Convert coordinates to what3words addresses")
    parser.add_argument("input", help="Input CSV file containing latitude and longitude")
    parser.add_argument("output", help="Output CSV file for results")
    parser.add_argument("--lat", help="Name of latitude column", default="Latitude")
    parser.add_argument("--lon", help="Name of longitude column", default="Longitude")
    args = parser.parse_args()

    process_csv(args.input, args.output, args.lat, args.lon)
    print(f"Processing complete. Results saved to {args.output}")

if __name__ == "__main__":
    main()