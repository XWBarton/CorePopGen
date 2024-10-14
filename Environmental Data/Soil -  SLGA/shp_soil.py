def process_geotiff(geotiff_path, gdf_reprojected):
    try:
        geotiff_file = os.path.basename(geotiff_path)
        logging.info(f"Processing: {geotiff_file}")
        
        with rasterio.open(geotiff_path) as src:
            nodata = src.nodata
            results = []
            for idx, row in gdf_reprojected.iterrows():
                geom = row.geometry
                try:
                    out_image, out_transform = mask(src, [geom], crop=True)
                    data = out_image[0]  # Assuming single band data
                    valid_data = data[data != nodata] if nodata is not None else data
                    valid_data = valid_data[np.isfinite(valid_data)]  # Remove inf and nan
                    
                    if len(valid_data) > 0:
                        mean = np.mean(valid_data)
                        median = np.median(valid_data)
                        min_val = np.min(valid_data)
                        max_val = np.max(valid_data)
                    else:
                        mean = median = min_val = max_val = None
                    
                    results.append({
                        'geometry_id': idx,
                        'mean': mean,
                        'median': median,
                        'min': min_val,
                        'max': max_val
                    })
                except Exception as e:
                    logging.error(f"Error processing geometry {idx} in {geotiff_file}: {e}")
            
            df = pd.DataFrame(results)
            output_csv = f"{os.path.splitext(geotiff_file)[0]}_results.csv"
            df.to_csv(output_csv, index=False)
            logging.info(f"Results saved to {output_csv}")
    except Exception as e:
        logging.error(f"Error processing {geotiff_file}: {e}")