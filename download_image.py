import os
import pandas as pd
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import time

def download_image(url, product_name, nrn, output_folder):
    """Download an image from a URL and save it with the specified naming convention"""
    if not url or pd.isna(url):
        return None
    
    try:
        # Get the file extension from the URL
        parsed = urlparse(url)
        ext = os.path.splitext(parsed.path)[1]
        
        # Clean the product name and NRN for filename
        safe_product_name = "".join([c for c in str(product_name) if c.isalpha() or c.isdigit() or c in (' ', '-', '_')]).rstrip()
        safe_product_name = safe_product_name.replace(' ', '_')
        safe_nrn = str(nrn).strip()
        
        # Create filename
        filename = f"{safe_product_name}_{safe_nrn}{ext}"
        filepath = os.path.join(output_folder, filename)
        
        # Download the image
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        
        return filename
    
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return None

def process_row(row, output_folder):
    """Process a single row to download image and return updated image filename"""
    product_name = row['ProductName']
    nrn = row['NRN']
    image_url = row['Image']
    
    new_filename = download_image(image_url, product_name, nrn, output_folder)
    return new_filename if new_filename else image_url

def main():
    # Configuration
    input_excel = 'druginput.xlsx'  # Change to your input Excel filename
    output_excel = 'drugoutput.xlsx'  # Output Excel filename
    image_folder = 'image'  # Folder to save downloaded images
    max_workers = 5  # Number of concurrent downloads
    
    # Create image folder if it doesn't exist
    os.makedirs(image_folder, exist_ok=True)
    
    # Read the Excel file
    df = pd.read_excel(input_excel, engine='openpyxl')
    
    # Download images and update the Image column
    print("Starting image downloads...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Prepare arguments for each row
        args = [(row, image_folder) for _, row in df.iterrows()]
        
        # Process rows in parallel
        results = list(executor.map(lambda x: process_row(*x), args))
    
    # Update the DataFrame with the new filenames
    df['Image'] = results
    
    # Save the updated Excel file
    df.to_excel(output_excel, index=False, engine='openpyxl')
    
    # Print summary
    downloaded_count = sum(1 for x in results if not str(x).startswith('http'))
    print(f"\nFinished in {time.time() - start_time:.2f} seconds")
    print(f"Total records processed: {len(df)}")
    print(f"Successfully downloaded images: {downloaded_count}")
    print(f"Failed downloads: {len(df) - downloaded_count}")
    print(f"Updated Excel file saved to: {output_excel}")
    print(f"Images saved to: {image_folder}")

if __name__ == "__main__":
    main()