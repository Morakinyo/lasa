import os
import requests
from urllib.parse import urlparse
from pathlib import Path

# List of JPEG URLs (replace these with your actual URLs)
jpeg_urls = [
    "https://example.com/image1.jpeg",
    "https://example.com/image2.jpeg",
    # ... add more URLs here
]

# Directory to save the images
output_dir = "downloaded_images"
os.makedirs(output_dir, exist_ok=True)

def get_filename_from_url(url, index):
    """Generate a unique filename from the URL."""
    parsed = urlparse(url)
    base_name = os.path.basename(parsed.path)
    if not base_name.lower().endswith(".jpeg"):
        base_name = f"image_{index}.jpeg"
    return f"{index:04d}_{base_name}"

# Download loop
for i, url in enumerate(jpeg_urls, start=1):
    try:
        print(f"Downloading {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        filename = get_filename_from_url(url, i)
        file_path = os.path.join(output_dir, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"Saved to {file_path}")
    except requests.RequestException as e:
        print(f"Failed to download {url}: {e}")
