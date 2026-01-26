#!/usr/bin/env python3
"""
S3 Download Script
Downloads files from S3 using concurrent workers with configurable chunk size
"""

import os
import sys

import argparse
import boto3
import threading
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError


def download_from_s3(s3_key, destination_folder, num_workers=12, chunk_size_mb=64):
    """
    Download file from S3 using multiple workers for parallel download

    Args:
        s3_key: S3 object key (path in bucket)
        destination_folder: Local folder to save the file
        num_workers: Number of concurrent download workers (default: 12)
        chunk_size_mb: Chunk size in megabytes (default: 64)
    """
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('RUNPOD_S3_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('RUNPOD_S3_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('RUNPOD_S3_REGION'),
        endpoint_url=os.environ.get('RUNPOD_S3_ENDPOINT')
    )

    bucket = os.environ.get('RUNPOD_S3_BUCKET')

    if not bucket:
        raise Exception("S3_BUCKET environment variable not set")

    # Get filename from s3_key
    filename = os.path.basename(s3_key)
    destination_path = os.path.join(destination_folder, filename)

    print(f"Downloading from S3: s3://{bucket}/{s3_key}")
    print(f"Destination: {destination_path}")
    print(f"Workers: {num_workers}, Chunk size: {chunk_size_mb}MB")

    try:
        # Get object size
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        file_size = response['ContentLength']

        print(f"File size: {file_size} bytes ({file_size / (1024 * 1024):.2f} MB)")

        # Create destination directory
        os.makedirs(destination_folder, exist_ok=True)

        # Convert chunk size to bytes
        chunk_size = chunk_size_mb * 1024 * 1024

        # If file is smaller than chunk size, download directly
        if file_size < chunk_size:
            print(f"Small file, downloading directly...")
            s3_client.download_file(bucket, s3_key, destination_path)
            print(f"Successfully downloaded to {destination_path}")
            return

        # Calculate number of chunks
        num_chunks = (file_size + chunk_size - 1) // chunk_size
        actual_workers = min(num_workers, num_chunks)

        print(f"Downloading in {num_chunks} chunks using {actual_workers} workers")

        # Create temporary file parts storage
        parts = []
        lock = threading.Lock()

        def download_chunk(chunk_num, start_byte, end_byte):
            """Download a specific byte range"""
            try:
                byte_range = f"bytes={start_byte}-{end_byte}"
                response = s3_client.get_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Range=byte_range
                )

                data = response['Body'].read()

                with lock:
                    parts.append((chunk_num, data))
                    progress = (len(parts) / num_chunks) * 100
                    print(
                        f"Progress: {progress:.1f}% - Downloaded chunk {chunk_num + 1}/{num_chunks} ({len(data)} bytes)")

            except Exception as e:
                print(f"Error downloading chunk {chunk_num}: {str(e)}")
                raise

        # Download chunks in parallel
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = []
            for i in range(num_chunks):
                start_byte = i * chunk_size
                end_byte = min(start_byte + chunk_size - 1, file_size - 1)

                future = executor.submit(download_chunk, i, start_byte, end_byte)
                futures.append(future)

            # Wait for all downloads to complete
            for future in futures:
                future.result()

        # Sort parts and write to file
        parts.sort(key=lambda x: x[0])
        print(f"Writing {len(parts)} chunks to {destination_path}")

        with open(destination_path, 'wb') as f:
            for _, data in parts:
                f.write(data)

        print(f"Successfully downloaded to {destination_path}")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise Exception(f"File not found in S3: s3://{bucket}/{s3_key}")
        else:
            raise Exception(f"S3 error: {str(e)}")
    except Exception as e:
        # Clean up partial file on error
        if os.path.exists(destination_path):
            os.remove(destination_path)
            print(f"Cleaned up partial file: {destination_path}")
        raise Exception(f"Failed to download from S3: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description='Download files from S3 using concurrent workers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables required:
  S3_BUCKET              S3 bucket name
  S3_ACCESS_KEY_ID       S3 access key
  S3_SECRET_ACCESS_KEY   S3 secret key
  S3_REGION              S3 region
  S3_ENDPOINT            S3 endpoint URL (optional)

Example:
  python s3_download.py models/checkpoint.safetensors /workspace/models
  python s3_download.py file.bin /tmp --workers 8 --chunk-size 128
        """
    )

    parser.add_argument('s3_filename', help='S3 object key (path in bucket)')
    parser.add_argument('dest_folder', help='Destination folder path')
    parser.add_argument('--workers', type=int, default=12, help='Number of concurrent workers (default: 12)')
    parser.add_argument('--chunk-size', type=int, default=64, help='Chunk size in MB (default: 64)')

    args = parser.parse_args()

    # Validate environment variables
    required_vars = ['RUNPOD_S3_BUCKET', 'RUNPOD_S3_ACCESS_KEY_ID', 'RUNPOD_S3_SECRET_ACCESS_KEY', 'RUNPOD_S3_REGION']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    try:
        download_from_s3(
            s3_key=args.s3_filename,
            destination_folder=args.dest_folder,
            num_workers=args.workers,
            chunk_size_mb=args.chunk_size
        )
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()