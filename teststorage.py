import uuid
import argparse
import sys
import os
import statistics
from datetime import datetime, timedelta
from azure.storage.blob import BlockBlobService, PublicAccess

def generate_and_upload_chunk(blob_client, container_name, size_in_mb):
    rand_bytes = os.urandom(size_in_mb*1024*1024)
    blob_client.create_blob_from_bytes(blob_name=str(uuid.uuid1), container_name = container_name,blob=rand_bytes)

if __name__ == '__main__':
    chunk_size = 5
    num_chunks = 5
    num_runs = 2

    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, help="The size in MB of chunks to upload to blob storage. (Default: 5 MB)")
    parser.add_argument("--num-chunks", type=int, help="The number of random chunks to upload to blob storage.")
    parser.add_argument("--num-runs", type=int, help="The number of runs for this test.")
    args = parser.parse_args()

    if args.chunk_size is not None:
        chunk_size = args.chunk_size
    if args.num_chunks is not None:
        num_chunks = args.num_chunks
    if args.num_runs is not None:
        num_runs = args.num_runs

    connection_string = os.getenv("STORAGE_TEST_CONN_STRING","")

    if(connection_string is None):
        print("Please set STORAGE_TEST_CONN_STRING to a blob connection string.")
    else:
        blob_client = BlockBlobService(connection_string=connection_string)
        print("Uploading to account:", blob_client.account_name, "Endpoint:", blob_client.primary_endpoint) 
        container_name = (datetime.now().strftime("%Y%m%d%H%M%S"))
        print("Creating container name: ",container_name)
        blob_client.create_container(container_name)
        run_times = []
        for i in range(num_runs):
            chunk_times = []
            for j in range(num_chunks):
                start_time = datetime.now()
                generate_and_upload_chunk(blob_client, container_name, chunk_size)
                end_time = datetime.now()
                total_time = end_time-start_time
                chunk_times.append(total_time.total_seconds())
            run_times.append(sum(chunk_times))
            print("Run", str(i+1), "complete:")
            print("Total time:", str(run_times[i]), "seconds.")
            print("Median time per chunk:", str(statistics.median(chunk_times)), "seconds.")
            print("Mean time per chunk:", str(statistics.mean(chunk_times)), "seconds.")
        print("All runs complete:")
        print("Total time:", str(sum(run_times)), "seconds.")
        print("Median time per run:", str(statistics.median(run_times)), "seconds.")
        print("Mean time per run:", str(statistics.mean(run_times)), "seconds.")