#!/usr/bin/env python
import boto3
import time
import argparse
import csv
import os

__DEFAULT_S3_OUTPUT_URL = "s3://foo"
__DEFAULT_PROFILE = "aberezin" if "AWS_PROFILE" not in os.environ else os.environ["AWS_PROFILE"]

def main():
    parser = argparse.ArgumentParser(description="Execute an Athena query and save the result as a CSV.")
    parser.add_argument('query', help='The Athena query to execute.')
    parser.add_argument('--database', default="stb_dev_tedm", help='The Athena database name. Default is stb_dev_tedm')
    parser.add_argument('--output-location', default=__DEFAULT_S3_OUTPUT_URL,
                        help=f'The S3 output location (e.g., s3://my-bucket/path  ). Default is {__DEFAULT_S3_OUTPUT_URL}.')
    parser.add_argument('--profile', default=__DEFAULT_PROFILE, help=f'The AWS CLI profile name to use.  Default is {__DEFAULT_PROFILE}')
    parser.add_argument('--output-file', default="out.csv", help='The local file path to save the result CSV. Default is "out.csv".')
    args = parser.parse_args()

    try:
        # Execute the query and get the result S3 URI
        s3_uri = execute_athena_query(
            query=args.query,
            database=args.database,
            output_location=ensure_no_trailing_slash(args.output_location),
            profile=args.profile
        )
        print(f"Query succeeded. Result saved in: {s3_uri}")

        # Download the CSV from S3
        download_csv_from_s3(
            s3_uri=s3_uri,
            profile=args.profile,
            output_file=args.output_file
        )
        print(f"CSV downloaded successfully to: {args.output_file}")
    except Exception as e:
        print(f"Error: {e}")


def execute_athena_query(query, database, output_location, profile):
    """
    Executes an Athena query and waits for the result to be available.

    :param query: The SQL query to execute.
    :param database: The Athena database to query.
    :param output_location: The S3 bucket location where query results are stored.
    :param profile: The AWS CLI profile to use.
    :return: The location of the result CSV file in S3.
    """
    # Use the specified AWS profile
    session = boto3.Session(profile_name=profile)
    client = session.client('athena')

    # Start the query execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait until the query completes
    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)  # Wait before polling again

    if status == 'SUCCEEDED':
        return f"{output_location}/{query_execution_id}.csv"
    else:
        raise Exception(f"Query {status}: {response['QueryExecution']['Status']['StateChangeReason']}")


def download_csv_from_s3(s3_uri, profile, output_file):
    """
    Downloads a CSV file from S3 and saves it locally.

    :param s3_uri: The S3 URI of the file.
    :param profile: The AWS CLI profile to use.
    :param output_file: Local file path to save the CSV.
    """
    session = boto3.Session(profile_name=profile)
    s3 = session.client('s3')

    s3_bucket, s3_key = parse_s3_uri(s3_uri)
    s3.download_file(s3_bucket, s3_key, output_file)


def parse_s3_uri(s3_uri):
    """
    Parses an S3 URI into bucket and key.

    :param s3_uri: The S3 URI (e.g., s3://my-bucket/path/to/file.csv).
    :return: A tuple of (bucket, key).
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'")
    parts = s3_uri[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError("Invalid S3 URI format.")
    return parts[0], parts[1]


def ensure_no_trailing_slash(s):
    return s.rstrip('/')


if __name__ == '__main__':
    main()
