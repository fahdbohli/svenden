#!/usr/bin/env python3
"""
Upload a local JSON file to Supabase Storage or (fallback) insert it into a Supabase table.

Usage:
  python supabase_upload_json.py --file settings/france.json [--bucket uploads] [--path france.json]

It reads SUPABASE_URL and SUPABASE_KEY from environment (use a .env file or export them).
"""
from supabase import create_client
from dotenv import load_dotenv
import os
import argparse
import json
from datetime import datetime
import sys


def upload_file_to_storage(client, bucket, remote_path, file_bytes):
    try:
        storage = client.storage
        # try upload
        resp = storage.from_(bucket).upload(remote_path, file_bytes)
        # supabase-py returns dict-like with 'error' key when something goes wrong
        if isinstance(resp, dict) and resp.get('error'):
            return False, resp
        return True, resp
    except Exception as e:
        return False, str(e)


def create_bucket_if_missing(client, bucket):
    try:
        return client.storage.create_bucket(bucket)
    except Exception as e:
        # creation might fail if permissions are missing
        return {'error': str(e)}


def insert_json_into_table(client, table_name, filename, json_obj):
    try:
        payload = {
            'filename': filename,
            'content': json_obj,
            'created_at': datetime.utcnow().isoformat()
        }
        res = client.table(table_name).insert(payload).execute()
        return True, res
    except Exception as e:
        return False, str(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', '-f', default='settings/france.json', help='Local JSON file to upload')
    parser.add_argument('--bucket', '-b', default='uploads', help='Supabase storage bucket to use')
    parser.add_argument('--path', '-p', default=None, help='Remote path/name in the bucket (defaults to the local filename)')
    parser.add_argument('--table', '-t', default='json_files', help='Fallback table name to insert JSON into')
    args = parser.parse_args()

    load_dotenv()

    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')

    if not url or not key:
        print('❌ SUPABASE_URL or SUPABASE_KEY not set in environment')
        sys.exit(2)

    if not os.path.exists(args.file):
        print(f'❌ Local file not found: {args.file}')
        sys.exit(3)

    filename = os.path.basename(args.file)
    remote_path = args.path or filename

    with open(args.file, 'rb') as fh:
        file_bytes = fh.read()

    # Try to parse JSON for the fallback insert
    try:
        with open(args.file, 'r', encoding='utf-8') as fh:
            json_obj = json.load(fh)
    except Exception:
        json_obj = None

    client = create_client(url, key)

    print(f"➡️ Trying to upload '{args.file}' to Supabase storage bucket '{args.bucket}' as '{remote_path}'...")
    ok, resp = upload_file_to_storage(client, args.bucket, remote_path, file_bytes)
    if ok:
        print('✅ Upload to storage successful')
        print(resp)
        sys.exit(0)

    print('⚠️ Upload to storage failed, attempting to create bucket if missing...')
    create_res = create_bucket_if_missing(client, args.bucket)
    if isinstance(create_res, dict) and create_res.get('error'):
        print('⚠️ Could not create bucket (permission or other issue):', create_res.get('error'))
    else:
        # Try upload again
        ok2, resp2 = upload_file_to_storage(client, args.bucket, remote_path, file_bytes)
        if ok2:
            print('✅ Upload to storage successful after bucket creation')
            print(resp2)
            sys.exit(0)

    # Fallback: insert into a table as JSON content
    if json_obj is None:
        print('❌ Fallback to table insert requires a valid JSON file (could not parse).')
        sys.exit(4)

    print(f"➡️ Falling back to inserting JSON into table '{args.table}' (column names: filename, content, created_at)...")
    ok3, resp3 = insert_json_into_table(client, args.table, filename, json_obj)
    if ok3:
        print('✅ Inserted JSON into table successfully')
        print(resp3.data if hasattr(resp3, 'data') else resp3)
        sys.exit(0)
    else:
        print('❌ Fallback insert failed:', resp3)
        sys.exit(5)


if __name__ == '__main__':
    main()
