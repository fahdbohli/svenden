from supabase_exporter import SupabaseExporter
import os

# Small test payload
opp = {
    'group_id': 'test-group-123',
    'sample': 'data',
}

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
if not url or not key:
    print('SUPABASE_URL or SUPABASE_KEY not set in environment')
    raise SystemExit(1)

exp = SupabaseExporter(url, key)
print('Attempting export...')
res = exp.export_opportunities([opp])
print('Result:', res)

print('Attempting storage upload test (small bytes)')
ok, resp = exp.upload_file_to_storage('uploads', 'test_prefix/test.txt', b'hello')
print('Upload ok:', ok, 'resp:', resp)
