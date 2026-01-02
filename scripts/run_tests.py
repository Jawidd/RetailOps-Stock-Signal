import duckdb


warehouse_db= '../warehouse/favorita.duckdb'
testfile= '../warehouse/tests/test_fct_store_item_daily_sales_quality.sql'

print("DATA QUALITY TESTS")

db = duckdb.connect(warehouse_db)

with open(testfile, 'r') as f:
    test_sql = f.read()

results = db.execute(test_sql).fetchall()

if len(results) == 0:
    print("✓ All tests passed")

else: 
    print("✗ Some tests failed, issues found:")
    for test_name,fail in results:
        print(f" - {test_name}: {fail}")
    print("\n Total failed tests:", len(results))

    exit(1)
db.close()

