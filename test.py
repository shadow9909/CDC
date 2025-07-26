import psycopg2
import time

# Connect to source database
source_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='source_db',
    user='postgres',
    password='password'
)
source_conn.autocommit = True

# Connect to target database
target_conn = psycopg2.connect(
    host='localhost',
    port=5433,
    database='target_db',
    user='postgres',
    password='password'
)

def check_target_data():
    """Check what's in the target database"""
    with target_conn.cursor() as cursor:
        cursor.execute("SELECT id, name, email, synced_at FROM users ORDER BY id")
        users = cursor.fetchall()
        print(f"Target users: {users}")
        
        cursor.execute("SELECT id, user_id, product_name, amount, synced_at FROM orders ORDER BY id")
        orders = cursor.fetchall()
        print(f"Target orders: {orders}")

def test_cdc_step_by_step():
    """Test CDC operations with pauses to see each step"""
    with source_conn.cursor() as cursor:
        print("\n🔥 Testing CDC step by step...")
        
        # Step 1: INSERT
        print("\n1️⃣ Testing INSERT...")
        cursor.execute("""
            INSERT INTO users (name, email) 
            VALUES ('CDC Test User', 'cdc@example.com')
            RETURNING id;
        """)
        user_id = cursor.fetchone()[0]
        print(f"   ✅ Inserted user ID: {user_id} in SOURCE database")
        
        print("   ⏳ Waiting 3 seconds for CDC...")
        time.sleep(3)
        print("   📊 TARGET database after INSERT:")
        check_target_data()
        
        # Step 2: UPDATE
        print("\n2️⃣ Testing UPDATE...")
        cursor.execute("""
            UPDATE users SET name = 'CDC Updated User' 
            WHERE id = %s
        """, (user_id,))
        print(f"   ✅ Updated user ID: {user_id} in SOURCE database")
        
        print("   ⏳ Waiting 3 seconds for CDC...")
        time.sleep(3)
        print("   📊 TARGET database after UPDATE:")
        check_target_data()
        
        # Step 3: INSERT ORDER
        print("\n3️⃣ Testing INSERT order...")
        cursor.execute("""
            INSERT INTO orders (user_id, product_name, amount) 
            VALUES (%s, 'CDC Test Product', 456.78)
            RETURNING id;
        """, (user_id,))
        order_id = cursor.fetchone()[0]
        print(f"   ✅ Inserted order ID: {order_id} in SOURCE database")
        
        print("   ⏳ Waiting 3 seconds for CDC...")
        time.sleep(3)
        print("   📊 TARGET database after INSERT order:")
        check_target_data()
        
        # Step 4: DELETE ORDER ONLY (keep user)
        print("\n4️⃣ Testing DELETE order...")
        cursor.execute("DELETE FROM orders WHERE id = %s", (order_id,))
        print(f"   ✅ Deleted order ID: {order_id} from SOURCE database")
        
        print("   ⏳ Waiting 3 seconds for CDC...")
        time.sleep(3)
        print("   📊 TARGET database after DELETE order:")
        check_target_data()
        
        # Ask if user wants to delete the user too
        print(f"\n❓ Do you want to delete user ID {user_id} as well? (y/n)")
        response = input().lower().strip()
        
        if response == 'y':
            print("\n5️⃣ Testing DELETE user...")
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            print(f"   ✅ Deleted user ID: {user_id} from SOURCE database")
            
            print("   ⏳ Waiting 3 seconds for CDC...")
            time.sleep(3)
            print("   📊 TARGET database after DELETE user:")
            check_target_data()
        else:
            print(f"   ✅ Keeping user ID {user_id} in both databases")

def check_snapshot_data():
    """Check if snapshot data exists"""
    print("\n📸 Checking snapshot data...")
    with target_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM users WHERE id IN (1, 2)")
        snapshot_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM orders WHERE id IN (1, 2)")
        snapshot_orders = cursor.fetchone()[0]
        
        print(f"   Snapshot users (1,2): {snapshot_users}")
        print(f"   Snapshot orders (1,2): {snapshot_orders}")
        
        if snapshot_users == 0 and snapshot_orders == 0:
            print("   ⚠️  No snapshot data found. This might be expected if connector was reset.")

if __name__ == "__main__":
    print("🚀 CDC Step-by-Step Test")
    print("=" * 50)
    
    print("📊 Initial target database state:")
    check_target_data()
    
    check_snapshot_data()
    
    test_cdc_step_by_step()
    
    print("\n🎉 CDC test completed!")
    print("📊 Final target database state:")
    check_target_data()
