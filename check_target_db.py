import psycopg2

def check_target_database():
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='target_db',
        user='postgres',
        password='password'
    )
    
    with conn.cursor() as cursor:
        # Check all users (including historical data)
        cursor.execute("SELECT * FROM users ORDER BY id")
        users = cursor.fetchall()
        print("👥 All users in target database:")
        for user in users:
            print(f"  {user}")
        
        # Check all orders
        cursor.execute("SELECT * FROM orders ORDER BY id")
        orders = cursor.fetchall()
        print("\n📦 All orders in target database:")
        for order in orders:
            print(f"  {order}")
            
        # Check if there's snapshot data (the initial data)
        cursor.execute("SELECT id, name, email FROM users WHERE id IN (1, 2)")
        snapshot_users = cursor.fetchall()
        print(f"\n📸 Snapshot users (should have John Doe, Jane Smith): {snapshot_users}")
        
        cursor.execute("SELECT id, product_name FROM orders WHERE id IN (1, 2)")
        snapshot_orders = cursor.fetchall()
        print(f"📸 Snapshot orders (should have Laptop, Phone): {snapshot_orders}")
    
    conn.close()

if __name__ == "__main__":
    check_target_database()
