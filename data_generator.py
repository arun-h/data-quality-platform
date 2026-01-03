from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
import os

fake = Faker()

def generate_orders(start_date, num_days=90):
    for day in range(num_days):
        dt = (start_date + timedelta(days=day)).strftime('%Y-%m-%d')
        
        # Generate 10-20K orders per day
        orders = []
        for _ in range(15000):
            orders.append({
                'order_id': fake.uuid4(),
                'user_id': fake.random_int(1, 10000),
                'product_id': fake.random_int(1, 1000),
                'amount': round(fake.random.uniform(10, 500), 2),
                'order_date': dt,
                'dt': dt
            })
        
        df = pd.DataFrame(orders)
        
        # Write as partitioned parquet
        path = f"data/orders_raw/dt={dt}"
        os.makedirs(path, exist_ok=True)
        df.to_parquet(f"{path}/data.parquet", index=False)
        
        print(f"Generated {len(orders)} orders for {dt}")
def generate_products(num_products=1000):
    """Generate product catalog"""
    products = []
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys']
    
    for product_id in range(1, num_products + 1):
        products.append({
            'product_id': product_id,
            'name': fake.catch_phrase(),
            'category': fake.random_element(categories),
            'price': round(fake.random.uniform(10, 500), 2)
        })
    
    df = pd.DataFrame(products)
    
    os.makedirs('data/products', exist_ok=True)
    df.to_parquet('data/products/data.parquet', index=False)
    
    print(f"Generated {len(products)} products")
    return df


#generate_orders(datetime(2024, 10, 1))

generate_products()