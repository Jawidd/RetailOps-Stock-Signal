"""
Synthetic Retail Data Generator

Generates realistic retail sales, inventory, and supply chain data
for the RetailOps Companion project.

Usage:
    python generate_synthetic_data.py --days 180 --output ../data/synthetic/
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import argparse
from pathlib import Path
import uuid

# Set random seeds for reproducibility
np.random.seed(42)
random.seed(42)

class RetailDataGenerator:
    """Generates synthetic retail data with realistic business patterns."""
    
    def __init__(self, start_date='2024-07-01', days=180):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = self.start_date + timedelta(days=days)
        self.days = days
        self.date_range = pd.date_range(self.start_date, self.end_date, freq='D')
        
    def generate_products(self, n_products=200):
        """Generate product dimension with realistic categories and pricing."""
        
        categories = {
            'Electronics': {
                'subcategories': ['Laptops', 'Phones', 'Tablets', 'Accessories'],
                'cost_range': (50, 800),
                'margin': (1.2, 1.5)
            },
            'Apparel': {
                'subcategories': ['Shirts', 'Pants', 'Shoes', 'Accessories'],
                'cost_range': (10, 120),
                'margin': (1.8, 2.5)
            },
            'Home Goods': {
                'subcategories': ['Furniture', 'Kitchenware', 'Decor', 'Bedding'],
                'cost_range': (15, 300),
                'margin': (1.5, 2.2)
            },
            'Groceries': {
                'subcategories': ['Produce', 'Dairy', 'Meat', 'Packaged'],
                'cost_range': (2, 30),
                'margin': (1.3, 1.6)
            }
        }
        
        products = []
        product_id = 1
        
        for category, details in categories.items():
            # Distribute products across categories (roughly equal)
            n_in_category = n_products // len(categories)
            
            for _ in range(n_in_category):
                subcategory = random.choice(details['subcategories'])
                cost = round(np.random.uniform(*details['cost_range']), 2)
                margin = np.random.uniform(*details['margin'])
                price = round(cost * margin, 2)
                
                # Assign supplier (will create suppliers next)
                supplier_id = f"SUP{random.randint(1, 30):03d}"
                
                products.append({
                    'product_id': f"P{product_id:04d}",
                    'product_name': f"{category} - {subcategory} #{product_id}",
                    'category': category,
                    'subcategory': subcategory,
                    'unit_cost': cost,
                    'unit_price': price,
                    'supplier_id': supplier_id,
                    'is_active': True if np.random.random() > 0.05 else False
                })
                product_id += 1
        
        return pd.DataFrame(products)
    
    def generate_stores(self, n_stores=20):
        """Generate store dimension with realistic attributes."""
        
        regions = ['North', 'South', 'East', 'West']
        store_types = ['Flagship', 'Standard', 'Outlet']
        cities = [
            'Birmingham', 'London', 'Manchester', 'Leeds', 'Liverpool',
            'Bristol', 'Newcastle', 'Sheffield', 'Edinburgh', 'Glasgow'
        ]
        
        stores = []
        for i in range(1, n_stores + 1):
            region = random.choice(regions)
            store_type = random.choice(store_types)
            city = random.choice(cities)
            
            # Flagship stores are larger
            if store_type == 'Flagship':
                sq_footage = random.randint(30000, 50000)
            elif store_type == 'Standard':
                sq_footage = random.randint(10000, 25000)
            else:  # Outlet
                sq_footage = random.randint(5000, 12000)
            
            # Introduce data quality issue: 15% of stores missing region
            if np.random.random() < 0.15:
                region = None
            
            stores.append({
                'store_id': f"S{i:03d}",
                'store_name': f"{city} {store_type}",
                'region': region,
                'store_type': store_type,
                'sq_footage': sq_footage,
                'opened_date': (datetime(2018, 1, 1) + 
                               timedelta(days=random.randint(0, 1825))).date()
            })
        
        return pd.DataFrame(stores)
    
    def generate_suppliers(self, n_suppliers=30):
        """Generate supplier dimension with lead times and reliability."""
        
        countries = ['UK', 'China', 'Germany', 'USA', 'Italy', 'India']
        
        suppliers = []
        for i in range(1, n_suppliers + 1):
            country = random.choice(countries)
            
            # Lead time varies by country (realistic)
            if country in ['UK']:
                lead_time = random.randint(3, 7)
            elif country in ['Germany', 'Italy']:
                lead_time = random.randint(7, 14)
            else:
                lead_time = random.randint(14, 21)
            
            # On-time rate: some suppliers are more reliable
            on_time_rate = np.random.beta(8, 2)  # Skewed toward high reliability
            
            suppliers.append({
                'supplier_id': f"SUP{i:03d}",
                'supplier_name': f"Supplier {i} Ltd",
                'lead_time_days': lead_time,
                'on_time_rate': round(on_time_rate, 3),
                'country': country
            })
        
        return pd.DataFrame(suppliers)
    
    def generate_sales(self, products_df, stores_df):
        """Generate sales transactions with realistic patterns."""
        
        sales = []
        
        # Category sales velocity (transactions per day per store)
        category_velocity = {
            'Electronics': 2,
            'Apparel': 8,
            'Home Goods': 4,
            'Groceries': 20
        }
        
        for current_date in self.date_range:
            # Day-of-week effect: weekends are busier
            day_multiplier = 1.4 if current_date.weekday() >= 5 else 1.0
            
            # Seasonal effect for apparel (spring/fall peaks)
            month = current_date.month
            if month in [3, 4, 9, 10]:
                seasonal_multiplier = 1.3
            elif month in [1, 2, 7, 8]:
                seasonal_multiplier = 0.8
            else:
                seasonal_multiplier = 1.0
            
            # Holiday effect (November/December)
            if month in [11, 12]:
                holiday_multiplier = 1.5
            else:
                holiday_multiplier = 1.0
            
            # Random promotion days (15% of days)
            is_promo_day = np.random.random() < 0.15
            promo_multiplier = 1.35 if is_promo_day else 1.0
            
            for _, store in stores_df.iterrows():
                for category in category_velocity.keys():
                    # Get products in this category
                    cat_products = products_df[
                        (products_df['category'] == category) &
                        (products_df['is_active'] == True)
                    ]
                    
                    # Base number of transactions for this category
                    base_txns = category_velocity[category]
                    
                    # Apply multipliers
                    expected_txns = base_txns * day_multiplier * seasonal_multiplier
                    
                    # Apparel gets seasonal boost
                    if category == 'Apparel':
                        expected_txns *= seasonal_multiplier
                    
                    # Electronics get holiday boost
                    if category == 'Electronics':
                        expected_txns *= holiday_multiplier
                    
                    expected_txns *= promo_multiplier
                    
                    # Actual transactions (Poisson distribution)
                    n_txns = np.random.poisson(expected_txns)
                    
                    for _ in range(n_txns):
                        # Select random product from category
                        product = cat_products.sample(1).iloc[0]
                        
                        # Quantity sold (most sales are 1-2 units)
                        quantity = np.random.choice([1, 2, 3], p=[0.7, 0.2, 0.1])
                        
                        # Price (use current price, with occasional discount)
                        unit_price = product['unit_price']
                        
                        if is_promo_day and np.random.random() < 0.3:
                            # 30% of items on promo days are discounted
                            discount_pct = np.random.uniform(0.10, 0.30)
                            discount_amount = round(unit_price * quantity * discount_pct, 2)
                        else:
                            discount_amount = 0.0
                        

                        # Introduce data quality issue: 1% missing discount
                        if np.random.random() < 0.01:
                            discount_amount = None
                        
                        total_amount = round((unit_price * quantity) - (discount_amount or 0), 2)
                        
                        sales.append({
                            'sale_id': str(uuid.uuid4()),
                            'sale_date': current_date.date(),
                            'store_id': store['store_id'],
                            'product_id': product['product_id'],
                            'quantity_sold': quantity,
                            'unit_price': unit_price,
                            'discount_amount': discount_amount,
                            'total_amount': total_amount
                        })
        
        return pd.DataFrame(sales)
    
    def generate_inventory(self, products_df, stores_df, sales_df):
        """Generate daily inventory snapshots with realistic replenishment."""
        
        inventory_records = []
        
        # Initialize starting inventory for each store-product combo
        inventory_state = {}
        
        for _, store in stores_df.iterrows():
            for _, product in products_df[products_df['is_active'] == True].iterrows():
                store_id = store['store_id']
                product_id = product['product_id']
                category = product['category']
                
                # Initial stock levels based on category velocity
                if category == 'Groceries':
                    # High turnover: 7-14 days of stock
                    initial_stock = random.randint(50, 150)
                    reorder_point = random.randint(20, 40)
                elif category == 'Apparel':
                    # Medium turnover: 14-21 days
                    initial_stock = random.randint(30, 80)
                    reorder_point = random.randint(15, 30)
                elif category == 'Home Goods':
                    # Lower turnover: 21-30 days
                    initial_stock = random.randint(20, 50)
                    reorder_point = random.randint(10, 20)
                else:  # Electronics
                    # Slow turnover: 30-45 days
                    initial_stock = random.randint(10, 30)
                    reorder_point = random.randint(5, 10)
                
                inventory_state[(store_id, product_id)] = {
                    'quantity_on_hand': initial_stock,
                    'quantity_on_order': 0,
                    'reorder_point': reorder_point,
                    'last_restock_date': None
                }
        
        # Simulate inventory changes day by day
        for current_date in self.date_range:
            # Get sales for this day
            daily_sales = sales_df[sales_df['sale_date'] == current_date.date()]
            
            # Group by store and product
            sales_summary = daily_sales.groupby(['store_id', 'product_id']).agg({
                'quantity_sold': 'sum'
            }).reset_index()
            
            # Update inventory for each store-product
            for key, state in inventory_state.items():
                store_id, product_id = key
                
                # Reduce stock by sales
                sales_row = sales_summary[
                    (sales_summary['store_id'] == store_id) &
                    (sales_summary['product_id'] == product_id)
                ]
                
                if not sales_row.empty:
                    sold = sales_row.iloc[0]['quantity_sold']
                    state['quantity_on_hand'] = max(0, state['quantity_on_hand'] - sold)
                
                # Receive shipments (simplified: instant delivery for now)
                if state['quantity_on_order'] > 0 and np.random.random() < 0.1:
                    # 10% chance per day of receiving order
                    state['quantity_on_hand'] += state['quantity_on_order']
                    state['quantity_on_order'] = 0
                    state['last_restock_date'] = current_date.date()
                
                # Check if reorder needed
                if (state['quantity_on_hand'] <= state['reorder_point'] and 
                    state['quantity_on_order'] == 0):
                    # Place order
                    order_quantity = state['reorder_point'] * 3  # Order 3x reorder point
                    state['quantity_on_order'] = order_quantity
                
                # Introduce data quality issue: 0.5% negative inventory (system error)
                quantity_on_hand = state['quantity_on_hand']
                if np.random.random() < 0.005:
                    quantity_on_hand = -random.randint(1, 5)
                
                # Record snapshot
                inventory_records.append({
                    'snapshot_date': current_date.date(),
                    'store_id': store_id,
                    'product_id': product_id,
                    'quantity_on_hand': quantity_on_hand,
                    'quantity_on_order': state['quantity_on_order'],
                    'reorder_point': state['reorder_point'],
                    'last_restock_date': state['last_restock_date']
                })
        
        return pd.DataFrame(inventory_records)
    
    def generate_shipments(self, products_df, stores_df, suppliers_df, inventory_df):
        """Generate shipment records with realistic lead times and delays."""
        
        shipments = []
        shipment_id = 1
        
        # Find reorder events from inventory (when quantity_on_order changes)
        # Simplified: generate shipment when inventory drops below reorder point
        
        for _, store in stores_df.iterrows():
            for _, product in products_df[products_df['is_active'] == True].iterrows():
                store_id = store['store_id']
                product_id = product['product_id']
                
                # Get inventory history for this store-product
                inv_history = inventory_df[
                    (inventory_df['store_id'] == store_id) &
                    (inventory_df['product_id'] == product_id)
                ].sort_values('snapshot_date')
                
                # Detect reorder points (when quantity_on_order becomes > 0)
                prev_on_order = 0
                for _, inv_row in inv_history.iterrows():
                    current_on_order = inv_row['quantity_on_order']
                    
                    if current_on_order > prev_on_order:
                        # A new order was placed
                        order_date = inv_row['snapshot_date']
                        quantity_ordered = current_on_order
                        
                        # Get supplier info
                        supplier = suppliers_df[
                            suppliers_df['supplier_id'] == product['supplier_id']
                        ].iloc[0]
                        
                        # Calculate expected delivery
                        lead_time = int(supplier['lead_time_days'])
                        expected_date = pd.to_datetime(order_date) + timedelta(days=lead_time)
                        
                        # Actual delivery (may be late based on supplier reliability)
                        if np.random.random() > supplier['on_time_rate']:
                            # Late delivery
                            delay_days = random.randint(1, 7)
                            received_date = expected_date + timedelta(days=delay_days)
                            is_late = True
                        else:
                            received_date = expected_date
                            is_late = False
                        
                        # Occasionally receive less than ordered (1% of time)
                        if np.random.random() < 0.01:
                            quantity_received = quantity_ordered - random.randint(1, 5)
                            quantity_received = max(0, quantity_received)
                        else:
                            quantity_received = quantity_ordered
                        
                        # Only add if received date is within our date range
                        if received_date <= self.end_date:
                            shipments.append({
                                'shipment_id': f"SHIP{shipment_id:05d}",
                                'order_date': order_date,
                                'expected_date': expected_date.date(),
                                'received_date': received_date.date(),
                                'store_id': store_id,
                                'product_id': product_id,
                                'supplier_id': supplier['supplier_id'],
                                'quantity_ordered': int(quantity_ordered),
                                'quantity_received': int(quantity_received),
                                'is_late': is_late
                            })
                            shipment_id += 1
                    
                    prev_on_order = current_on_order
        
        # Introduce duplicates (1% of shipments)
        df = pd.DataFrame(shipments)
        if len(df) > 0:
            n_duplicates = int(len(df) * 0.01)
            duplicates = df.sample(n=n_duplicates, replace=True)
            df = pd.concat([df, duplicates], ignore_index=True)
        
        return df
    
    def generate_all(self, output_dir='../data/synthetic/'):
        """Generate all datasets and save to CSV files."""
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print("Generating synthetic retail data...")
        print(f"Date range: {self.start_date.date()} to {self.end_date.date()}")
        print(f"Duration: {self.days} days\n")
        
        # Generate dimensions
        print("Generating products...")
        products_df = self.generate_products(n_products=200)
        print(f"  ✓ Generated {len(products_df)} products")
        
        print("Generating stores...")
        stores_df = self.generate_stores(n_stores=20)
        print(f"  ✓ Generated {len(stores_df)} stores")
        
        print("Generating suppliers...")
        suppliers_df = self.generate_suppliers(n_suppliers=30)
        print(f"  ✓ Generated {len(suppliers_df)} suppliers")
        
        # Generate facts (this takes longer)
        print("\nGenerating sales transactions...")
        sales_df = self.generate_sales(products_df, stores_df)
        print(f"  ✓ Generated {len(sales_df)} sales transactions")
        
        print("Generating inventory snapshots...")
        inventory_df = self.generate_inventory(products_df, stores_df, sales_df)
        print(f"  ✓ Generated {len(inventory_df)} inventory records")
        
        print("Generating shipments...")
        shipments_df = self.generate_shipments(products_df, stores_df, suppliers_df, inventory_df)
        print(f"  ✓ Generated {len(shipments_df)} shipments")
        
        # Save to CSV
        print("\nSaving files...")
        products_df.to_csv(output_path / 'products.csv', index=False)
        stores_df.to_csv(output_path / 'stores.csv', index=False)
        suppliers_df.to_csv(output_path / 'suppliers.csv', index=False)
        sales_df.to_csv(output_path / 'sales.csv', index=False)
        inventory_df.to_csv(output_path / 'inventory.csv', index=False)
        shipments_df.to_csv(output_path / 'shipments.csv', index=False)
        
        print(f"\n✓ All files saved to {output_path}")
        
        # Print summary statistics
        print("\n" + "="*60)
        print("DATA SUMMARY")
        print("="*60)
        print(f"Products: {len(products_df)}")
        print(f"Stores: {len(stores_df)}")
        print(f"Suppliers: {len(suppliers_df)}")
        print(f"Sales transactions: {len(sales_df):,}")
        print(f"Inventory snapshots: {len(inventory_df):,}")
        print(f"Shipments: {len(shipments_df):,}")
        print(f"\nTotal revenue: ${sales_df['total_amount'].sum():,.2f}")
        print(f"Average transaction: ${sales_df['total_amount'].mean():.2f}")
        print(f"Date range: {sales_df['sale_date'].min()} to {sales_df['sale_date'].max()}")
        
        # Data quality issues summary
        print("\n" + "="*60)
        print("DATA QUALITY ISSUES (Intentional)")
        print("="*60)
        null_discounts = sales_df['discount_amount'].isna().sum()
        print(f"Sales with missing discount_amount: {null_discounts} ({null_discounts/len(sales_df)*100:.2f}%)")
        
        negative_inventory = (inventory_df['quantity_on_hand'] < 0).sum()
        print(f"Inventory records with negative quantity: {negative_inventory} ({negative_inventory/len(inventory_df)*100:.2f}%)")
        
        null_regions = stores_df['region'].isna().sum()
        print(f"Stores with missing region: {null_regions} ({null_regions/len(stores_df)*100:.2f}%)")
        
        duplicate_shipments = len(shipments_df) - len(shipments_df.drop_duplicates(subset=['order_date', 'store_id', 'product_id']))
        print(f"Duplicate shipments: {duplicate_shipments} (~1%)")
        
        print("\n" + "="*60)
        
        return {
            'products': products_df,
            'stores': stores_df,
            'suppliers': suppliers_df,
            'sales': sales_df,
            'inventory': inventory_df,
            'shipments': shipments_df
        }


def main():
    parser = argparse.ArgumentParser(description='Generate synthetic retail data')
    parser.add_argument('--days', type=int, default=180, 
                       help='Number of days to generate (default: 180)')
    parser.add_argument('--start-date', type=str, default='2024-07-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, default='../data/synthetic/',
                       help='Output directory')
    
    args = parser.parse_args()
    
    generator = RetailDataGenerator(start_date=args.start_date, days=args.days)
    generator.generate_all(output_dir=args.output)


if __name__ == '__main__':
    main()