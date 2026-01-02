import duckdb
import pandas as pd


warehouse_db= '../warehouse/favorita.duckdb'

db = duckdb.connect(warehouse_db)

print("ANALYZING STORE DAILY SALES DATA")



# Overall sales summary
print("\n1.=== Overall Sales Summary ===")
summary = db.execute("""
    select 
        count(distinct sales_date) as total_days,
        count(distinct store_nbr) as total_stores,
        sum(sold_units) as total_units_sold,
        sum(total_unit_sales) as total_sales_amount,
        sum(transactions_count) as total_transactions,
        avg(sold_units) as avg_daily_units_sold_per_store,
        avg(total_unit_sales) as avg_daily_sales_amount_per_store
    from fact.fct_store_daily_sales
""").fetchdf()
print(summary)
# print(summary.to_string(index=False))


# top stores
print("\n2=== Top 10 Stores by Total Sales ===")
top_stores = db.execute("""
    select 
        store_nbr,
        city,
        state,
        store_type,
        sum(total_unit_sales) as total_sales_amount,
        sum(sold_units) as total_units_sold,
        sum(transactions_count) as total_transactions,
        avg(total_unit_sales) as avg_daily_sales_amount,
        avg(sold_units) as avg_daily_units_sold
    from fact.fct_store_daily_sales
    group by store_nbr , city, state, store_type
    order by total_sales_amount desc
    limit 10
""").fetchdf()
print(top_stores)


#weekend vs weekday sales if(day_of_week in (6,7) then weekend else weekday)
print("\n3=== Weekend vs Weekday Sales Summary ===")
weekend_weekday_summary = db.execute("""
    select
        case when day_of_week in (6,7) then 'weekend' else 'weekday' end as day_type,
        sum(total_unit_sales) as total_sales_amount,
        sum(sold_units) as total_units_sold,
        sum(transactions_count) as total_transactions
    from fact.fct_store_daily_sales
    group by case when day_of_week in (6,7) then 'weekend' else 'weekday' end
""").fetchdf()
print(weekend_weekday_summary)


# monthly trend
print("\n4=== Monthly Sales Trend ===")
monthly_trend = db.execute("""
    select
        month,
        sum(total_unit_sales) as total_sales_amount,
        sum(sold_units) as total_units_sold,
        sum(transactions_count) as total_transactions
    from fact.fct_store_daily_sales
    group by year, month
    order by year, month
""").fetchdf()
print(monthly_trend)


# holiday vs non-holiday sales
print("\n5=== Holiday vs Non-Holiday  avg sales ===")
holiday_summary = db.execute("""
    select
        is_holiday,
        avg(total_unit_sales) as avg_sales_amount,
        avg(sold_units) as avg_units_sold,
        avg(transactions_count) as avg_transactions
    from fact.fct_store_daily_sales
    group by is_holiday
""").fetchdf()
print(holiday_summary)

# sales by store type
print("\n6=== Sales by Store Type ===")
store_type_summary = db.execute("""
    select
        store_type,
        sum(total_unit_sales) as total_sales_amount,
        sum(sold_units) as total_units_sold,
        sum(transactions_count) as total_transactions
    from fact.fct_store_daily_sales
    group by store_type
    order by total_sales_amount desc
""").fetchdf()
print(store_type_summary)   


# avg sales on Wagedays vs non wage days
print("\n7=== Sales on Wagedays vs non wage days ===")
wage_day_summary = db.execute("""
    select
        is_wage_day,
        avg(total_unit_sales) as avg_sales_amount,
        avg(sold_units) as avg_units_sold,
        avg(transactions_count) as avg_transactions
    from fact.fct_store_daily_sales
    group by is_wage_day
""").fetchdf()
print(wage_day_summary) 


# avg sales on earthquake days vs non earthquake days
print("\n8=== Sales on Earthquake days vs non earthquake days ===")
earthquake_day_summary = db.execute("""
    select
        is_earthquake_period,
        avg(total_unit_sales) as avg_sales_amount,
        avg(sold_units) as avg_units_sold,
        avg(transactions_count) as avg_transactions
    from fact.fct_store_daily_sales
    group by is_earthquake_period
""").fetchdf()
print(earthquake_day_summary)   


db.close()