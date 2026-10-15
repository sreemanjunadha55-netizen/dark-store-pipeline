import json
import numpy as np
import pandas as pd
from pathlib import Path

# ---------- Settings ----------
SEED = 42
START = pd.Timestamp("2026-09-01")
DAYS = 30
RESTOCK_HOUR = 6
OUT = Path("data/raw")
TRUTH = Path("data/truth")

rng = np.random.default_rng(SEED)


# ---------- Weather effects on demand ----------
RAIN_THRESHOLD_MM = 0.5
DEFAULT_RAIN_BOOST = 1.25
RAIN_BOOST = {"P009": 1.5, "P010": 1.5, "P014": 1.4}
HOT_THRESHOLD_C = 30
HEAT_BOOST = {"P011": 1.4, "P012": 1.4, "P013": 1.6}


# ---------- Stores ----------
stores = pd.DataFrame(
    [
        ("S001", "Bengaluru", "Koramangala", 12.9352, 77.6245),
        ("S002", "Bengaluru", "Whitefield", 12.9698, 77.7500),
        ("S003", "Mumbai", "Andheri", 19.1136, 72.8697),
        ("S004", "Mumbai", "Powai", 19.1176, 72.9060),
        ("S005", "Delhi", "Saket", 28.5245, 77.2066),
        ("S006", "Delhi", "Dwarka", 28.5823, 77.0500),
        ("S007", "Hyderabad", "Gachibowli", 17.4401, 78.3489),
        ("S008", "Hyderabad", "Kukatpally", 17.4948, 78.3996),
        ("S009", "Pune", "Kothrud", 18.5074, 73.8077),
        ("S010", "Chennai", "Adyar", 13.0012, 80.2565),
    ],
    columns=["store_id", "city", "area", "latitude", "longitude"],
)

# ---------- Products ----------
products = pd.DataFrame(
    [
        ("P001", "Toned Milk 500ml", "Dairy", 27, 4.0),
        ("P002", "Brown Bread 400g", "Bakery", 45, 2.5),
        ("P003", "Eggs 6 pack", "Dairy", 60, 3.0),
        ("P004", "Curd 400g", "Dairy", 35, 2.0),
        ("P005", "Bananas 1 dozen", "Fresh Produce", 55, 1.8),
        ("P006", "Tomato 500g", "Fresh Produce", 30, 2.2),
        ("P007", "Onion 1kg", "Fresh Produce", 45, 1.6),
        ("P008", "Potato 1kg", "Fresh Produce", 40, 1.5),
        ("P009", "Instant Noodles", "Packaged", 14, 2.4),
        ("P010", "Potato Chips 50g", "Snacks", 20, 2.0),
        ("P011", "Cola 750ml", "Beverages", 40, 1.5),
        ("P012", "Bottled Water 1L", "Beverages", 20, 1.3),
        ("P013", "Ice Cream Cup", "Frozen", 50, 0.9),
        ("P014", "Instant Coffee 50g", "Beverages", 150, 0.6),
        ("P015", "Pain Relief Strip", "Health", 30, 0.4),
        ("P016", "Detergent 1kg", "Household", 120, 0.3),
    ],
    columns=["product_id", "product_name", "category", "price_inr", "base_per_hour"],
)

# ---------- Demand shape ----------
# One multiplier per hour of the day (0 = midnight ... 23 = 11 PM)
HOUR_FACTOR = np.array([
    0.2, 0.1, 0.1, 0.1, 0.2, 0.4,
    0.8, 1.3, 1.5, 1.2, 1.0, 1.0,
    1.2, 1.1, 0.9, 0.9, 1.0, 1.3,
    1.8, 2.0, 1.9, 1.5, 0.9, 0.4,
])

hours = pd.date_range(START, periods=DAYS * 24, freq="h")
hour_of_day = hours.hour.to_numpy()
weekend_factor = np.where(hours.dayofweek >= 5, 1.25, 1.0)
shape = HOUR_FACTOR[hour_of_day] * weekend_factor

store_size = dict(zip(stores["store_id"], rng.uniform(0.7, 1.4, len(stores)).round(2)))



# ---------- Load real weather ----------
weather_files = sorted((OUT / "weather").glob("date=*/weather.parquet"))
if not weather_files:
    raise SystemExit("No weather files found. Run data_generator/fetch_weather.py first.")

weather = pd.concat([pd.read_parquet(f) for f in weather_files], ignore_index=True)
rain = weather.pivot(index="weather_ts", columns="city", values="precipitation_mm").reindex(hours)
temp = weather.pivot(index="weather_ts", columns="city", values="temperature_c").reindex(hours)
print("Weather hours matched:", int(rain.notna().all(axis=1).sum()), "of", len(hours))

rain = rain.fillna(0)
temp = temp.ffill().bfill()
is_rain = {city: rain[city].to_numpy() > RAIN_THRESHOLD_MM for city in rain.columns}
is_hot = {city: temp[city].to_numpy() >= HOT_THRESHOLD_C for city in temp.columns}



# ---------- Simulation ----------
order_rows, snap_rows, truth_rows = [], [], []

for s in stores.itertuples():
    size = store_size[s.store_id]
    for p in products.itertuples():
        rain_mult = np.where(is_rain[s.city], RAIN_BOOST.get(p.product_id, DEFAULT_RAIN_BOOST), 1.0)
        heat_mult = np.where(is_hot[s.city], HEAT_BOOST.get(p.product_id, 1.0), 1.0)
        demand = rng.poisson(p.base_per_hour * size * shape * rain_mult * heat_mult)
        cover = rng.uniform(0.8, 1.3)
        restock_qty = max(5, int(p.base_per_hour * size * HOUR_FACTOR.sum() * cover))
        stock = restock_qty

        for i in range(len(hours)):
            if hour_of_day[i] == RESTOCK_HOUR:
                stock = restock_qty

            wanted = int(demand[i])
            sold = min(wanted, stock)
            stock -= sold
            lost = wanted - sold

            remaining = sold
            while remaining > 0:
                qty = min(remaining, int(rng.choice([1, 2, 3], p=[0.7, 0.2, 0.1])))
                order_rows.append((s.store_id, p.product_id, i, qty,
                                   int(rng.integers(0, 3600)), p.price_inr))
                remaining -= qty

            snap_rows.append((s.store_id, p.product_id, i, stock))
            if lost > 0:
                truth_rows.append((s.store_id, p.product_id, i, lost))


                # ---------- Build tables ----------
orders = pd.DataFrame(order_rows, columns=[
    "store_id", "product_id", "hour_idx", "qty", "offset_s", "unit_price_inr"])
orders["order_ts"] = hours[orders["hour_idx"].to_numpy()] + pd.to_timedelta(
    orders["offset_s"].to_numpy(), unit="s")
orders = orders.sort_values("order_ts").reset_index(drop=True)
orders["order_id"] = "O" + (orders.index + 1).astype(str).str.zfill(8)
orders = orders[["order_id", "store_id", "product_id", "qty", "unit_price_inr", "order_ts"]]

inventory = pd.DataFrame(snap_rows, columns=["store_id", "product_id", "hour_idx", "qty_on_shelf"])
inventory["snapshot_ts"] = hours[inventory["hour_idx"].to_numpy()] + pd.Timedelta(hours=1)
inventory = inventory.drop(columns="hour_idx")

truth = pd.DataFrame(truth_rows, columns=["store_id", "product_id", "hour_idx", "lost_units"])
truth["hour_ts"] = hours[truth["hour_idx"].to_numpy()]
truth = truth.drop(columns="hour_idx")

stockout_share = (inventory["qty_on_shelf"] == 0).mean()


# ---------- Make the data messy ----------
dups = orders.sample(frac=0.01, random_state=SEED)
orders = pd.concat([orders, dups]).sort_values("order_ts").reset_index(drop=True)
bad_idx = inventory.sample(frac=0.002, random_state=SEED).index
inventory.loc[bad_idx, "qty_on_shelf"] = -1

# ---------- Save ----------
def write_daily(df, ts_col, name):
    days = df[ts_col].dt.strftime("%Y-%m-%d")
    for day, part in df.groupby(days):
        folder = OUT / name / f"date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        part.to_parquet(folder / f"{name}.parquet", index=False)

OUT.mkdir(parents=True, exist_ok=True)
TRUTH.mkdir(parents=True, exist_ok=True)
stores.to_csv(OUT / "stores.csv", index=False)
products.to_csv(OUT / "products.csv", index=False)
write_daily(orders, "order_ts", "orders")
write_daily(inventory, "snapshot_ts", "inventory")
truth.to_parquet(TRUTH / "lost_sales_truth.parquet", index=False)

weather_effects = {
    "rain_threshold_mm": RAIN_THRESHOLD_MM,
    "default_rain_boost": DEFAULT_RAIN_BOOST,
    "rain_boost_by_product": RAIN_BOOST,
    "hot_threshold_c": HOT_THRESHOLD_C,
    "heat_boost_by_product": HEAT_BOOST,
}
(TRUTH / "weather_effects.json").write_text(json.dumps(weather_effects, indent=2))

print("Stores:", len(stores), "| Products:", len(products))
print("Order rows:", len(orders), "(includes", len(dups), "duplicates)")
print("Inventory rows:", len(inventory), "(includes", len(bad_idx), "bad -1 readings)")
print(f"Share of store-product-hours with zero stock: {stockout_share:.1%}")
print("Truth rows (hours with lost sales):", len(truth))