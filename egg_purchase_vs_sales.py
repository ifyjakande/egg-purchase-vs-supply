import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import base64
import json
import hashlib
import os
import subprocess
import urllib.request

# --- Config from environment variables ---
# Sheet IDs and staff mapping come from a single PIPELINE_CONFIG_JSON secret that mirrors local_config.json.
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
PIPELINE_CONFIG = json.loads(os.environ["PIPELINE_CONFIG_JSON"])
TARGET_SPREADSHEET_ID = PIPELINE_CONFIG["target_spreadsheet_id"]
PURCHASE_SHEET_ID = PIPELINE_CONFIG["purchase_sheet_id"]
TRACKER_SHEET_ID = PIPELINE_CONFIG["tracker_sheet_id"]
SALES_STAFF = PIPELINE_CONFIG["sales_staff"]

GOOGLE_SPACE_WEBHOOK_URL = os.environ.get("GOOGLE_SPACE_WEBHOOK_URL")

PURCHASE_WORKSHEET = "Daily Egg Purchase Log"
SALES_WORKSHEET = "Daily Sales Log"
TRACKER_WORKSHEET = "Kaduna to Abuja"

TRACKER_TABS = {
    TRACKER_WORKSHEET: datetime(2026, 3, 10),
    "Kaduna to Kano": datetime(2026, 2, 27),
    "Kaduna Local Sales": datetime(2026, 1, 1),
}

BREAKAGE_THRESHOLD = 0.6  # percent

EGG_PRODUCTS = {"eggs", "cracked egg", "broken", "broken egg"}

MONTH_ORDER = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}

WAT = timezone(timedelta(hours=1))


# --- Helpers ---
def find_col(headers, name):
    """Find column index by header name (case-insensitive, stripped)."""
    name_lower = name.strip().lower()
    for i, h in enumerate(headers):
        if str(h).strip().lower() == name_lower:
            return i
    return None


def parse_date(date_str):
    """Parse 'DD-Mon-YYYY' to (year, month_name). Returns None on failure."""
    if not date_str or not str(date_str).strip():
        return None
    s = str(date_str).strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return (dt.year, dt.strftime("%B"))
        except ValueError:
            continue
    return None


def parse_date_obj(date_str):
    """Parse date string to a datetime object. Returns None on failure."""
    if not date_str or not str(date_str).strip():
        return None
    s = str(date_str).strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_num(s):
    """Parse number string (may have commas) to float. Returns 0 on failure."""
    if s is None:
        return 0
    s = str(s).strip().replace(",", "")
    if not s:
        return 0
    try:
        return float(s)
    except ValueError:
        return 0


def safe_get(row, idx):
    """Safely get value from row by index."""
    if idx is None or idx >= len(row):
        return ""
    return row[idx]


def wat_now():
    """Get current time in WAT (UTC+1), formatted as 12hr AM/PM."""
    return datetime.now(WAT).strftime("%b %d, %Y %I:%M %p WAT")


# --- Hash-based change detection ---
def compute_data_hash(*datasets):
    """Compute SHA-256 hash of all source data combined."""
    h = hashlib.sha256()
    for data in datasets:
        h.update(json.dumps(data, sort_keys=True, default=str).encode())
    return h.hexdigest()


def fetch_data_state():
    """Fetch data_state.json from the data-state branch via GitHub API."""
    try:
        result = subprocess.run(
            ["gh", "api", "repos/{owner}/{repo}/contents/data_state.json",
             "--jq", ".content", "-H", "Accept: application/vnd.github.v3+json",
             "--method", "GET", "-f", "ref=data-state"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            content = base64.b64decode(result.stdout.strip()).decode()
            return json.loads(content)
    except Exception as e:
        print(f"  Could not fetch data state: {e}")
    return None


def save_data_state(state):
    """Save data_state.json to /tmp for the workflow to commit."""
    with open("/tmp/data_state.json", "w") as f:
        json.dump(state, f, indent=2)
    print(f"  Saved data state to /tmp/data_state.json")


# --- Auth ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
service_account_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
gc = gspread.authorize(creds)


# --- Read Purchase Data ---
print("Reading purchase data...")
purchase_book = gc.open_by_key(PURCHASE_SHEET_ID)
purchase_ws = purchase_book.worksheet(PURCHASE_WORKSHEET)
purchase_data = purchase_ws.get_all_values()

# Auto-detect header row by scanning for "Date" + "Number of Crates"
p_header_idx = None
for i, row in enumerate(purchase_data):
    row_lower = [str(c).strip().lower() for c in row]
    if "date" in row_lower and "number of crates" in row_lower:
        p_header_idx = i
        break

purchase_headers = purchase_data[p_header_idx]
p_date_col = find_col(purchase_headers, "Date")
p_crates_col = find_col(purchase_headers, "Number of Crates")
p_eggs_per_crate_col = find_col(purchase_headers, "Eggs per Crate")
p_broken_col = find_col(purchase_headers, "Broken/Damaged Eggs")
p_cracked_col = find_col(purchase_headers, "Cracked Eggs")

print(f"  Purchase header row: {p_header_idx}")
print(f"  Purchase columns: Date={p_date_col}, Crates={p_crates_col}, "
      f"EggsPerCrate={p_eggs_per_crate_col}, Broken={p_broken_col}, Cracked={p_cracked_col}")

# Group by (year, month)
purchase_monthly = defaultdict(lambda: {"crates": 0, "total_eggs": 0, "broken": 0, "cracked": 0})

for row in purchase_data[p_header_idx + 1:]:
    date_val = safe_get(row, p_date_col)
    parsed = parse_date(date_val)
    if not parsed:
        continue
    key = parsed  # (year, month_name)
    crates = parse_num(safe_get(row, p_crates_col))
    eggs_per = parse_num(safe_get(row, p_eggs_per_crate_col))
    if eggs_per == 0:
        eggs_per = 30
    broken = parse_num(safe_get(row, p_broken_col))
    cracked = parse_num(safe_get(row, p_cracked_col))

    purchase_monthly[key]["crates"] += crates
    purchase_monthly[key]["total_eggs"] += crates * eggs_per
    purchase_monthly[key]["broken"] += broken
    purchase_monthly[key]["cracked"] += cracked

print(f"  Found {len(purchase_monthly)} purchase months")


# --- Read Sales Data ---
print("Reading sales data...")

# Aggregated sales across all staff: {(year,month): {"eggs":0, "cracked":0, "broken":0}}
sales_monthly = defaultdict(lambda: {"eggs": 0, "cracked": 0, "broken": 0})
# Sample eggs (given out, not sold): {(year,month): pieces}
samples_monthly = defaultdict(float)
# Victor Abuja eggs: {(year,month): pieces}
victor_abuja = defaultdict(float)
# Femi Abuja eggs: {(year,month): {"eggs":0, "cracked":0, "broken":0}}
femi_abuja = defaultdict(lambda: {"eggs": 0, "cracked": 0, "broken": 0})
# Track which staff have egg sales
staff_with_egg_sales = set()

all_sales_raw = {}
for staff_name, sheet_id in SALES_STAFF.items():
    print(f"  Reading {staff_name}...")
    book = gc.open_by_key(sheet_id)
    ws = book.worksheet(SALES_WORKSHEET)
    all_vals = ws.get_all_values()
    all_sales_raw[staff_name] = all_vals

    # Find header row -- first row with recognizable headers
    header_row_idx = None
    headers = []
    for i, row in enumerate(all_vals):
        row_lower = [str(c).strip().lower() for c in row]
        if "date" in row_lower and "product type" in row_lower:
            header_row_idx = i
            headers = row
            break

    if header_row_idx is None:
        print(f"    WARNING: Could not find headers for {staff_name}, skipping")
        continue

    date_col = find_col(headers, "Date")
    state_col = find_col(headers, "State")
    product_col = find_col(headers, "Product Type")
    crates_col = find_col(headers, "Crates")
    pieces_col = find_col(headers, "Pieces")
    status_col = find_col(headers, "Status")

    print(f"    Columns: Date={date_col}, State={state_col}, Product={product_col}, "
          f"Crates={crates_col}, Pieces={pieces_col}, Status={status_col}")

    for row in all_vals[header_row_idx + 1:]:
        date_val = safe_get(row, date_col)
        parsed = parse_date(date_val)
        if not parsed:
            continue

        product_type = str(safe_get(row, product_col)).strip().lower()
        if product_type not in EGG_PRODUCTS:
            continue

        pieces = parse_num(safe_get(row, pieces_col))
        state = str(safe_get(row, state_col)).strip().lower()
        status = str(safe_get(row, status_col)).strip().lower()
        key = parsed
        staff_with_egg_sales.add(staff_name)

        # Samples are eggs given out, not sold -- track separately and skip sales accumulation
        if status == "sample":
            samples_monthly[key] += pieces
            continue

        # Victor Abuja -- track separately (these are transfers, not end sales)
        is_victor_abuja = staff_name == "Victor" and state == "abuja"
        if is_victor_abuja and product_type == "eggs":
            victor_abuja[key] += pieces

        # Classify product into Sales (All Staff)
        # Exclude Victor's Abuja entries -- Femi captures the actual Abuja sales
        if not is_victor_abuja:
            if product_type == "eggs":
                sales_monthly[key]["eggs"] += pieces
            elif product_type == "cracked egg":
                sales_monthly[key]["cracked"] += pieces
            elif product_type in ("broken", "broken egg"):
                sales_monthly[key]["broken"] += pieces

        # Femi Abuja
        if staff_name == "Femi" and state == "abuja":
            if product_type == "eggs":
                femi_abuja[key]["eggs"] += pieces
            elif product_type == "cracked egg":
                femi_abuja[key]["cracked"] += pieces
            elif product_type in ("broken", "broken egg"):
                femi_abuja[key]["broken"] += pieces

print(f"  Found {len(sales_monthly)} sales months")


# --- Read Egg Movement Tracker (Kaduna to Abuja) ---
print("Reading egg movement tracker (Kaduna to Abuja)...")
tracker_book = gc.open_by_key(TRACKER_SHEET_ID)
tracker_ws = tracker_book.worksheet(TRACKER_WORKSHEET)
tracker_data = tracker_ws.get_all_values()

# Auto-detect header row by scanning for "Date" + "Eggs Shipped"
t_header_idx = None
for i, row in enumerate(tracker_data):
    row_lower = [str(c).strip().lower() for c in row]
    if "date" in row_lower and "eggs shipped" in row_lower:
        t_header_idx = i
        break

tracker_headers = tracker_data[t_header_idx]
t_date_col = find_col(tracker_headers, "Date")
t_shipped_col = find_col(tracker_headers, "Eggs Shipped")
t_delivered_col = find_col(tracker_headers, "Eggs Delivered")
t_broken_col = find_col(tracker_headers, "Eggs Broken")
t_cracked_col = find_col(tracker_headers, "Cracked Eggs")

print(f"  Tracker header row: {t_header_idx}")
print(f"  Tracker columns: Date={t_date_col}, Shipped={t_shipped_col}, "
      f"Delivered={t_delivered_col}, Broken={t_broken_col}, Cracked={t_cracked_col}")

tracker_monthly = defaultdict(lambda: {"shipped": 0, "delivered": 0, "broken": 0, "cracked": 0})

for row in tracker_data[t_header_idx + 1:]:
    date_val = safe_get(row, t_date_col)
    parsed = parse_date(date_val)
    if not parsed:
        continue
    if parsed[0] < 2026:
        continue
    key = parsed
    tracker_monthly[key]["shipped"] += parse_num(safe_get(row, t_shipped_col))
    tracker_monthly[key]["delivered"] += parse_num(safe_get(row, t_delivered_col))
    tracker_monthly[key]["broken"] += parse_num(safe_get(row, t_broken_col))
    tracker_monthly[key]["cracked"] += parse_num(safe_get(row, t_cracked_col))

print(f"  Found {len(tracker_monthly)} tracker months")


# --- Read additional tracker tabs for breakage alerts ---
print("Reading additional tracker tabs for breakage alerts...")
all_tracker_raw = {TRACKER_WORKSHEET: tracker_data}
for tab_name in TRACKER_TABS:
    if tab_name == TRACKER_WORKSHEET:
        continue  # already read
    print(f"  Reading tracker tab: {tab_name}...")
    tab_ws = tracker_book.worksheet(tab_name)
    all_tracker_raw[tab_name] = tab_ws.get_all_values()


# --- Breakage/Cracking Alert Logic ---
print("Checking for breakage/cracking alerts...")
breakage_alerts = []

for tab_name, cutoff_date in TRACKER_TABS.items():
    tab_data = all_tracker_raw[tab_name]

    # Find header row
    tab_header_idx = None
    for i, row in enumerate(tab_data):
        row_lower = [str(c).strip().lower() for c in row]
        if "date" in row_lower and "eggs shipped" in row_lower:
            tab_header_idx = i
            break

    if tab_header_idx is None:
        print(f"  WARNING: Could not find headers for {tab_name}, skipping")
        continue

    tab_headers = tab_data[tab_header_idx]
    col_date = find_col(tab_headers, "Date")
    col_customer = find_col(tab_headers, "Customer Name")
    col_shipped = find_col(tab_headers, "Eggs Shipped")
    col_broken = find_col(tab_headers, "Eggs Broken")
    col_cracked = find_col(tab_headers, "Cracked Eggs")

    for row in tab_data[tab_header_idx + 1:]:
        date_str = safe_get(row, col_date)
        dt = parse_date_obj(date_str)
        if dt is None or dt <= cutoff_date:
            continue

        shipped = parse_num(safe_get(row, col_shipped))
        if shipped <= 0:
            continue

        broken = parse_num(safe_get(row, col_broken))
        cracked = parse_num(safe_get(row, col_cracked))
        broken_pct = broken / shipped * 100
        cracked_pct = cracked / shipped * 100

        if broken_pct > BREAKAGE_THRESHOLD or cracked_pct > BREAKAGE_THRESHOLD:
            customer = str(safe_get(row, col_customer)).strip()
            breakage_alerts.append({
                "tab": tab_name,
                "date": dt.strftime("%d-%b-%Y"),
                "customer": customer,
                "shipped": int(shipped),
                "broken": int(broken),
                "cracked": int(cracked),
                "broken_pct": broken_pct,
                "cracked_pct": cracked_pct,
            })

print(f"  Found {len(breakage_alerts)} shipments exceeding {BREAKAGE_THRESHOLD}% threshold")


# --- Hash-based change detection (only on scheduled CI runs) ---
IS_CI = os.environ.get("GITHUB_ACTIONS") == "true"
IS_SCHEDULED = IS_CI and os.environ.get("GITHUB_EVENT_NAME") == "schedule"
now_wat = wat_now()
previous_state = None
new_state = {}

if IS_CI:
    current_hash = compute_data_hash(purchase_data, all_sales_raw, all_tracker_raw)
    previous_state = fetch_data_state()

    if IS_SCHEDULED:
        print("\nChecking for data changes...")
        if previous_state and previous_state.get("hash") == current_hash:
            print("  No changes detected in source data. Skipping update.")
            previous_state["last_checked"] = now_wat
            save_data_state(previous_state)
            exit(0)
        print("  Data has changed (or first run). Proceeding with update...")
    else:
        print("\nManual CI run — skipping hash check, forcing update.")

    new_state = {
        "hash": current_hash,
        "last_checked": now_wat,
        "last_updated": now_wat,
    }
else:
    print("\nRunning locally — skipping hash check.")


# --- Merge all data by year+month ---
all_keys = set()
all_keys.update(purchase_monthly.keys())
all_keys.update(sales_monthly.keys())
all_keys.update(samples_monthly.keys())
all_keys.update(victor_abuja.keys())
all_keys.update(femi_abuja.keys())
all_keys.update(tracker_monthly.keys())

sorted_keys = sorted(all_keys, key=lambda k: (k[0], MONTH_ORDER.get(k[1], 0)))

print(f"\nTotal months: {len(sorted_keys)}")

prev_carry = 0  # eggs rolled over from prior month (surpluses only)
rows = []
for key in sorted_keys:
    year, month = key
    p = purchase_monthly[key]
    s = sales_monthly[key]

    crates = p["crates"]
    total_eggs = p["total_eggs"]
    broken_p = p["broken"]
    cracked_p = p["cracked"]
    usable = total_eggs - broken_p  # informational: eggs intact on arrival (cracked still sellable)

    good_sold = s["eggs"]
    broken_sold = s["broken"]  # broken loss recorded in sales — already includes arrival breakage
    cracked_sold = s["cracked"]
    total_sold = good_sold + cracked_sold
    samples = samples_monthly[key]

    # P vs S with asymmetric carry-over: surpluses roll forward, deficits are bucketed
    prior_surplus = prev_carry
    adjusted_sd = total_eggs + prior_surplus - total_sold - broken_sold - samples
    unaccounted = max(0, -adjusted_sd)
    carried_forward = max(0, adjusted_sd)
    prev_carry = carried_forward

    v_sent = victor_abuja[key]
    f_data = femi_abuja[key]
    f_good = f_data["eggs"]
    f_broken = f_data["broken"]
    f_cracked = f_data["cracked"]
    f_total_received = f_good + f_broken + f_cracked
    transfer_var = v_sent - f_total_received

    # Tracker data
    t = tracker_monthly[key]
    t_shipped = t["shipped"]
    t_delivered = t["delivered"]
    t_broken = t["broken"]
    t_cracked = t["cracked"]
    t_victor_vs_tracker = v_sent - t_shipped
    t_delivered_vs_femi = t_delivered - f_total_received

    rows.append([
        month, year,
        crates, total_eggs, broken_p, cracked_p, usable,
        good_sold, broken_sold, cracked_sold, total_sold, samples,
        prior_surplus, adjusted_sd, unaccounted, carried_forward,
        v_sent, f_good, f_broken, f_cracked, transfer_var,
        t_shipped, t_delivered, t_broken, t_cracked,
        t_victor_vs_tracker, t_delivered_vs_femi,
    ])


# --- Write to target spreadsheet ---
print("\nWriting to target spreadsheet...")
target_book = gc.open_by_key(TARGET_SPREADSHEET_ID)
target_ws = target_book.sheet1

# Rename worksheet
target_ws.update_title("Egg Purchase vs Sales")

# Unmerge all existing cells before clearing
target_book.batch_update({"requests": [{
    "unmergeCells": {
        "range": {
            "sheetId": target_ws.id,
            "startRowIndex": 0,
            "endRowIndex": target_ws.row_count,
            "startColumnIndex": 0,
            "endColumnIndex": target_ws.col_count,
        }
    }
}]})

# Get the sheet ID for formatting
sheet_id = target_ws.id

# Build output: title (row 1), section headers (row 2), column headers (row 3), data (row 4+)
COLUMN_HEADERS = [
    "Month", "Year",
    "Crates Purchased", "Total Eggs Purchased", "Broken Eggs (Purchase)",
    "Cracked Eggs (Purchase)", "Eggs Available for Sale",
    "Good Eggs Sold", "Broken Eggs (Loss)", "Cracked Eggs Sold", "Total Eggs Sold", "Samples",
    "Eggs Carried In", "Surplus / Deficit", "Unaccounted", "Eggs On Hand",
    "Victor Eggs Sent (Abuja)", "Femi Good Eggs (Abuja)", "Femi Broken (Abuja)",
    "Femi Cracked (Abuja)", "Transfer Variance (Sent - Received)",
    "Tracker Shipped", "Tracker Delivered", "Transit Broken",
    "Transit Cracked", "Victor Sent vs Tracker Shipped", "Tracker Delivered vs Femi Sold",
]

# Name -> index lookup. All downstream code references columns by name, so
# reordering COLUMN_HEADERS automatically reorders everything else.
COL = {name: i for i, name in enumerate(COLUMN_HEADERS)}

# Sections in display order. Each maps a section label to its first column's name.
# Section end is inferred from the next section's start (or end of headers).
SECTIONS_DEF = [
    ("Period", "Month"),
    ("PURCHASE", "Crates Purchased"),
    ("SALES (All Staff)", "Good Eggs Sold"),
    ("P vs S", "Eggs Carried In"),
    ("VICTOR → FEMI (Abuja)", "Victor Eggs Sent (Abuja)"),
    ("EGG MOVEMENT TRACKER (Kaduna → Abuja)", "Tracker Shipped"),
]

# Columns whose values get red/green bold treatment based on sign.
VARIANCE_COLS = [
    "Surplus / Deficit",
    "Transfer Variance (Sent - Received)",
    "Victor Sent vs Tracker Shipped",
    "Tracker Delivered vs Femi Sold",
]

# Column widths keyed by header name (pixels).
COL_WIDTHS = {
    "Month": 70, "Year": 45,
    "Crates Purchased": 70, "Total Eggs Purchased": 75, "Broken Eggs (Purchase)": 65,
    "Cracked Eggs (Purchase)": 65, "Eggs Available for Sale": 75,
    "Good Eggs Sold": 75, "Broken Eggs (Loss)": 65, "Cracked Eggs Sold": 65,
    "Total Eggs Sold": 75, "Samples": 65,
    "Eggs Carried In": 70, "Surplus / Deficit": 75, "Unaccounted": 75, "Eggs On Hand": 75,
    "Victor Eggs Sent (Abuja)": 75, "Femi Good Eggs (Abuja)": 75,
    "Femi Broken (Abuja)": 65, "Femi Cracked (Abuja)": 65,
    "Transfer Variance (Sent - Received)": 80,
    "Tracker Shipped": 75, "Tracker Delivered": 75, "Transit Broken": 65,
    "Transit Cracked": 65, "Victor Sent vs Tracker Shipped": 80,
    "Tracker Delivered vs Femi Sold": 80,
}


def compute_section_ranges(headers):
    """Returns ordered list of (label, start_col, end_col) for sections present in `headers`."""
    starts = []
    for label, first_col in SECTIONS_DEF:
        if first_col in headers:
            starts.append((label, headers.index(first_col)))
    starts.sort(key=lambda x: x[1])
    result = []
    for i, (label, start) in enumerate(starts):
        end = starts[i + 1][1] if i + 1 < len(starts) else len(headers)
        result.append((label, start, end))
    return result


num_cols = len(COLUMN_HEADERS)

# Prepare all cell values
all_output = []
# Row 1: title in C1 with embedded timestamp and current holding
current_holding = int(rows[-1][COL["Eggs On Hand"]]) if rows else 0
title_main = "PULLUS - Egg Purchase vs Sales Monthly Summary"
title_separator = "  |  "
title_timestamp = f"Last Updated: {now_wat}"
title_holding = f"Current Holding: {current_holding:,} eggs"
title_text = title_main + title_separator + title_holding + title_separator + title_timestamp
title_row = ["", ""] + [title_text] + [""] * (num_cols - 3)
all_output.append(title_row)
# Row 2: section headers (only first cell of each section)
section_ranges_main = compute_section_ranges(COLUMN_HEADERS)
section_row = [""] * num_cols
for label, start, _end in section_ranges_main:
    section_row[start] = label
all_output.append(section_row)
# Row 3: column headers
all_output.append(COLUMN_HEADERS)
# Data rows
for r in rows:
    # Convert all numbers to int where appropriate
    out_row = [r[0], int(r[1])]
    for v in r[2:]:
        out_row.append(int(v) if v == int(v) else v)
    all_output.append(out_row)

# Totals row -- sum most numeric columns, but special-case the P vs S section:
# Eggs Carried In = period start (0); Eggs On Hand = latest month (current holding);
# Surplus/Deficit = current holding - cumulative unaccounted (true cumulative net,
# avoids double-counting carry-overs that were already consumed).
totals_row = ["TOTAL", ""]
for col_idx in range(2, num_cols):
    if col_idx == COL["Eggs Carried In"]:
        totals_row.append(0)  # period start, nothing carried into the first month
    elif col_idx == COL["Eggs On Hand"]:
        totals_row.append(int(rows[-1][col_idx]) if rows else 0)  # current holding = latest month
    elif col_idx == COL["Surplus / Deficit"]:
        on_hand_end = int(rows[-1][COL["Eggs On Hand"]]) if rows else 0
        unaccounted_total = int(sum(r[COL["Unaccounted"]] for r in rows))
        totals_row.append(on_hand_end - unaccounted_total)
    else:
        totals_row.append(int(sum(r[col_idx] for r in rows)))
all_output.append(totals_row)

# Clear and write
target_ws.clear()
target_ws.update(all_output, "A1")

print(f"  Wrote {len(rows)} data rows")


# --- Apply formatting via batchUpdate ---
print("Applying formatting...")


def rgb(hex_color):
    """Convert hex color to Google Sheets RGB dict (0-1 floats)."""
    h = hex_color.lstrip("#")
    return {
        "red": int(h[0:2], 16) / 255,
        "green": int(h[2:4], 16) / 255,
        "blue": int(h[4:6], 16) / 255,
    }


def cell_format(bg_hex, fg_hex, bold=False, font_size=None, h_align=None):
    """Build a CellFormat dict."""
    fmt = {
        "backgroundColor": rgb(bg_hex),
        "textFormat": {
            "foregroundColor": rgb(fg_hex),
            "bold": bold,
        },
    }
    if font_size:
        fmt["textFormat"]["fontSize"] = font_size
    if h_align:
        fmt["horizontalAlignment"] = h_align
    return fmt


def grid_range(start_row, end_row, start_col, end_col):
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


WHITE = "#FFFFFF"
DARK_NAVY = "#1B2A4A"
DARK_GRAY = "#4A4A4A"
DEEP_TEAL = "#0D7377"
STEEL_BLUE = "#2E5E86"
DARK_AMBER = "#BF8B2E"
DEEP_PURPLE = "#5B3A6B"
DARK_BROWN = "#5D4037"
CHARCOAL = "#333333"

LIGHT_GRAY = "#E8E8E8"
LIGHT_TEAL = "#E0F2F1"
LIGHT_BLUE = "#E3EDF5"
LIGHT_AMBER = "#FFF3E0"
LIGHT_PURPLE = "#F3E5F5"
LIGHT_BROWN = "#EFEBE9"

ROW_WHITE = "#FFFFFF"
ROW_ALT = "#F8F9FA"

# Section label -> (dark bg color, light bg color). Used for header row + column header row formatting.
SECTION_COLORS = {
    "Period": (DARK_GRAY, LIGHT_GRAY),
    "PURCHASE": (DEEP_TEAL, LIGHT_TEAL),
    "SALES (All Staff)": (STEEL_BLUE, LIGHT_BLUE),
    "P vs S": (DARK_AMBER, LIGHT_AMBER),
    "VICTOR → FEMI (Abuja)": (DEEP_PURPLE, LIGHT_PURPLE),
    "EGG MOVEMENT TRACKER (Kaduna → Abuja)": (DARK_BROWN, LIGHT_BROWN),
}

total_rows = len(all_output)

requests = []

# --- Merges ---
# Title row: merge C1:W1 only (A1:B1 left blank for frozen cols)
requests.append({
    "mergeCells": {
        "range": grid_range(0, 1, 2, num_cols),
        "mergeType": "MERGE_ALL",
    }
})
# Section header merges (row 2) -- derived from section ranges
section_merges = [(start, end) for _label, start, end in section_ranges_main]
for start, end in section_merges:
    requests.append({
        "mergeCells": {
            "range": grid_range(1, 2, start, end),
            "mergeType": "MERGE_ALL",
        }
    })

# --- Title row format ---
requests.append({
    "repeatCell": {
        "range": grid_range(0, 1, 0, num_cols),
        "cell": {"userEnteredFormat": cell_format(DARK_NAVY, WHITE, bold=True, font_size=14, h_align="CENTER")},
        "fields": "userEnteredFormat",
    }
})
# Format runs: main title bold 14pt, Current Holding bold 11pt (prominent), timestamp italic gray 9pt
holding_run_start = len(title_main)  # where "  |  Current Holding..." begins
timestamp_run_start = holding_run_start + len(title_separator) + len(title_holding)  # where "  |  Last Updated..." begins
requests.append({
    "updateCells": {
        "range": grid_range(0, 1, 2, 3),  # C1 only (merged title cell)
        "rows": [{
            "values": [{
                "textFormatRuns": [
                    {"startIndex": 0, "format": {"fontFamily": "Lato", "fontSize": 14, "bold": True, "foregroundColor": rgb(WHITE)}},
                    {"startIndex": holding_run_start, "format": {"fontFamily": "Lato", "fontSize": 11, "bold": True, "foregroundColor": rgb(WHITE)}},
                    {"startIndex": timestamp_run_start, "format": {"fontFamily": "Lato", "fontSize": 9, "bold": False, "italic": True, "foregroundColor": rgb("#B0B0B0")}},
                ]
            }]
        }],
        "fields": "textFormatRuns",
    }
})

# --- Section header row format (row 2) ---
section_colors = [(start, end, SECTION_COLORS[label][0]) for label, start, end in section_ranges_main]
for start, end, color in section_colors:
    requests.append({
        "repeatCell": {
            "range": grid_range(1, 2, start, end),
            "cell": {"userEnteredFormat": cell_format(color, WHITE, bold=True, font_size=11, h_align="CENTER")},
            "fields": "userEnteredFormat",
        }
    })

# --- Column header row format (row 3) ---
col_header_colors = [(start, end, SECTION_COLORS[label][1], CHARCOAL) for label, start, end in section_ranges_main]
for start, end, bg, fg in col_header_colors:
    fmt = cell_format(bg, fg, bold=True, h_align="CENTER")
    fmt["wrapStrategy"] = "WRAP"
    requests.append({
        "repeatCell": {
            "range": grid_range(2, 3, start, end),
            "cell": {"userEnteredFormat": fmt},
            "fields": "userEnteredFormat",
        }
    })

# --- Data row formatting ---
data_start_row = 3  # 0-indexed row 3 = spreadsheet row 4
for i in range(len(rows)):
    row_idx = data_start_row + i
    bg = ROW_WHITE if i % 2 == 0 else ROW_ALT

    # Base format: alternating bg, center aligned, reset text format (clears stale bold/color from prior layouts)
    requests.append({
        "repeatCell": {
            "range": grid_range(row_idx, row_idx + 1, 0, num_cols),
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": rgb(bg),
                    "horizontalAlignment": "CENTER",
                    "textFormat": {"bold": False, "foregroundColor": rgb(CHARCOAL)},
                }
            },
            "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)",
        }
    })

    # Number format for numeric columns (C onwards, index 2+)
    requests.append({
        "repeatCell": {
            "range": grid_range(row_idx, row_idx + 1, 2, num_cols),
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    "backgroundColor": rgb(bg),
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat(numberFormat,backgroundColor,horizontalAlignment)",
        }
    })

    # Conditional color for variance columns (L=11, Q=16, V=21, W=22)
    for var_col in [COL[n] for n in VARIANCE_COLS]:
        val = rows[i][var_col]
        text_color = "#0A7A0A" if val >= 0 else "#CC0000"
        requests.append({
            "repeatCell": {
                "range": grid_range(row_idx, row_idx + 1, var_col, var_col + 1),
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"foregroundColor": rgb(text_color), "bold": True},
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                        "backgroundColor": rgb(bg),
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat",
            }
        })

# --- Totals row formatting ---
totals_row_idx = total_rows - 1  # last row
requests.append({
    "repeatCell": {
        "range": grid_range(totals_row_idx, totals_row_idx + 1, 0, num_cols),
        "cell": {
            "userEnteredFormat": {
                "backgroundColor": rgb(LIGHT_GRAY),
                "textFormat": {"bold": True, "foregroundColor": rgb(DARK_NAVY)},
                "horizontalAlignment": "CENTER",
            }
        },
        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
    }
})
# Number format for totals numeric columns
requests.append({
    "repeatCell": {
        "range": grid_range(totals_row_idx, totals_row_idx + 1, 2, num_cols),
        "cell": {
            "userEnteredFormat": {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                "backgroundColor": rgb(LIGHT_GRAY),
                "textFormat": {"bold": True, "foregroundColor": rgb(DARK_NAVY)},
                "horizontalAlignment": "CENTER",
            }
        },
        "fields": "userEnteredFormat",
    }
})
# Variance color in totals row
for var_col in [COL[n] for n in VARIANCE_COLS]:
    val = totals_row[var_col]
    text_color = "#0A7A0A" if val >= 0 else "#CC0000"
    requests.append({
        "repeatCell": {
            "range": grid_range(totals_row_idx, totals_row_idx + 1, var_col, var_col + 1),
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"foregroundColor": rgb(text_color), "bold": True},
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    "backgroundColor": rgb(LIGHT_GRAY),
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat",
        }
    })
# Top border on totals row to separate from data
requests.append({
    "updateBorders": {
        "range": grid_range(totals_row_idx, totals_row_idx + 1, 0, num_cols),
        "top": {"style": "SOLID_MEDIUM", "color": rgb(CHARCOAL)},
    }
})

# --- Borders ---
# Thin borders on all cells
thin_border = {"style": "SOLID", "color": rgb("#D0D0D0")}
requests.append({
    "updateBorders": {
        "range": grid_range(0, total_rows, 0, num_cols),
        "top": thin_border,
        "bottom": thin_border,
        "left": thin_border,
        "right": thin_border,
        "innerHorizontal": thin_border,
        "innerVertical": thin_border,
    }
})

# Thicker borders between sections
thick_border = {"style": "SOLID_MEDIUM", "color": rgb(CHARCOAL)}
section_boundaries = [start for _label, start, _end in section_ranges_main]
for col in section_boundaries:
    requests.append({
        "updateBorders": {
            "range": grid_range(0, total_rows, col, min(col + 1, num_cols)),
            "left": thick_border,
        }
    })
# Right edge
requests.append({
    "updateBorders": {
        "range": grid_range(0, total_rows, num_cols - 1, num_cols),
        "right": thick_border,
    }
})
# Top and bottom thick borders
requests.append({
    "updateBorders": {
        "range": grid_range(0, 1, 0, num_cols),
        "top": thick_border,
    }
})
requests.append({
    "updateBorders": {
        "range": grid_range(totals_row_idx, totals_row_idx + 1, 0, num_cols),
        "bottom": thick_border,
    }
})

# --- Set tight column widths (pixels) ---
col_widths = [COL_WIDTHS[name] for name in COLUMN_HEADERS]
for i, width in enumerate(col_widths):
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": i,
                "endIndex": i + 1,
            },
            "properties": {"pixelSize": width},
            "fields": "pixelSize",
        }
    })

# --- Freeze header rows ---
requests.append({
    "updateSheetProperties": {
        "properties": {
            "sheetId": sheet_id,
            "gridProperties": {
                "frozenRowCount": 3,
                "frozenColumnCount": 2,
                "rowCount": total_rows,
                "columnCount": num_cols,
            },
        },
        "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount,gridProperties.rowCount,gridProperties.columnCount",
    }
})

# Apply Lato font family across the entire sheet (runs last so it wins over broader masks)
requests.append({
    "repeatCell": {
        "range": grid_range(0, total_rows, 0, num_cols),
        "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Lato"}}},
        "fields": "userEnteredFormat.textFormat.fontFamily",
    }
})

# Execute all formatting
target_book.batch_update({"requests": requests})

print("Done! Main sheet updated.")


# --- Logic & Definitions Sheet ---
print("Writing Logic & Definitions sheet...")

# Create or get the sheet
try:
    logic_ws = target_book.worksheet("Logic & Definitions")
except gspread.exceptions.WorksheetNotFound:
    logic_ws = target_book.add_worksheet("Logic & Definitions", rows=50, cols=3)

logic_ws.clear()
logic_id = logic_ws.id

logic_content = [
    ["PULLUS - Egg Purchase vs Sales: Logic & Definitions", "", ""],
    ["", "", ""],
    ["SECTION", "COLUMN", "WHAT IT MEANS"],
    ["", "", ""],
    ["PURCHASE", "", "Eggs bought from suppliers in Kaduna"],
    ["", "Crates Purchased", "Total crates bought that month"],
    ["", "Total Eggs Purchased", "Crates × Eggs per Crate (defaults to 30 when not set in the source sheet)"],
    ["", "Broken Eggs (Purchase)", "Broken on arrival from supplier — cannot be sold. Note: these same eggs are also captured in Broken Eggs (Loss) on the sales side, so reconciliation uses Total Eggs Purchased (not Available)."],
    ["", "Cracked Eggs (Purchase)", "Cracked on arrival — still sold at a lower price"],
    ["", "Eggs Available for Sale", "Total Eggs minus Broken (Purchase). Informational; the P vs S reconciliation uses Total Eggs Purchased."],
    ["", "", ""],
    ["SALES (All Staff)", "", "Actual egg sales to end customers"],
    ["", "", "Victor's Abuja entries are excluded here to avoid double counting."],
    ["", "", "Victor transfers eggs to Femi in Abuja. Femi sells them to customers. If we count both, those eggs are counted twice. So for Abuja, we only use Femi's records. Victor's other sales (e.g. Kano) are included."],
    ["", "Good Eggs Sold", "Whole eggs sold to customers"],
    ["", "Broken Eggs (Loss)", "Broken eggs recorded in sales sheets — losses, not sales. Includes arrival breakage (same eggs as Broken (Purchase))."],
    ["", "Cracked Eggs Sold", "Cracked eggs sold at a lower price"],
    ["", "Total Eggs Sold", "Good + Cracked only. Broken eggs are excluded because they are losses"],
    ["", "Samples", "Eggs given out as samples (Status = 'sample' in the sales sheet). Not counted as sales but still reduce inventory"],
    ["", "", ""],
    ["P vs S", "", "Monthly reconciliation. Surpluses roll forward, deficits are bucketed as unaccounted so a bad month doesn't pollute the next."],
    ["", "Eggs Carried In", "Eggs we had at the start of the month (= prior month's Eggs On Hand). Always 0 or positive."],
    ["", "Surplus / Deficit", "(Total Eggs Purchased + Eggs Carried In) minus Total Eggs Sold minus Broken Eggs (Loss) minus Samples"],
    ["", "", "Positive = real leftover stock at month end — rolls into next month's Eggs Carried In"],
    ["", "", "Negative = sold/lost more than we had — bucketed in Unaccounted, does NOT carry forward"],
    ["", "Unaccounted", "Eggs we couldn't reconcile (= abs(Surplus/Deficit) when negative). Likely data capture errors or actual missing eggs."],
    ["", "Eggs On Hand", "Eggs left in inventory at end of month (= Surplus/Deficit when positive, else 0). The LATEST month's value is our CURRENT HOLDING."],
    ["", "", ""],
    ["VICTOR to FEMI (Abuja)", "", "Egg transfer from Victor in Kaduna to Femi in Abuja"],
    ["", "Victor Eggs Sent", "Whole eggs Victor logged as sent to Abuja (only the 'eggs' product type — cracked/broken Abuja entries from Victor are not tracked here)"],
    ["", "Femi Good Eggs", "Good eggs Femi sold in Abuja"],
    ["", "Femi Broken", "Broken eggs Femi sold in Abuja"],
    ["", "Femi Cracked", "Cracked eggs Femi sold in Abuja"],
    ["", "Transfer Variance", "Victor sent minus Femi's total (good + broken + cracked)"],
    ["", "", "Positive = Femi hasn't sold or recorded everything Victor sent"],
    ["", "", "Negative = Femi sold more than Victor sent that month (using prior stock)"],
    ["", "", ""],
    ["EGG MOVEMENT TRACKER", "", "Separate tracker sheet recording physical shipments from Kaduna to Abuja"],
    ["", "Tracker Shipped", "Eggs loaded and sent from Kaduna"],
    ["", "Tracker Delivered", "Eggs that arrived intact (Shipped minus breakage)"],
    ["", "Transit Broken", "Eggs broken during transport"],
    ["", "Transit Cracked", "Eggs cracked during transport"],
    ["", "Victor Sent vs Tracker Shipped", "Victor's figure minus Tracker shipped - should ideally be zero"],
    ["", "Tracker Delivered vs Femi Sold", "Tracker delivered minus Femi's total sales - should ideally be zero"],
    ["", "", ""],
    ["NOTES", "", ""],
    ["", "1", "Tracker data is filtered to 2026 onwards. Purchase and sales sheets are read as-is — make sure source sheets only contain current data."],
    ["", "2", f"Staff with egg sales currently: {', '.join(sorted(staff_with_egg_sales))}"],
    ["", "3", "Femi records in two places: his Sales Log and the Egg Movement Tracker — these should match"],
]

logic_ws.update(logic_content, "A1")

# Format the logic sheet
logic_requests = []

# Title row
logic_requests.append({
    "repeatCell": {
        "range": {"sheetId": logic_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {
            "backgroundColor": rgb(DARK_NAVY),
            "textFormat": {"foregroundColor": rgb(WHITE), "bold": True, "fontSize": 13},
        }},
        "fields": "userEnteredFormat",
    }
})
# Merge title
logic_requests.append({
    "mergeCells": {
        "range": {"sheetId": logic_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 3},
        "mergeType": "MERGE_ALL",
    }
})
# Header row (row 3)
logic_requests.append({
    "repeatCell": {
        "range": {"sheetId": logic_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 0, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {
            "backgroundColor": rgb(LIGHT_GRAY),
            "textFormat": {"bold": True, "foregroundColor": rgb(CHARCOAL)},
        }},
        "fields": "userEnteredFormat",
    }
})
# Section name cells -- bold with section colors
section_rows_colors = [
    (4, DEEP_TEAL),    # PURCHASE
    (11, STEEL_BLUE),  # SALES
    (20, DARK_AMBER),  # P vs S
    (28, DEEP_PURPLE), # VICTOR to FEMI
    (37, DARK_BROWN),  # TRACKER
    (45, DARK_GRAY),   # NOTES
]
for row_idx, color in section_rows_colors:
    logic_requests.append({
        "repeatCell": {
            "range": {"sheetId": logic_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {
                "backgroundColor": rgb(color),
                "textFormat": {"foregroundColor": rgb(WHITE), "bold": True},
            }},
            "fields": "userEnteredFormat",
        }
    })

# Column widths
logic_col_widths = [200, 200, 600]
for i, w in enumerate(logic_col_widths):
    logic_requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": logic_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w},
            "fields": "pixelSize",
        }
    })

# Wrap text on column C
logic_requests.append({
    "repeatCell": {
        "range": {"sheetId": logic_id, "startRowIndex": 0, "endRowIndex": len(logic_content), "startColumnIndex": 2, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
        "fields": "userEnteredFormat.wrapStrategy",
    }
})

# Trim grid
logic_requests.append({
    "updateSheetProperties": {
        "properties": {
            "sheetId": logic_id,
            "gridProperties": {
                "rowCount": len(logic_content),
                "columnCount": 3,
            },
        },
        "fields": "gridProperties.rowCount,gridProperties.columnCount",
    }
})

# Apply Lato font family across the logic sheet (runs last so it wins over broader masks)
logic_requests.append({
    "repeatCell": {
        "range": {"sheetId": logic_id, "startRowIndex": 0, "endRowIndex": len(logic_content), "startColumnIndex": 0, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Lato"}}},
        "fields": "userEnteredFormat.textFormat.fontFamily",
    }
})

target_book.batch_update({"requests": logic_requests})

print("Done! Logic & Definitions sheet created.")


# --- Quick Guide Sheet (management-friendly explainer) ---
print("Writing Quick Guide sheet...")

try:
    guide_ws = target_book.worksheet("Quick Guide")
except gspread.exceptions.WorksheetNotFound:
    guide_ws = target_book.add_worksheet("Quick Guide", rows=50, cols=3)

guide_ws.clear()
guide_id = guide_ws.id

guide_content = [
    ["PULLUS Egg Dashboard — Quick Guide", "", ""],
    ["", "", ""],
    ["AT A GLANCE", "", ""],
    ["", "Current Holding", "Top of the main dashboard. Tells you how many eggs we physically have in stock right now."],
    ["", "TOTAL row, Unaccounted", "Should be 0. Higher means eggs we can't explain — investigate."],
    ["", "", ""],
    ["THREE METRICS TO WATCH", "", ""],
    ["", "Unaccounted", "Eggs we can't reconcile. Each non-zero value is either a data entry error or an actual missing egg."],
    ["", "Transfer Variance", "Gap between what Victor sent to Abuja and what Femi recorded as received. Should be 0."],
    ["", "Tracker variances (last two columns)", "Gap between physical shipment records and sales. Should be 0."],
    ["", "", ""],
    ["WHAT EACH SECTION MEANS", "", ""],
    ["", "Teal — PURCHASE", "What we bought from suppliers in Kaduna"],
    ["", "Blue — SALES (All Staff)", "What we sold across all staff sheets — broken, cracked, samples included"],
    ["", "Amber — P vs S", "Reconciliation. Surpluses roll forward as Eggs Carried In; real losses get bucketed in Unaccounted."],
    ["", "Purple — VICTOR → FEMI", "Internal egg transfers from Kaduna to Abuja"],
    ["", "Brown — EGG MOVEMENT TRACKER", "Physical shipment ground truth from the tracker sheet"],
    ["", "", ""],
    ["NUMBER COLORS", "", ""],
    ["", "Green bold", "Positive — surplus or reconciled. Good."],
    ["", "Red bold", "Negative — deficit or variance. Needs attention."],
    ["", "", ""],
    ["WORKED EXAMPLE", "", ""],
    ["", "Feb: bought 19,800, used 19,200", "Surplus of 600 eggs left over. Carried into March."],
    ["", "Mar: bought 24,960, used 25,560", "The 600 from Feb covered the gap — net 0, no real loss."],
    ["", "If 50 eggs go truly missing later", "Unaccounted will show 50 in that month's row. That's your alert."],
    ["", "", ""],
    ["RED FLAGS", "", ""],
    ["", "Unaccounted > 0", "Check that month's sales sheets, broken-loss entries, and sample records"],
    ["", "Transfer Variance keeps growing", "Sit Victor and Femi together to reconcile their logs"],
    ["", "Eggs On Hand keeps growing", "Sales lagging? Stock sitting too long? Theft hiding as inventory?"],
    ["", "", ""],
    ["NEED MORE DETAIL?", "", ""],
    ["", "Logic & Definitions tab", "Column-by-column technical reference"],
]

guide_ws.update(guide_content, "A1")

# Format the guide sheet
guide_requests = []

# Title row
guide_requests.append({
    "repeatCell": {
        "range": {"sheetId": guide_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {
            "backgroundColor": rgb(DARK_NAVY),
            "textFormat": {"foregroundColor": rgb(WHITE), "bold": True, "fontSize": 14},
            "horizontalAlignment": "CENTER",
        }},
        "fields": "userEnteredFormat",
    }
})
guide_requests.append({
    "mergeCells": {
        "range": {"sheetId": guide_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 3},
        "mergeType": "MERGE_ALL",
    }
})

# Section header rows (col A has bold text, merged across 3 cols)
section_header_rows_guide = [
    (2, STEEL_BLUE),    # AT A GLANCE
    (6, DARK_AMBER),    # THREE METRICS TO WATCH
    (11, DEEP_TEAL),    # WHAT EACH SECTION MEANS
    (18, DARK_GRAY),    # NUMBER COLORS
    (22, STEEL_BLUE),   # WORKED EXAMPLE
    (27, "#CC0000"),    # RED FLAGS (warning red)
    (32, DEEP_PURPLE),  # NEED MORE DETAIL?
]
for row_idx, color in section_header_rows_guide:
    guide_requests.append({
        "repeatCell": {
            "range": {"sheetId": guide_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 0, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {
                "backgroundColor": rgb(color),
                "textFormat": {"foregroundColor": rgb(WHITE), "bold": True, "fontSize": 11},
            }},
            "fields": "userEnteredFormat",
        }
    })
    guide_requests.append({
        "mergeCells": {
            "range": {"sheetId": guide_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 0, "endColumnIndex": 3},
            "mergeType": "MERGE_ALL",
        }
    })

# Column widths
guide_col_widths = [60, 280, 600]
for i, w in enumerate(guide_col_widths):
    guide_requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": guide_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w},
            "fields": "pixelSize",
        }
    })

# Wrap text on column C
guide_requests.append({
    "repeatCell": {
        "range": {"sheetId": guide_id, "startRowIndex": 0, "endRowIndex": len(guide_content), "startColumnIndex": 2, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}},
        "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
    }
})

# Bold for column B labels
guide_requests.append({
    "repeatCell": {
        "range": {"sheetId": guide_id, "startRowIndex": 0, "endRowIndex": len(guide_content), "startColumnIndex": 1, "endColumnIndex": 2},
        "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "foregroundColor": rgb(CHARCOAL)}}},
        "fields": "userEnteredFormat.textFormat(bold,foregroundColor)",
    }
})

# Trim grid
guide_requests.append({
    "updateSheetProperties": {
        "properties": {
            "sheetId": guide_id,
            "gridProperties": {
                "rowCount": len(guide_content),
                "columnCount": 3,
            },
        },
        "fields": "gridProperties.rowCount,gridProperties.columnCount",
    }
})

# Apply Lato across the guide sheet
guide_requests.append({
    "repeatCell": {
        "range": {"sheetId": guide_id, "startRowIndex": 0, "endRowIndex": len(guide_content), "startColumnIndex": 0, "endColumnIndex": 3},
        "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Lato"}}},
        "fields": "userEnteredFormat.textFormat.fontFamily",
    }
})

target_book.batch_update({"requests": guide_requests})

print("Done! Quick Guide sheet created.")


# --- Management Dashboard (subset to secondary book) ---
MGMT_TARGET = PIPELINE_CONFIG.get("management_target_spreadsheet_id")
if MGMT_TARGET and rows:
    print("\nWriting Management Dashboard subset...")

    # Trim to Period + PURCHASE + SALES + P vs S (everything before VICTOR section).
    MGMT_END_COL = COL["Victor Eggs Sent (Abuja)"]  # first col we want to exclude
    mgmt_headers = COLUMN_HEADERS[:MGMT_END_COL]
    mgmt_num_cols = len(mgmt_headers)
    mgmt_section_ranges = compute_section_ranges(mgmt_headers)

    mgmt_book = gc.open_by_key(MGMT_TARGET)
    try:
        mgmt_ws = mgmt_book.worksheet("Egg Purchase vs Sales")
    except gspread.exceptions.WorksheetNotFound:
        mgmt_ws = mgmt_book.add_worksheet("Egg Purchase vs Sales", rows=50, cols=mgmt_num_cols)
    mgmt_id = mgmt_ws.id

    # Unmerge any existing merged cells
    mgmt_book.batch_update({"requests": [{
        "unmergeCells": {
            "range": {
                "sheetId": mgmt_id,
                "startRowIndex": 0,
                "endRowIndex": mgmt_ws.row_count,
                "startColumnIndex": 0,
                "endColumnIndex": mgmt_ws.col_count,
            }
        }
    }]})

    # Build output
    mgmt_output = []
    mgmt_title_row = ["", ""] + [title_text] + [""] * (mgmt_num_cols - 3)
    mgmt_output.append(mgmt_title_row)
    mgmt_section_row = [""] * mgmt_num_cols
    for label, start, _end in mgmt_section_ranges:
        mgmt_section_row[start] = label
    mgmt_output.append(mgmt_section_row)
    mgmt_output.append(mgmt_headers)
    for r in rows:
        out_row = [r[0], int(r[1])]
        for v in r[2:MGMT_END_COL]:
            out_row.append(int(v) if v == int(v) else v)
        mgmt_output.append(out_row)
    # Totals row with same special-cases
    mgmt_totals_row = ["TOTAL", ""]
    for col_idx in range(2, mgmt_num_cols):
        if col_idx == COL["Eggs Carried In"]:
            mgmt_totals_row.append(0)
        elif col_idx == COL["Eggs On Hand"]:
            mgmt_totals_row.append(int(rows[-1][col_idx]))
        elif col_idx == COL["Surplus / Deficit"]:
            on_hand_end = int(rows[-1][COL["Eggs On Hand"]])
            unaccounted_total = int(sum(r[COL["Unaccounted"]] for r in rows))
            mgmt_totals_row.append(on_hand_end - unaccounted_total)
        else:
            mgmt_totals_row.append(int(sum(r[col_idx] for r in rows)))
    mgmt_output.append(mgmt_totals_row)

    mgmt_ws.clear()
    mgmt_ws.update(mgmt_output, "A1")

    mgmt_total_rows = len(mgmt_output)
    mgmt_totals_row_idx = mgmt_total_rows - 1

    def mgmt_grid(start_row, end_row, start_col, end_col):
        return {
            "sheetId": mgmt_id,
            "startRowIndex": start_row,
            "endRowIndex": end_row,
            "startColumnIndex": start_col,
            "endColumnIndex": end_col,
        }

    mgmt_requests = []

    # Merges
    mgmt_requests.append({
        "mergeCells": {"range": mgmt_grid(0, 1, 2, mgmt_num_cols), "mergeType": "MERGE_ALL"}
    })
    for _label, start, end in mgmt_section_ranges:
        mgmt_requests.append({
            "mergeCells": {"range": mgmt_grid(1, 2, start, end), "mergeType": "MERGE_ALL"}
        })

    # Title row format
    mgmt_requests.append({
        "repeatCell": {
            "range": mgmt_grid(0, 1, 0, mgmt_num_cols),
            "cell": {"userEnteredFormat": cell_format(DARK_NAVY, WHITE, bold=True, font_size=14, h_align="CENTER")},
            "fields": "userEnteredFormat",
        }
    })
    # Title text runs (reuse holding_run_start/timestamp_run_start computed for main)
    mgmt_requests.append({
        "updateCells": {
            "range": mgmt_grid(0, 1, 2, 3),
            "rows": [{
                "values": [{
                    "textFormatRuns": [
                        {"startIndex": 0, "format": {"fontFamily": "Lato", "fontSize": 14, "bold": True, "foregroundColor": rgb(WHITE)}},
                        {"startIndex": holding_run_start, "format": {"fontFamily": "Lato", "fontSize": 11, "bold": True, "foregroundColor": rgb(WHITE)}},
                        {"startIndex": timestamp_run_start, "format": {"fontFamily": "Lato", "fontSize": 9, "bold": False, "italic": True, "foregroundColor": rgb("#B0B0B0")}},
                    ]
                }]
            }],
            "fields": "textFormatRuns",
        }
    })

    # Section header row
    for label, start, end in mgmt_section_ranges:
        mgmt_requests.append({
            "repeatCell": {
                "range": mgmt_grid(1, 2, start, end),
                "cell": {"userEnteredFormat": cell_format(SECTION_COLORS[label][0], WHITE, bold=True, font_size=11, h_align="CENTER")},
                "fields": "userEnteredFormat",
            }
        })

    # Column header row
    for label, start, end in mgmt_section_ranges:
        fmt = cell_format(SECTION_COLORS[label][1], CHARCOAL, bold=True, h_align="CENTER")
        fmt["wrapStrategy"] = "WRAP"
        mgmt_requests.append({
            "repeatCell": {
                "range": mgmt_grid(2, 3, start, end),
                "cell": {"userEnteredFormat": fmt},
                "fields": "userEnteredFormat",
            }
        })

    # Data row formatting
    for i in range(len(rows)):
        row_idx = 3 + i
        bg = ROW_WHITE if i % 2 == 0 else ROW_ALT
        mgmt_requests.append({
            "repeatCell": {
                "range": mgmt_grid(row_idx, row_idx + 1, 0, mgmt_num_cols),
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": rgb(bg),
                        "horizontalAlignment": "CENTER",
                        "textFormat": {"bold": False, "foregroundColor": rgb(CHARCOAL)},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)",
            }
        })
        mgmt_requests.append({
            "repeatCell": {
                "range": mgmt_grid(row_idx, row_idx + 1, 2, mgmt_num_cols),
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                        "backgroundColor": rgb(bg),
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat(numberFormat,backgroundColor,horizontalAlignment)",
            }
        })
        # Variance coloring on any variance column that falls inside the subset
        for vc_name in VARIANCE_COLS:
            vc_idx = COL[vc_name]
            if vc_idx >= mgmt_num_cols:
                continue
            val = rows[i][vc_idx]
            text_color = "#0A7A0A" if val >= 0 else "#CC0000"
            mgmt_requests.append({
                "repeatCell": {
                    "range": mgmt_grid(row_idx, row_idx + 1, vc_idx, vc_idx + 1),
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"foregroundColor": rgb(text_color), "bold": True},
                            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                            "backgroundColor": rgb(bg),
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat",
                }
            })

    # Totals row formatting
    mgmt_requests.append({
        "repeatCell": {
            "range": mgmt_grid(mgmt_totals_row_idx, mgmt_totals_row_idx + 1, 0, mgmt_num_cols),
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": rgb(LIGHT_GRAY),
                    "textFormat": {"bold": True, "foregroundColor": rgb(DARK_NAVY)},
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })
    mgmt_requests.append({
        "repeatCell": {
            "range": mgmt_grid(mgmt_totals_row_idx, mgmt_totals_row_idx + 1, 2, mgmt_num_cols),
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    "backgroundColor": rgb(LIGHT_GRAY),
                    "textFormat": {"bold": True, "foregroundColor": rgb(DARK_NAVY)},
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat",
        }
    })
    for vc_name in VARIANCE_COLS:
        vc_idx = COL[vc_name]
        if vc_idx >= mgmt_num_cols:
            continue
        val = mgmt_totals_row[vc_idx]
        text_color = "#0A7A0A" if val >= 0 else "#CC0000"
        mgmt_requests.append({
            "repeatCell": {
                "range": mgmt_grid(mgmt_totals_row_idx, mgmt_totals_row_idx + 1, vc_idx, vc_idx + 1),
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"foregroundColor": rgb(text_color), "bold": True},
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                        "backgroundColor": rgb(LIGHT_GRAY),
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat",
            }
        })
    mgmt_requests.append({
        "updateBorders": {
            "range": mgmt_grid(mgmt_totals_row_idx, mgmt_totals_row_idx + 1, 0, mgmt_num_cols),
            "top": {"style": "SOLID_MEDIUM", "color": rgb(CHARCOAL)},
        }
    })

    # Borders
    thin = {"style": "SOLID", "color": rgb("#D0D0D0")}
    mgmt_requests.append({
        "updateBorders": {
            "range": mgmt_grid(0, mgmt_total_rows, 0, mgmt_num_cols),
            "top": thin, "bottom": thin, "left": thin, "right": thin,
            "innerHorizontal": thin, "innerVertical": thin,
        }
    })
    thick = {"style": "SOLID_MEDIUM", "color": rgb(CHARCOAL)}
    for c in [start for _label, start, _end in mgmt_section_ranges]:
        mgmt_requests.append({
            "updateBorders": {
                "range": mgmt_grid(0, mgmt_total_rows, c, min(c + 1, mgmt_num_cols)),
                "left": thick,
            }
        })
    mgmt_requests.append({
        "updateBorders": {
            "range": mgmt_grid(0, mgmt_total_rows, mgmt_num_cols - 1, mgmt_num_cols),
            "right": thick,
        }
    })
    mgmt_requests.append({
        "updateBorders": {"range": mgmt_grid(0, 1, 0, mgmt_num_cols), "top": thick}
    })
    mgmt_requests.append({
        "updateBorders": {"range": mgmt_grid(mgmt_totals_row_idx, mgmt_totals_row_idx + 1, 0, mgmt_num_cols), "bottom": thick}
    })

    # Column widths (looked up by name)
    for i, name in enumerate(mgmt_headers):
        mgmt_requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": mgmt_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": COL_WIDTHS[name]},
                "fields": "pixelSize",
            }
        })

    # Freeze rows + first 2 cols, trim grid
    mgmt_requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": mgmt_id,
                "gridProperties": {
                    "frozenRowCount": 3,
                    "frozenColumnCount": 2,
                    "rowCount": mgmt_total_rows,
                    "columnCount": mgmt_num_cols,
                },
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount,gridProperties.rowCount,gridProperties.columnCount",
        }
    })

    # Apply Lato across the management sheet (last so it wins over broader masks)
    mgmt_requests.append({
        "repeatCell": {
            "range": mgmt_grid(0, mgmt_total_rows, 0, mgmt_num_cols),
            "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Lato"}}},
            "fields": "userEnteredFormat.textFormat.fontFamily",
        }
    })

    mgmt_book.batch_update({"requests": mgmt_requests})
    print("Done! Management Dashboard written to secondary book.")


# --- Send Breakage Alerts to Google Space ---
if breakage_alerts and GOOGLE_SPACE_WEBHOOK_URL:
    print("\nProcessing breakage alerts...")

    # Load previously alerted shipments from data state
    alerted_set = set()
    if IS_CI and previous_state:
        alerted_set = set(previous_state.get("alerted_shipments", []))

    # Build unique keys and filter out already-alerted shipments
    new_alerts = []
    for alert in breakage_alerts:
        key = f"{alert['tab']}|{alert['date']}|{alert['customer']}|{alert['shipped']}"
        if key not in alerted_set:
            new_alerts.append((key, alert))

    if new_alerts:
        # Group alerts by tab
        grouped = {}
        for key, alert in new_alerts:
            tab = alert["tab"]
            if tab not in grouped:
                grouped[tab] = []
            grouped[tab].append((key, alert))

        # Build card message
        sections = []
        for tab, tab_alerts in grouped.items():
            widgets = []
            for key, a in tab_alerts:
                broken_color = "#CC0000" if a["broken_pct"] > BREAKAGE_THRESHOLD else "#4A4A4A"
                cracked_color = "#CC0000" if a["cracked_pct"] > BREAKAGE_THRESHOLD else "#4A4A4A"
                widgets.append({"decoratedText": {
                    "startIcon": {"materialIcon": {"name": "warning", "fill": True}},
                    "topLabel": f"{a['date']}  \u2022  {a['customer']}",
                    "text": (
                        f"Shipped: <b>{a['shipped']:,}</b>  |  "
                        f"Broken: <font color=\"{broken_color}\"><b>{a['broken']:,} ({a['broken_pct']:.2f}%)</b></font>  |  "
                        f"Cracked: <font color=\"{cracked_color}\"><b>{a['cracked']:,} ({a['cracked_pct']:.2f}%)</b></font>"
                    ),
                    "wrapText": True,
                }})
            sections.append({
                "header": tab.upper(),
                "widgets": widgets,
            })

        card_payload = {
            "cardsV2": [{
                "cardId": "breakage-alert",
                "card": {
                    "header": {
                        "title": "Egg Breakage/Cracking Alert",
                        "subtitle": f"Threshold: {BREAKAGE_THRESHOLD}%  |  {len(new_alerts)} shipment(s) flagged",
                    },
                    "sectionDividerStyle": "SOLID_DIVIDER",
                    "sections": sections,
                },
            }],
        }
        payload = json.dumps(card_payload).encode("utf-8")

        req = urllib.request.Request(
            GOOGLE_SPACE_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json; charset=UTF-8"},
            method="POST",
        )
        alert_sent = False
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                print(f"  Alert sent to Google Space (HTTP {resp.status})")
                alert_sent = True
        except Exception as e:
            print(f"  WARNING: Failed to send alert to Google Space: {type(e).__name__}")

        # Only mark as alerted if the POST succeeded
        if alert_sent:
            for key, _ in new_alerts:
                alerted_set.add(key)

        if IS_CI:
            new_state["alerted_shipments"] = sorted(alerted_set)
    else:
        print("  All flagged shipments already alerted. No new alerts to send.")
        if IS_CI:
            new_state["alerted_shipments"] = sorted(alerted_set)
elif breakage_alerts:
    print(f"\n  {len(breakage_alerts)} breakage alerts found but GOOGLE_SPACE_WEBHOOK_URL not set. Skipping.")
else:
    print("\n  No breakage alerts to send.")

# Preserve alerted_shipments in state when alerts weren't processed
if IS_CI and "alerted_shipments" not in new_state and previous_state:
    new_state["alerted_shipments"] = previous_state.get("alerted_shipments", [])

# Save data state for the workflow to commit (CI only)
if IS_CI:
    save_data_state(new_state)

