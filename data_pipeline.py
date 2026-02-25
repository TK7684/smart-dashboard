"""
Shopee Dashboard Data Pipeline
===============================
Combines and processes data from 4 folders:
- Shopee Ad
- Shopee Live
- Shopee orders
- Shopee Video

Outputs master datasets for Looker Studio dashboard
"""

import pandas as pd
import numpy as np
import os
import re
import glob
import sys
from datetime import datetime, timedelta
from pathlib import Path
import warnings
import duckdb

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURATION
# ==========================================
# Use script location as base directory (works on both Windows and Linux)
BASE_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = BASE_DIR / "processed_data"

# Source folders - support both Windows (spaces) and Linux (underscores) naming
def get_source_dir(name_with_space):
    """Return the directory that exists (with space or underscore)"""
    dir_with_underscore = BASE_DIR / name_with_space.replace(" ", "_")
    dir_with_space = BASE_DIR / name_with_space
    return dir_with_underscore if dir_with_underscore.exists() else dir_with_space

ORDERS_DIR = get_source_dir("Shopee orders")
ADS_DIR = get_source_dir("Shopee Ad")
LIVE_DIR = get_source_dir("Shopee Live")
VIDEO_DIR = get_source_dir("Shopee Video")
TIKTOK_LIVE_DIR = get_source_dir("Tiktok Live")
TIKTOK_VIDEO_DIR = get_source_dir("Tiktok Video")

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

# Column name mappings (Thai to English)
ORDER_COLUMN_MAP = {
    'หมายเลขคำสั่งซื้อ': 'Order_ID',
    'สถานะการสั่งซื้อ': 'Order_Status',
    'วันที่ทำการสั่งซื้อ': 'Order_Date',
    'ชื่อสินค้า': 'Product_Name',
    'เลขอ้างอิง SKU (SKU Reference No.)': 'SKU',
    'ราคาตั้งต้น': 'Original_Price',
    'ราคาขาย': 'Selling_Price',
    'จำนวน': 'Quantity',
    'จำนวนที่ส่งคืน': 'Return_Qty',
    'ราคาขายสุทธิ': 'Net_Sales',
    'ส่วนลดจาก Shopee': 'Shopee_Discount',
    'โค้ดส่วนลดชำระโดยผู้ขาย': 'Seller_Discount',
    'โค้ดส่วนลดชำระโดย Shopee (เช่น โค้ดจากโปรแกรม ร้านโค้ดคุ้ม, โค้ดส่วนลด Shopee, โค้ดส่วนลด Shopee Mall)': 'Shopee_Voucher',
    'ค่าคอมมิชชั่น': 'Commission',
    'Transaction Fee': 'Transaction_Fee',
    'ค่าบริการ': 'Service_Fee',
    'ค่าจัดส่งที่ชำระโดยผู้ซื้อ': 'Shipping_Fee',
    'จำนวนเงินทั้งหมด': 'Total_Amount',
    'จังหวัด': 'Province',
    'เขต/อำเภอ': 'District',
    'รหัสไปรษณีย์': 'Postal_Code',
    'ช่องทางการชำระเงิน': 'Payment_Method',
    'ชื่อผู้ใช้ (ผู้ซื้อ)': 'Buyer_Username',
}

# Valid order statuses (Thai)
VALID_STATUSES = ['สำเร็จแล้ว', 'ที่ต้องจัดส่ง', 'กำลังจัดส่ง', 'รอการชำระเงิน']

# Keywords to exclude (non-skincare/supplement products)
EXCLUDE_KEYWORDS = ['iphone', 'ipad', 'apple', 'samsung', 'phone', 'case', 'cable',
                    'charger', 'headphone', 'earphone', 'laptop', 'computer']

# TikTok column name mappings (Thai to English)
TIKTOK_LIVE_COLUMN_MAP = {
    'ครีเอเตอร์ ID': 'Creator_ID',
    'ครีเอเตอร์': 'Creator',
    'ชื่อเล่น': 'Nickname',
    'เริ่ม': 'Start_Time',
    'ระยะเวลา': 'Duration',
    'มูลค่าสินค้ารวมจาก LIVE (฿)': 'GMV',
    'สินค้าที่เพิ่ม': 'Products_Added',
    'ผลิตภัณฑ์ต่าง ๆ ที่ขาย': 'Products_Sold',
    'คำสั่งซื้อ SKU ที่สร้างขึ้น': 'Orders_Created',
    'คำสั่งซื้อ SKU จาก LIVE': 'Orders',
    'รายการจาก LIVE ที่ขายได้': 'Items_Sold',
    'ลูกค้าที่ไม่ซ้ำกัน': 'Unique_Customers',
    'ราคาเฉลี่ย (฿)': 'Avg_Price',
    'อัตราการคลิกเพื่อสั่งซื้อ (LIVE)': 'CTR',
    'GMV ที่มาจากไลฟ์ (฿)': 'Live_GMV',
    'ผู้ชม': 'Viewers',
    'ยอดการดู': 'Views',
    'ระยะเวลาการดูโดยเฉลี่ย (ไลฟ์สตรีม)': 'Avg_Watch_Time',
    'ความคิดเห็น': 'Comments',
    'การแชร์': 'Shares',
    'ยอดการถูกใจของ LIVE': 'Likes',
    'ผู้ติดตามใหม่ (วิดีโอครีเอเตอร์)': 'New_Followers',
    'การแสดงผลสินค้า': 'Product_Impressions',
    'การคลิกผลิตภัณฑ์': 'Product_Clicks',
    'อัตราการคลิกผ่าน (CTR)': 'Click_Through_Rate',
}

TIKTOK_VIDEO_COLUMN_MAP = {
    'ชื่อครีเอเตอร์': 'Creator',
    'ครีเอเตอร์ ID': 'Creator_ID',
    'ข้อมูลวิดีโอ': 'Video_Title',
    'วิดีโอ ID': 'Video_ID',
    'เวลา': 'Post_Time',
    'สินค้า': 'Product',
    'VV': 'Views',
    'การกดถูกใจ': 'Likes',
    'ความคิดเห็น': 'Comments',
    'การแชร์': 'Shares',
    'ผู้ติดตามใหม่': 'New_Followers',
    'การคลิก V-to-L': 'V_to_L_Clicks',
    'การแสดงผลสินค้า': 'Product_Impressions',
    'การคลิกผลิตภัณฑ์': 'Product_Clicks',
    'ลูกค้าที่ไม่ซ้ำกัน': 'Unique_Customers',
    'คำสั่งซื้อ': 'Orders',
    'รายการในวิดีโอที่ขายได้': 'Items_Sold',
    'มูลค่าสินค้ารวม (วิดีโอ) (฿)': 'GMV',
    'GPM (฿)': 'GPM',
    'GMV ที่มาจากวิดีโอขายสินค้า (฿)': 'Video_Sales_GMV',
    'อัตราการคลิกผ่าน (วิดีโอ)': 'Video_CTR',
    'อัตรา V-to-L': 'V_to_L_Rate',
    'อัตราการดูวิดีโอจนจบ': 'Completion_Rate',
    'อัตราการคลิกเพื่อสั่งซื้อ (วิดีโอ)': 'Conversion_Rate',
    'การวินิจฉัย': 'Diagnosis',
}

# TikTok Video alternate format (Video_List files)
TIKTOK_VIDEO_ALT_COLUMN_MAP = {
    'ชื่อวิดีโอ': 'Video_Title',
    'ลิงก์วิดีโอ': 'Video_Link',
    'วันที่โพสต์วิดีโอ': 'Post_Date',
    'ชื่อผู้ใช้ของครีเอเตอร์': 'Creator',
    'GMV': 'GMV',
    'จำนวนที่ขายได้จากแอฟฟิลิเอต': 'Items_Sold',
    'GMV จากวิดีโอขายสินค้าของแอฟฟิลิเอต': 'Video_Sales_GMV',
    'มูลค่าคำสั่งซื้อเฉลี่ยจากวิดีโอขายสินค้า': 'Avg_Order_Value',
    'ค่าคอมมิชชั่นโดยประมาณ': 'Commission',
    'ค่าธรรมเนียมคงที่โดยประมาณ': 'Fixed_Fee',
    'คำสั่งซื้อแอฟฟิลิเอต': 'Orders',
    'ยอดการแสดงผลวิดีโอขายสินค้า': 'Product_Impressions',
    'CTR จากแอฟฟิลิเอต': 'CTR',
    'GPM จากวิดีโอขายสินค้า': 'GPM',
    'จำนวนสินค้าแอฟฟิลิเอตที่คืนเงิน': 'Refund_Items',
    'GMV ของการคืนเงินจากแอฟฟิลิเอต': 'Refund_GMV',
    'ความคิดเห็นในวิดีโอขายสินค้า': 'Comments',
    'การกดถูกใจในวิดีโอขายสินค้า': 'Likes',
}


# ==========================================
# UTILITY FUNCTIONS
# ==========================================
def parse_thai_currency(value):
    """Convert Thai currency string to float"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # Remove currency symbol and commas
    value = str(value).replace('฿', '').replace(',', '').replace('"', '').strip()
    try:
        return float(value)
    except:
        return 0.0


def parse_percentage(value):
    """Convert percentage string to decimal"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) / 100 if float(value) > 1 else float(value)
    value = str(value).replace('%', '').replace(',', '').strip()
    try:
        result = float(value)
        return result / 100 if result > 1 else result
    except:
        return 0.0


def parse_time_duration(value):
    """Parse time duration strings like '4ชั่วโมง9นาที28วินาที'"""
    if pd.isna(value):
        return 0

    value = str(value)
    hours = minutes = seconds = 0

    # Extract hours
    h_match = re.search(r'(\d+)ชั่วโมง', value)
    if h_match:
        hours = int(h_match.group(1))

    # Extract minutes
    m_match = re.search(r'(\d+)นาที', value)
    if m_match:
        minutes = int(m_match.group(1))

    # Extract seconds
    s_match = re.search(r'(\d+)วินาที', value)
    if s_match:
        seconds = int(s_match.group(1))

    return hours * 3600 + minutes * 60 + seconds


def extract_date_from_filename(filename, folder_type):
    """Extract date range from filename based on folder type"""
    filename = str(filename)

    if folder_type == 'orders':
        # Pattern: Order.all.20240101_20240131.xlsx
        match = re.search(r'(\d{8})_(\d{8})', filename)
        if match:
            start = datetime.strptime(match.group(1), '%Y%m%d')
            end = datetime.strptime(match.group(2), '%Y%m%d')
            return start, end

    elif folder_type == 'ads':
        # Pattern: ข้อมูล-Shopee-Ads-01_01_2026-31_01_2026.csv
        match = re.search(r'(\d{2})_(\d{2})_(\d{4})-(\d{2})_(\d{2})_(\d{4})', filename)
        if match:
            start = datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            end = datetime(int(match.group(6)), int(match.group(5)), int(match.group(4)))
            return start, end

    elif folder_type == 'live':
        # Pattern: overview-v2_1m_2026-01-31_...
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
        if match:
            date = datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            return date, date

    elif folder_type == 'video':
        # Pattern: export-sc_0_1m_2026-01-31_...
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
        if match:
            date = datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            return date, date

    return None, None


def get_all_files(directory, extension):
    """Get all files with given extension, excluding desktop.ini"""
    files = []
    for f in glob.glob(str(directory / f"*.{extension}")):
        if 'desktop.ini' not in f.lower():
            files.append(Path(f))
    return sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)


# ==========================================
# DATA LOADING FUNCTIONS
# ==========================================
def load_orders():
    """Load and combine all order files"""
    print("\n📦 Loading Order Data...")
    all_orders = []
    files = get_all_files(ORDERS_DIR, 'xlsx')

    for file in files:
        try:
            df = pd.read_excel(file)
            start_date, end_date = extract_date_from_filename(file.name, 'orders')
            df['File_Source'] = file.name
            df['File_Date_Range'] = f"{start_date} - {end_date}" if start_date else None
            all_orders.append(df)
            print(f"   ✓ {file.name}: {len(df)} records")
        except Exception as e:
            print(f"   ✗ Error loading {file.name}: {e}")

    if not all_orders:
        return pd.DataFrame()

    combined = pd.concat(all_orders, ignore_index=True)
    print(f"   Total: {len(combined)} order records")
    return combined


def load_ads():
    """Load and combine all ads files"""
    print("\n📊 Loading Ads Data...")
    all_ads = []
    files = get_all_files(ADS_DIR, 'csv')

    for file in files:
        try:
            # Read file and find header row
            with open(file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            # Find the line that starts with 'ลำดับ' (sequence number)
            header_row = None
            for i, line in enumerate(lines):
                if line.startswith('ลำดับ') or 'ลำดับ,' in line:
                    header_row = i
                    break

            if header_row is None:
                print(f"   ✗ {file.name}: Could not find header row")
                continue

            # Read CSV starting from header row
            df = pd.read_csv(file, skiprows=header_row, encoding='utf-8')

            # Clean column names
            df.columns = df.columns.str.strip()

            start_date, end_date = extract_date_from_filename(file.name, 'ads')
            df['File_Source'] = file.name
            df['Period_Start'] = start_date
            df['Period_End'] = end_date
            all_ads.append(df)
            print(f"   ✓ {file.name}: {len(df)} records")
        except Exception as e:
            print(f"   ✗ Error loading {file.name}: {e}")

    if not all_ads:
        return pd.DataFrame()

    combined = pd.concat(all_ads, ignore_index=True)
    print(f"   Total: {len(combined)} ads records")
    return combined


def load_live():
    """Load and combine all live streaming files"""
    print("\n🎬 Loading Live Streaming Data...")
    all_live = []
    files = get_all_files(LIVE_DIR, 'csv')

    for file in files:
        try:
            df = pd.read_csv(file, encoding='utf-8', header=None)

            # Parse the complex structure - extract main metrics from row 3
            if len(df) >= 3:
                # Get headers from row 1 and 2
                headers_row1 = df.iloc[0].fillna('')
                headers_row2 = df.iloc[1].fillna('')
                data_row = df.iloc[2]

                # Create combined headers
                combined_headers = []
                current_header = ''
                for h1, h2 in zip(headers_row1, headers_row2):
                    if h1:
                        current_header = str(h1)
                    combined_headers.append(f"{current_header}_{h2}" if h2 else current_header)

                # Create dataframe with data
                row_df = pd.DataFrame([data_row.values], columns=combined_headers)
                row_df['File_Source'] = file.name

                start_date, end_date = extract_date_from_filename(file.name, 'live')
                row_df['Report_Date'] = end_date
                all_live.append(row_df)
                print(f"   ✓ {file.name}")
        except Exception as e:
            print(f"   ✗ Error loading {file.name}: {e}")

    if not all_live:
        return pd.DataFrame()

    combined = pd.concat(all_live, ignore_index=True)
    print(f"   Total: {len(combined)} live records")
    return combined


def load_video():
    """Load and combine all video engagement files"""
    print("\n🎥 Loading Video Engagement Data...")
    all_video = []
    files = get_all_files(VIDEO_DIR, 'csv')

    for file in files:
        try:
            df = pd.read_csv(file, encoding='utf-8', header=None)

            # Parse similar to live data
            if len(df) >= 3:
                headers_row1 = df.iloc[0].fillna('')
                headers_row2 = df.iloc[1].fillna('')
                data_row = df.iloc[2]

                combined_headers = []
                current_header = ''
                for h1, h2 in zip(headers_row1, headers_row2):
                    if h1:
                        current_header = str(h1)
                    combined_headers.append(f"{current_header}_{h2}" if h2 else current_header)

                row_df = pd.DataFrame([data_row.values], columns=combined_headers)
                row_df['File_Source'] = file.name

                start_date, end_date = extract_date_from_filename(file.name, 'video')
                row_df['Report_Date'] = end_date
                all_video.append(row_df)
                print(f"   ✓ {file.name}")
        except Exception as e:
            print(f"   ✗ Error loading {file.name}: {e}")

    if not all_video:
        return pd.DataFrame()

    combined = pd.concat(all_video, ignore_index=True)
    print(f"   Total: {len(combined)} video records")
    return combined


def load_tiktok_live():
    """Load and combine all TikTok live streaming files"""
    print("\n🎬 Loading TikTok Live Streaming Data...")
    all_live = []
    files = get_all_files(TIKTOK_LIVE_DIR, 'xlsx')

    for file in files:
        try:
            # Read Excel with multi-row header
            # Row 0: Date range info (ช่วงวันที่: YYYY-MM-DD ~ YYYY-MM-DD)
            # Row 1: Empty (all NaN)
            # Row 2: Column headers
            # Row 3+: Data
            df = pd.read_excel(file, header=2)

            # Extract date range from first row (skip the header row we used)
            df_preview = pd.read_excel(file, header=None, nrows=1)
            date_info = str(df_preview.iloc[0, 0]) if len(df_preview) > 0 else ""

            # Parse date range from format: "ช่วงวันที่: YYYY-MM-DD ~ YYYY-MM-DD"
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})', date_info)
            if date_match:
                start_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                end_date = datetime.strptime(date_match.group(2), '%Y-%m-%d')
            else:
                # Fallback: try to extract any date from filename or date_info
                date_match = re.search(r'(\d{4})-?(\d{2})-?(\d{2})', file.name)
                if date_match:
                    start_date = end_date = datetime(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))
                else:
                    start_date = end_date = None

            df['File_Source'] = file.name
            df['Report_Date'] = end_date
            df['Report_Start'] = start_date
            all_live.append(df)
            print(f"   ✓ {file.name}: {len(df)} records")
        except Exception as e:
            print(f"   ✗ Error loading {file.name}: {e}")

    if not all_live:
        return pd.DataFrame()

    combined = pd.concat(all_live, ignore_index=True)
    print(f"   Total: {len(combined)} TikTok live records")
    return combined


def load_tiktok_video():
    """Load and combine all TikTok video files (handles two different formats)"""
    print("\n🎥 Loading TikTok Video Data...")
    all_video = []
    files = get_all_files(TIKTOK_VIDEO_DIR, 'xlsx')

    for file in files:
        try:
            # First, detect the file format by checking the first cell
            df_preview = pd.read_excel(file, header=None, nrows=3)
            first_cell = str(df_preview.iloc[0, 0]) if len(df_preview) > 0 else ""

            # Check if it's the alternate format (Video_List files)
            # Alternate format has "ชื่อวิดีโอ" as the first header
            if 'ชื่อวิดีโอ' in first_cell or 'วิดีโอ' in first_cell and 'ช่วงวันที่' not in first_cell:
                # Alternate format: header at row 0
                df = pd.read_excel(file, header=0)
                start_date = end_date = None
            else:
                # Standard format: header at row 2
                df = pd.read_excel(file, header=2)

                # Extract date range from first row
                date_info = first_cell

                # Parse date range
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})', date_info)
                if date_match:
                    start_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                    end_date = datetime.strptime(date_match.group(2), '%Y-%m-%d')
                else:
                    date_match = re.search(r'(\d{4})-?(\d{2})-?(\d{2})', file.name)
                    if date_match:
                        start_date = end_date = datetime(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))
                    else:
                        start_date = end_date = None

            df['File_Source'] = file.name
            df['Report_Date'] = end_date
            df['Report_Start'] = start_date
            all_video.append(df)
            print(f"   ✓ {file.name}: {len(df)} records")
        except Exception as e:
            print(f"   ✗ Error loading {file.name}: {e}")

    if not all_video:
        return pd.DataFrame()

    combined = pd.concat(all_video, ignore_index=True)
    print(f"   Total: {len(combined)} TikTok video records")
    return combined


# ==========================================
# DATA CLEANING & TRANSFORMATION
# ==========================================
def clean_orders(df):
    """Clean and transform order data"""
    print("\n🧹 Cleaning Order Data...")

    if df.empty:
        return df

    # Rename columns
    df = df.rename(columns=ORDER_COLUMN_MAP)

    # Filter valid statuses
    df = df[df['Order_Status'].isin(VALID_STATUSES)]
    print(f"   After status filter: {len(df)} records")

    # Filter out non-skincare/supplement products
    if 'Product_Name' in df.columns:
        pattern = '|'.join(EXCLUDE_KEYWORDS)
        mask = ~df['Product_Name'].str.lower().str.contains(pattern, na=False)
        df = df[mask]
        print(f"   After product filter: {len(df)} records")

    # Parse dates
    if 'Order_Date' in df.columns:
        df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
        df['Order_Date_Only'] = df['Order_Date'].dt.date

    # Convert numeric columns
    numeric_cols = ['Net_Sales', 'Quantity', 'Original_Price', 'Selling_Price',
                    'Commission', 'Transaction_Fee', 'Service_Fee', 'Shipping_Fee',
                    'Total_Amount', 'Shopee_Discount', 'Seller_Discount']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).apply(parse_thai_currency)

    # Calculate true net revenue (after all fees)
    df['Total_Fees'] = df.get('Commission', 0) + df.get('Transaction_Fee', 0) + df.get('Service_Fee', 0)
    df['True_Net_Revenue'] = df.get('Net_Sales', 0) - df['Total_Fees']
    df['Total_Discount'] = df.get('Shopee_Discount', 0) + df.get('Seller_Discount', 0)

    # Add platform identifier
    df['Platform'] = 'Shopee'

    print(f"   Final: {len(df)} cleaned order records")
    return df


def clean_ads(df):
    """Clean and transform ads data"""
    print("\n🧹 Cleaning Ads Data...")

    if df.empty:
        return df

    # Column mappings for ads
    ads_column_map = {
        'ชื่อโฆษณา': 'Ad_Name',
        'สถานะ': 'Status',
        'ประเภทโฆษณา': 'Ad_Type',
        'รหัสสินค้า': 'Product_ID',
        'การมองเห็น': 'Impressions',
        'จำนวนคลิก': 'Clicks',
        'อัตราการคลิก (CTR)': 'CTR',
        'การสั่งซื้อ': 'Orders',
        'การสั่งซื้อโดยตรง': 'Direct_Orders',
        'อัตราการสั่งซื้อ': 'Conversion_Rate',
        'สินค้าที่ขายแล้ว': 'Products_Sold',
        'ยอดขาย': 'Sales',
        'ยอดขายโดยตรง': 'Direct_Sales',
        'ค่าโฆษณา': 'Ad_Cost',
        'ยอดขาย/รายจ่าย (ROAS)': 'ROAS',
        'ACOS': 'ACOS',
    }

    df = df.rename(columns=ads_column_map)

    # Convert numeric columns
    numeric_cols = ['Impressions', 'Clicks', 'Orders', 'Direct_Orders',
                    'Products_Sold', 'Sales', 'Direct_Sales', 'Ad_Cost']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    # Convert percentage columns
    pct_cols = ['CTR', 'Conversion_Rate', 'ACOS']
    for col in pct_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_percentage)
        else:
            df[col] = 0

    # Calculate ROAS if not present or all null
    if 'ROAS' not in df.columns:
        df['ROAS'] = 0

    mask = (df['Ad_Cost'] > 0)
    df.loc[mask, 'ROAS'] = df.loc[mask, 'Sales'] / df.loc[mask, 'Ad_Cost']

    # Calculate ACOS if not present
    if 'ACOS' not in df.columns:
        df['ACOS'] = 0

    mask = (df['Sales'] > 0)
    df.loc[mask, 'ACOS'] = df.loc[mask, 'Ad_Cost'] / df.loc[mask, 'Sales']

    df['Platform'] = 'Shopee'
    print(f"   Final: {len(df)} cleaned ads records")
    return df


def clean_live(df):
    """Clean and transform live streaming data"""
    print("\n🧹 Cleaning Live Streaming Data...")

    if df.empty:
        return df

    # Reset index
    df = df.reset_index(drop=True)

    # Extract key metrics from the complex column names
    # Map common patterns
    metric_map = {}

    for col in df.columns:
        col_str = str(col)
        if 'ระยะเวลาเก็บข้อมูล' in col_str:
            metric_map[col] = 'Report_Period'
        elif 'ยอดขาย(คำสั่งซื้อที่เกิดขึ้น)' in col_str:
            metric_map[col] = 'Sales_Pending'
        elif 'ยอดขาย(คำสั่งซื้อที่ยืนยันแล้ว)' in col_str:
            metric_map[col] = 'Sales_Confirmed'
        elif 'คำสั่งซื้อ(คำสั่งซื้อที่เกิดขึ้น)' in col_str:
            metric_map[col] = 'Orders_Pending'
        elif 'คำสั่งซื้อ(คำสั่งซื้อที่ยืนยันแล้ว)' in col_str:
            metric_map[col] = 'Orders_Confirmed'
        elif 'จำนวน Live ทั้งหมด' in col_str:
            metric_map[col] = 'Total_Live_Sessions'
        elif 'ระยะเวลา Live ทั้งหมด' in col_str:
            metric_map[col] = 'Total_Live_Duration'
        elif 'ผู้ชมทั้งหมด' in col_str:
            metric_map[col] = 'Total_Viewers'
        elif 'PCU' in col_str:
            metric_map[col] = 'Peak_Concurrent_Users'
        elif 'GPM(คำสั่งซื้อที่เกิดขึ้น)' in col_str:
            metric_map[col] = 'GPM_Pending'
        elif 'GPM(คำสั่งซื้อที่ยืนยันแล้ว)' in col_str:
            metric_map[col] = 'GPM_Confirmed'

    df = df.rename(columns=metric_map)

    # Parse Report_Date from File_Source
    if 'File_Source' in df.columns:
        dates = []
        for f in df['File_Source']:
            try:
                if pd.isna(f):
                    dates.append(None)
                else:
                    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(f))
                    if match:
                        dates.append(datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))))
                    else:
                        dates.append(None)
            except:
                dates.append(None)
        df['Report_Date'] = dates

    # Parse duration
    if 'Total_Live_Duration' in df.columns:
        df['Live_Duration_Seconds'] = df['Total_Live_Duration'].apply(parse_time_duration)
        df['Live_Duration_Hours'] = df['Live_Duration_Seconds'] / 3600

    # Convert numeric values
    for col in df.columns:
        if col not in ['File_Source', 'Report_Date', 'Report_Period', 'Total_Live_Duration']:
            try:
                df[col] = df[col].apply(parse_thai_currency)
            except:
                pass

    df['Platform'] = 'Shopee'
    print(f"   Final: {len(df)} cleaned live records")
    return df


def clean_video(df):
    """Clean and transform video engagement data"""
    print("\n🧹 Cleaning Video Engagement Data...")

    if df.empty:
        return df

    # Reset index
    df = df.reset_index(drop=True)

    # Map video-specific columns
    metric_map = {}
    for col in df.columns:
        col_str = str(col)
        if 'ระยะเวลาเก็บข้อมูล' in col_str:
            metric_map[col] = 'Report_Period'
        elif 'ยอดขาย(คำสั่งซื้อที่เกิดขึ้น)' in col_str:
            metric_map[col] = 'Video_Sales_Pending'
        elif 'ยอดขาย(คำสั่งซื้อที่ยืนยันแล้ว)' in col_str:
            metric_map[col] = 'Video_Sales_Confirmed'
        elif 'คำสั่งซื้อ(คำสั่งซื้อที่เกิดขึ้น)' in col_str:
            metric_map[col] = 'Video_Orders_Pending'
        elif 'คำสั่งซื้อ(คำสั่งซื้อที่ยืนยันแล้ว)' in col_str:
            metric_map[col] = 'Video_Orders_Confirmed'
        elif 'ผู้ชมทั้งหมด' in col_str:
            metric_map[col] = 'Total_Viewers'
        elif 'การเข้าชม' in col_str:
            metric_map[col] = 'Total_Views'
        elif 'GPM(คำสั่งซื้อที่เกิดขึ้น)' in col_str:
            metric_map[col] = 'Video_GPM_Pending'
        elif 'GPM(คำสั่งซื้อที่ยืนยันแล้ว)' in col_str:
            metric_map[col] = 'Video_GPM_Confirmed'
        elif 'วิดีโอที่มีสินค้า' in col_str:
            metric_map[col] = 'Videos_With_Products'
        elif 'วิดีโอที่สร้างรายได้' in col_str:
            metric_map[col] = 'Revenue_Generating_Videos'
        elif 'ถูกใจ' in col_str:
            metric_map[col] = 'Total_Likes'
        elif 'แชร์ทั้งหมด' in col_str:
            metric_map[col] = 'Total_Shares'
        elif 'คอมเมนต์ทั้งหมด' in col_str:
            metric_map[col] = 'Total_Comments'
        elif 'ผู้ติดตามใหม่' in col_str:
            metric_map[col] = 'New_Followers'

    df = df.rename(columns=metric_map)

    # Parse Report_Date from File_Source
    if 'File_Source' in df.columns:
        dates = []
        for f in df['File_Source']:
            try:
                if pd.isna(f):
                    dates.append(None)
                else:
                    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(f))
                    if match:
                        dates.append(datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))))
                    else:
                        dates.append(None)
            except:
                dates.append(None)
        df['Report_Date'] = dates

    # Convert numeric values
    for col in df.columns:
        if col not in ['File_Source', 'Report_Date', 'Report_Period']:
            try:
                df[col] = df[col].apply(parse_thai_currency)
            except:
                pass

    df['Platform'] = 'Shopee'
    print(f"   Final: {len(df)} cleaned video records")
    return df


def clean_tiktok_live(df):
    """Clean and transform TikTok live streaming data"""
    print("\n🧹 Cleaning TikTok Live Streaming Data...")

    if df.empty:
        return df

    # Reset index
    df = df.reset_index(drop=True)

    # Rename columns using mapping
    df = df.rename(columns=TIKTOK_LIVE_COLUMN_MAP)

    # Parse duration format (e.g., "7h 12min" or "45min 30s")
    if 'Duration' in df.columns:
        df['Duration_Seconds'] = df['Duration'].apply(parse_tiktok_duration)
        df['Duration_Minutes'] = df['Duration_Seconds'] / 60

    # Convert numeric columns
    numeric_cols = ['GMV', 'Orders', 'Orders_Created', 'Items_Sold', 'Viewers', 'Views',
                    'Comments', 'Shares', 'Likes', 'New_Followers', 'Unique_Customers',
                    'Product_Impressions', 'Product_Clicks', 'Avg_Price', 'Live_GMV']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_thai_currency)
        else:
            df[col] = 0

    # Add platform identifier
    df['Platform'] = 'TikTok'

    # Parse Report_Date
    if 'Report_Date' in df.columns:
        df['Report_Date'] = pd.to_datetime(df['Report_Date'], errors='coerce')

    # Calculate derived metrics
    if 'Views' in df.columns and df['Views'].sum() > 0:
        df['Engagement_Rate'] = (df.get('Likes', 0) + df.get('Comments', 0) + df.get('Shares', 0)) / df['Views']
    else:
        df['Engagement_Rate'] = 0

    # Handle CTR if it exists as string
    if 'CTR' in df.columns:
        df['CTR'] = df['CTR'].apply(parse_percentage)
    if 'Click_Through_Rate' in df.columns:
        df['Click_Through_Rate'] = df['Click_Through_Rate'].apply(parse_percentage)

    print(f"   Final: {len(df)} cleaned TikTok live records")
    return df


def clean_tiktok_video(df):
    """Clean and transform TikTok video data (handles mixed formats)"""
    print("\n🧹 Cleaning TikTok Video Data...")

    if df.empty:
        return df

    # Reset index
    df = df.reset_index(drop=True)

    # Standardize columns - handle both formats
    # First, try to identify rows from each format based on File_Source

    # Map Thai columns to English for each format
    # For standard format (Performance List)
    standard_map = TIKTOK_VIDEO_COLUMN_MAP.copy()
    # For alternate format (Video_List)
    alt_map = TIKTOK_VIDEO_ALT_COLUMN_MAP.copy()

    # Rename columns that match either format
    rename_map = {}
    for col in df.columns:
        if col in standard_map:
            rename_map[col] = standard_map[col]
        elif col in alt_map:
            rename_map[col] = alt_map[col]

    df = df.rename(columns=rename_map)

    # Convert numeric columns - handle mixed types safely
    numeric_cols = ['Views', 'Likes', 'Comments', 'Shares', 'New_Followers',
                    'Orders', 'GMV', 'GPM', 'Video_Sales_GMV', 'Unique_Customers',
                    'Product_Impressions', 'Product_Clicks', 'V_to_L_Clicks', 'Items_Sold',
                    'Commission', 'Fixed_Fee', 'Avg_Order_Value', 'Refund_Items', 'Refund_GMV']

    for col in numeric_cols:
        if col in df.columns:
            # Convert each value safely
            def safe_convert(x):
                try:
                    if pd.isna(x):
                        return 0.0
                    if isinstance(x, (int, float)):
                        return float(x)
                    return parse_thai_currency(x)
                except:
                    return 0.0
            df[col] = df[col].apply(safe_convert)
        else:
            df[col] = 0

    # Convert percentage columns
    pct_cols = ['Video_CTR', 'V_to_L_Rate', 'Completion_Rate', 'Conversion_Rate', 'CTR']
    for col in pct_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_percentage(x) if pd.notna(x) else 0)
        else:
            df[col] = 0

    # Add platform identifier
    df['Platform'] = 'TikTok'

    # Parse Report_Date
    if 'Report_Date' in df.columns:
        df['Report_Date'] = pd.to_datetime(df['Report_Date'], errors='coerce')

    # Parse Post_Time if available
    if 'Post_Time' in df.columns:
        df['Post_Date'] = pd.to_datetime(df['Post_Time'], errors='coerce').dt.date

    # Parse Post_Date if available (from alternate format)
    if 'Post_Date' in df.columns and df['Post_Date'].dtype == 'object':
        df['Post_Date'] = pd.to_datetime(df['Post_Date'], errors='coerce').dt.date

    # Calculate engagement rate
    if 'Views' in df.columns:
        # Handle potential duplicate columns by squeezing to Series
        views_col = df['Views']
        if isinstance(views_col, pd.DataFrame):
            views_col = views_col.iloc[:, 0]
        views = pd.to_numeric(views_col, errors='coerce').fillna(0)

        likes = 0
        if 'Likes' in df.columns:
            likes_col = df['Likes']
            if isinstance(likes_col, pd.DataFrame):
                likes_col = likes_col.iloc[:, 0]
            likes = pd.to_numeric(likes_col, errors='coerce').fillna(0)

        comments = 0
        if 'Comments' in df.columns:
            comments_col = df['Comments']
            if isinstance(comments_col, pd.DataFrame):
                comments_col = comments_col.iloc[:, 0]
            comments = pd.to_numeric(comments_col, errors='coerce').fillna(0)

        shares = 0
        if 'Shares' in df.columns:
            shares_col = df['Shares']
            if isinstance(shares_col, pd.DataFrame):
                shares_col = shares_col.iloc[:, 0]
            shares = pd.to_numeric(shares_col, errors='coerce').fillna(0)

        # Calculate engagement rate safely
        engagement = np.zeros(len(df))
        mask = views > 0
        engagement[mask] = (likes[mask] if isinstance(likes, pd.Series) else likes +
                           comments[mask] if isinstance(comments, pd.Series) else comments +
                           shares[mask] if isinstance(shares, pd.Series) else shares) / views[mask]
        df['Engagement_Rate'] = engagement
    else:
        df['Engagement_Rate'] = 0

    print(f"   Final: {len(df)} cleaned TikTok video records")
    return df


def parse_tiktok_duration(value):
    """Parse TikTok duration format (e.g., '7h 12min', '45min 30s', '2h')"""
    if pd.isna(value):
        return 0

    value = str(value)
    hours = minutes = seconds = 0

    # Extract hours
    h_match = re.search(r'(\d+)\s*h', value, re.IGNORECASE)
    if h_match:
        hours = int(h_match.group(1))

    # Extract minutes
    m_match = re.search(r'(\d+)\s*min', value, re.IGNORECASE)
    if m_match:
        minutes = int(m_match.group(1))

    # Extract seconds
    s_match = re.search(r'(\d+)\s*s', value, re.IGNORECASE)
    if s_match:
        seconds = int(s_match.group(1))

    return hours * 3600 + minutes * 60 + seconds


# ==========================================
# MASTER DATA AGGREGATION
# ==========================================
def create_daily_sales_master(orders_df):
    """Create daily sales aggregated data"""
    print("\n📈 Creating Daily Sales Master...")

    if orders_df.empty:
        return pd.DataFrame()

    # Group by date
    daily = orders_df.groupby(['Order_Date_Only', 'Platform']).agg({
        'Order_ID': 'nunique',
        'Net_Sales': 'sum',
        'True_Net_Revenue': 'sum',
        'Quantity': 'sum',
        'Total_Fees': 'sum',
        'Total_Discount': 'sum',
        'Commission': 'sum',
        'Transaction_Fee': 'sum',
        'Service_Fee': 'sum',
    }).reset_index()

    daily.columns = ['Date', 'Platform', 'Orders', 'GMV', 'Net_Revenue', 'Units_Sold',
                     'Total_Fees', 'Total_Discount', 'Commission', 'Transaction_Fee', 'Service_Fee']

    # Calculate AOV
    daily['AOV'] = daily.apply(lambda x: x['GMV'] / x['Orders'] if x['Orders'] > 0 else 0, axis=1)

    # Sort by date
    daily['Date'] = pd.to_datetime(daily['Date'])
    daily = daily.sort_values(['Platform', 'Date'])

    # Calculate time-based comparisons
    daily['DoD_GMV_Growth'] = daily.groupby('Platform')['GMV'].pct_change(1)
    daily['WoW_GMV_Growth'] = daily.groupby('Platform')['GMV'].pct_change(7)
    daily['MoM_GMV_Growth'] = daily.groupby('Platform')['GMV'].pct_change(30)
    daily['QoQ_GMV_Growth'] = daily.groupby('Platform')['GMV'].pct_change(90)

    # GMV Segmentation (Min/Middle/Max)
    p80_gmv = daily['GMV'].quantile(0.8)
    p20_gmv = daily['GMV'].quantile(0.2)

    conditions = [
        (daily['GMV'] >= p80_gmv),
        (daily['GMV'] <= p20_gmv)
    ]
    choices = ['Max (Top 20%)', 'Min (Bottom 20%)']
    daily['GMV_Segment'] = np.select(conditions, choices, default='Middle')

    # AOV Segmentation
    avg_aov = daily['AOV'].mean()
    conditions_aov = [
        (daily['AOV'] > avg_aov * 1.2),
        (daily['AOV'] < avg_aov * 0.8)
    ]
    choices_aov = ['Max (>20% Avg)', 'Min (<-20% Avg)']
    daily['AOV_Segment'] = np.select(conditions_aov, choices_aov, default='Middle')

    print(f"   Created {len(daily)} daily records")
    return daily


def create_product_master(orders_df):
    """Create product-level aggregated data"""
    print("\n🏷️ Creating Product Master...")

    if orders_df.empty:
        return pd.DataFrame()

    # Group by product and SKU
    group_cols = ['Product_Name', 'Platform']
    if 'SKU' in orders_df.columns:
        group_cols = ['Product_Name', 'SKU', 'Platform']

    products = orders_df.groupby(group_cols).agg({
        'Order_ID': 'nunique',
        'Net_Sales': 'sum',
        'True_Net_Revenue': 'sum',
        'Quantity': 'sum',
        'Total_Discount': 'sum',
    }).reset_index()

    products.columns = ['Product_Name', 'SKU', 'Platform', 'Orders', 'Total_GMV', 'Net_Revenue',
                        'Total_Qty', 'Total_Discount'] if 'SKU' in group_cols else ['Product_Name', 'Platform', 'Orders', 'Total_GMV', 'Net_Revenue',
                        'Total_Qty', 'Total_Discount']

    # Add SKU if not present
    if 'SKU' not in products.columns:
        products['SKU'] = 'N/A'

    # Reorder columns
    products = products[['Product_Name', 'SKU', 'Platform', 'Orders', 'Total_GMV', 'Net_Revenue',
                        'Total_Qty', 'Total_Discount']]

    # Calculate average selling price
    products['Avg_Price'] = products.apply(
        lambda x: x['Total_GMV'] / x['Total_Qty'] if x['Total_Qty'] > 0 else 0, axis=1
    )

    # Product segmentation
    median_gmv = products['Total_GMV'].median()
    median_qty = products['Total_Qty'].median()

    def categorize_product(row):
        if row['Total_GMV'] > median_gmv and row['Total_Qty'] > median_qty:
            return 'Star (High GMV, High Volume)'
        elif row['Total_GMV'] > median_gmv:
            return 'Hero (High GMV)'
        elif row['Total_Qty'] > median_qty:
            return 'Volume (High Volume, Low GMV)'
        else:
            return 'Core (Average)'

    products['Product_Segment'] = products.apply(categorize_product, axis=1)

    # Calculate GMV contribution
    total_gmv = products['Total_GMV'].sum()
    products['GMV_Contribution_%'] = (products['Total_GMV'] / total_gmv * 100).round(2)

    # Risk assessment
    def risk_assessment(segment, contribution):
        if 'Hero' in segment and contribution > 60:
            return '⚠️ RISK: Over-reliance on hero products'
        elif 'Core' in segment and contribution < 25:
            return '⚠️ URGENT: Need to push core products'
        return '✅ Healthy'

    products['Risk_Status'] = products.apply(
        lambda x: risk_assessment(x['Product_Segment'], x['GMV_Contribution_%']), axis=1
    )

    # Sort by GMV
    products = products.sort_values('Total_GMV', ascending=False)

    print(f"   Created {len(products)} product records")
    return products


def create_ads_master(ads_df):
    """Create ads performance master data"""
    print("\n📊 Creating Ads Master...")

    if ads_df.empty:
        return pd.DataFrame()

    # Aggregate by campaign/ad name
    ads_summary = ads_df.groupby(['Ad_Name', 'Ad_Type', 'Status', 'Platform']).agg({
        'Impressions': 'sum',
        'Clicks': 'sum',
        'Orders': 'sum',
        'Direct_Orders': 'sum',
        'Products_Sold': 'sum',
        'Sales': 'sum',
        'Direct_Sales': 'sum',
        'Ad_Cost': 'sum',
    }).reset_index()

    # Calculate metrics
    ads_summary['CTR'] = ads_summary.apply(
        lambda x: x['Clicks'] / x['Impressions'] if x['Impressions'] > 0 else 0, axis=1
    )
    ads_summary['Conversion_Rate'] = ads_summary.apply(
        lambda x: x['Orders'] / x['Clicks'] if x['Clicks'] > 0 else 0, axis=1
    )
    ads_summary['ROAS'] = ads_summary.apply(
        lambda x: x['Sales'] / x['Ad_Cost'] if x['Ad_Cost'] > 0 else 0, axis=1
    )
    ads_summary['ACOS'] = ads_summary.apply(
        lambda x: x['Ad_Cost'] / x['Sales'] if x['Sales'] > 0 else 0, axis=1
    )

    # Campaign efficiency classification
    def classify_campaign(row):
        if row['ROAS'] >= 5:
            return '🚀 Excellent (ROAS ≥ 5)'
        elif row['ROAS'] >= 3:
            return '✅ Good (ROAS 3-5)'
        elif row['ROAS'] >= 1:
            return '⚠️ Break-even (ROAS 1-3)'
        elif row['Orders'] == 0 and row['Clicks'] > 100:
            return '🔴 Bleeding (No sales, high clicks)'
        else:
            return '📊 Needs Monitoring'

    ads_summary['Campaign_Health'] = ads_summary.apply(classify_campaign, axis=1)

    print(f"   Created {len(ads_summary)} campaign records")
    return ads_summary


def create_geographic_master(orders_df):
    """Create geographic distribution data"""
    print("\n🗺️ Creating Geographic Master...")

    if orders_df.empty or 'Province' not in orders_df.columns:
        return pd.DataFrame()

    geo = orders_df.groupby(['Province', 'Platform']).agg({
        'Order_ID': 'nunique',
        'Net_Sales': 'sum',
        'Quantity': 'sum',
    }).reset_index()

    geo.columns = ['Province', 'Platform', 'Orders', 'GMV', 'Units_Sold']
    geo = geo.sort_values('GMV', ascending=False)

    print(f"   Created {len(geo)} geographic records")
    return geo


def create_daily_geographic(orders_df):
    """Create daily geographic data for time-filtered analysis"""
    print("\n🗺️ Creating Daily Geographic...")

    if orders_df.empty or 'Province' not in orders_df.columns:
        return pd.DataFrame()

    # Ensure date column exists
    if 'Order_Date_Only' not in orders_df.columns:
        orders_df['Order_Date_Only'] = pd.to_datetime(orders_df['Order_Date']).dt.date

    daily_geo = orders_df.groupby(['Order_Date_Only', 'Province', 'Platform']).agg({
        'Order_ID': 'nunique',
        'Net_Sales': 'sum',
        'Quantity': 'sum',
    }).reset_index()

    daily_geo.columns = ['Date', 'Province', 'Platform', 'Orders', 'GMV', 'Units_Sold']
    daily_geo = daily_geo.sort_values(['Date', 'GMV'], ascending=[True, False])

    print(f"   Created {len(daily_geo)} daily geographic records")
    return daily_geo


# ==========================================
# DUCKDB SQL FUNCTIONS
# ==========================================
def create_duckdb_database(daily_df, product_df, ads_df, geo_df, orders_df, live_df=None, video_df=None, daily_geo_df=None,
                           tiktok_live_df=None, tiktok_video_df=None):
    """Create DuckDB database for fast SQL queries"""
    print("\n🗄️ Creating DuckDB Database...")

    db_path = OUTPUT_DIR / "shopee_dashboard.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create tables from dataframes (using CREATE TABLE AS SELECT)
    if not daily_df.empty:
        conn.execute("CREATE OR REPLACE TABLE daily_sales AS SELECT * FROM daily_df")

    if not product_df.empty:
        conn.execute("CREATE OR REPLACE TABLE products AS SELECT * FROM product_df")

    if not ads_df.empty:
        conn.execute("CREATE OR REPLACE TABLE ads_performance AS SELECT * FROM ads_df")

    if not geo_df.empty:
        conn.execute("CREATE OR REPLACE TABLE geographic AS SELECT * FROM geo_df")

    if not orders_df.empty:
        # Select key columns for orders_raw to keep size manageable
        key_cols = ['Order_ID', 'Order_Status', 'Order_Date', 'Product_Name',
                    'SKU', 'Quantity', 'Net_Sales', 'True_Net_Revenue', 'Platform', 'Province']
        available_cols = [c for c in key_cols if c in orders_df.columns]
        orders_subset = orders_df[available_cols].copy()
        conn.execute("CREATE OR REPLACE TABLE orders_raw AS SELECT * FROM orders_subset")

    # Add daily geographic table
    if daily_geo_df is not None and not daily_geo_df.empty:
        conn.execute("CREATE OR REPLACE TABLE daily_geographic AS SELECT * FROM daily_geo_df")
        print("   Added daily_geographic table")

    # Add live and video tables
    if live_df is not None and not live_df.empty:
        conn.execute("CREATE OR REPLACE TABLE combined_live AS SELECT * FROM live_df")
        print("   Added combined_live table")

    if video_df is not None and not video_df.empty:
        conn.execute("CREATE OR REPLACE TABLE combined_video AS SELECT * FROM video_df")
        print("   Added combined_video table")

    # Add TikTok tables
    if tiktok_live_df is not None and not tiktok_live_df.empty:
        conn.execute("CREATE OR REPLACE TABLE tiktok_live AS SELECT * FROM tiktok_live_df")
        print("   Added tiktok_live table")

    if tiktok_video_df is not None and not tiktok_video_df.empty:
        conn.execute("CREATE OR REPLACE TABLE tiktok_video AS SELECT * FROM tiktok_video_df")
        print("   Added tiktok_video table")

    # Create useful views
    conn.execute("""
        CREATE OR REPLACE VIEW kpi_summary AS
        SELECT
            SUM(Orders) as Total_Orders,
            SUM(GMV) as Total_GMV,
            SUM(Net_Revenue) as Total_Net_Revenue,
            AVG(AOV) as Average_AOV,
            SUM(Units_Sold) as Total_Units
        FROM daily_sales
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW top_products AS
        SELECT
            Product_Name,
            Product_Segment,
            Total_GMV,
            Total_Qty,
            "GMV_Contribution_%" as GMV_Contribution,
            Risk_Status
        FROM products
        ORDER BY Total_GMV DESC
        LIMIT 20
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW daily_trend AS
        SELECT
            Date,
            GMV,
            Orders,
            AOV,
            GMV_Segment,
            DoD_GMV_Growth,
            WoW_GMV_Growth,
            MoM_GMV_Growth
        FROM daily_sales
        ORDER BY Date DESC
    """)

    conn.close()
    print(f"   Database created: {db_path}")
    return db_path


# ==========================================
# MAIN EXECUTION
# ==========================================
def run_pipeline():
    """Main pipeline execution"""
    print("=" * 60)
    print("🚀 SHOPEE + TIKTOK DATA PIPELINE")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    orders_raw = load_orders()
    ads_raw = load_ads()
    live_raw = load_live()
    video_raw = load_video()
    tiktok_live_raw = load_tiktok_live()
    tiktok_video_raw = load_tiktok_video()

    # Clean data
    orders_clean = clean_orders(orders_raw)
    ads_clean = clean_ads(ads_raw)
    live_clean = clean_live(live_raw)
    video_clean = clean_video(video_raw)
    tiktok_live_clean = clean_tiktok_live(tiktok_live_raw)
    tiktok_video_clean = clean_tiktok_video(tiktok_video_raw)

    # Create master datasets
    daily_master = create_daily_sales_master(orders_clean)
    product_master = create_product_master(orders_clean)
    ads_master = create_ads_master(ads_clean)
    geo_master = create_geographic_master(orders_clean)
    daily_geo_master = create_daily_geographic(orders_clean)

    # Create DuckDB database
    db_path = create_duckdb_database(
        daily_master, product_master, ads_master, geo_master, orders_clean,
        live_clean, video_clean, daily_geo_master,
        tiktok_live_clean, tiktok_video_clean
    )

    # Export to CSV for Looker Studio
    print("\n💾 Exporting Master Files...")

    if not daily_master.empty:
        daily_master.to_csv(OUTPUT_DIR / "Master_Daily_Sales.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Master_Daily_Sales.csv")

    if not product_master.empty:
        product_master.to_csv(OUTPUT_DIR / "Master_Product_Sales.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Master_Product_Sales.csv")

    if not ads_master.empty:
        ads_master.to_csv(OUTPUT_DIR / "Master_Ads_Performance.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Master_Ads_Performance.csv")

    if not geo_master.empty:
        geo_master.to_csv(OUTPUT_DIR / "Master_Geographic.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Master_Geographic.csv")

    # Export combined raw data
    if not orders_clean.empty:
        orders_clean.to_csv(OUTPUT_DIR / "Combined_Orders.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Combined_Orders.csv")

    if not ads_clean.empty:
        ads_clean.to_csv(OUTPUT_DIR / "Combined_Ads.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Combined_Ads.csv")

    if not live_clean.empty:
        live_clean.to_csv(OUTPUT_DIR / "Combined_Live.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Combined_Live.csv")

    if not video_clean.empty:
        video_clean.to_csv(OUTPUT_DIR / "Combined_Video.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Combined_Video.csv")

    # Export TikTok data
    if not tiktok_live_clean.empty:
        tiktok_live_clean.to_csv(OUTPUT_DIR / "Combined_TikTok_Live.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Combined_TikTok_Live.csv")

    if not tiktok_video_clean.empty:
        tiktok_video_clean.to_csv(OUTPUT_DIR / "Combined_TikTok_Video.csv", index=False, encoding='utf-8-sig')
        print(f"   ✓ Combined_TikTok_Video.csv")

    # Summary report
    print("\n" + "=" * 60)
    print("📊 DATA SUMMARY")
    print("=" * 60)

    if not daily_master.empty:
        print(f"\n📅 Date Range: {daily_master['Date'].min().date()} to {daily_master['Date'].max().date()}")
        print(f"📦 Total Orders: {daily_master['Orders'].sum():,.0f}")
        print(f"💰 Total GMV: ฿{daily_master['GMV'].sum():,.2f}")
        print(f"💵 Net Revenue: ฿{daily_master['Net_Revenue'].sum():,.2f}")
        print(f"📈 Average AOV: ฿{daily_master['AOV'].mean():,.2f}")

    if not product_master.empty:
        print(f"\n🏷️ Products: {len(product_master)} unique SKUs")
        print(f"   Star Products: {len(product_master[product_master['Product_Segment'].str.contains('Star')])}")
        print(f"   Hero Products: {len(product_master[product_master['Product_Segment'].str.contains('Hero')])}")

    if not ads_master.empty:
        print(f"\n📊 Campaigns: {len(ads_master)} active")
        print(f"   Total Ad Spend: ฿{ads_master['Ad_Cost'].sum():,.2f}")
        print(f"   Total Ad Sales: ฿{ads_master['Sales'].sum():,.2f}")
        print(f"   Average ROAS: {ads_master['ROAS'].mean():.2f}x")

    # TikTok summary
    if not tiktok_live_clean.empty:
        print(f"\n🎬 TikTok Live: {len(tiktok_live_clean)} sessions")
        print(f"   Total GMV: ฿{tiktok_live_clean['GMV'].sum():,.2f}")
        print(f"   Total Orders: {tiktok_live_clean['Orders'].sum():,.0f}")
        print(f"   Total Viewers: {tiktok_live_clean['Viewers'].sum():,.0f}")

    if not tiktok_video_clean.empty:
        print(f"\n🎥 TikTok Video: {len(tiktok_video_clean)} videos")
        # Handle potential duplicate columns (returns DataFrame instead of Series)
        gmv_col = tiktok_video_clean['GMV']
        orders_col = tiktok_video_clean['Orders']
        views_col = tiktok_video_clean['Views']
        gmv_sum = gmv_col.iloc[:, 0].sum() if isinstance(gmv_col, pd.DataFrame) else gmv_col.sum()
        orders_sum = orders_col.iloc[:, 0].sum() if isinstance(orders_col, pd.DataFrame) else orders_col.sum()
        views_sum = views_col.iloc[:, 0].sum() if isinstance(views_col, pd.DataFrame) else views_col.sum()
        print(f"   Total GMV: ฿{gmv_sum:,.2f}")
        print(f"   Total Orders: {orders_sum:,.0f}")
        print(f"   Total Views: {views_sum:,.0f}")

    print("\n" + "=" * 60)
    print(f"✅ Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print("=" * 60)

    return {
        'daily': daily_master,
        'products': product_master,
        'ads': ads_master,
        'geo': geo_master,
        'tiktok_live': tiktok_live_clean,
        'tiktok_video': tiktok_video_clean,
        'db_path': db_path
    }


if __name__ == "__main__":
    results = run_pipeline()
