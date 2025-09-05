import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import json
import io
import re
from datetime import datetime
import time

# Page configuration
st.set_page_config(
    page_title="Data Update Manager",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .tab-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        margin: 1rem 0;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #cce7ff;
        border: 1px solid #b8daff;
        color: #004085;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'client' not in st.session_state:
    st.session_state.client = None

def authenticate_google_sheets():
    """Authenticate with Google Sheets API"""
    try:
        # Get credentials from Streamlit secrets
        creds_dict = dict(st.secrets["gcp_service_account"])
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets", 
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        
        return client
    except Exception as e:
        st.error(f"❌ Lỗi xác thực Google Sheets: {str(e)}")
        return None

def get_google_sheet(client, sheet_id, worksheet_name):
    """Get specific worksheet from Google Sheets"""
    try:
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        return worksheet
    except Exception as e:
        st.error(f"❌ Không thể mở worksheet '{worksheet_name}': {str(e)}")
        return None

def update_sheet_data(worksheet, data):
    """Update Google Sheet with new data"""
    try:
        # Clear existing data
        worksheet.clear()
        
        # Update with new data
        set_with_dataframe(worksheet, data, include_index=False)
        
        return True
    except Exception as e:
        st.error(f"❌ Lỗi cập nhật dữ liệu: {str(e)}")
        return False

def extract_date_from_filename(filename):
    """Extract start and end date from SellerBoard filename"""
    try:
        # Pattern: NewEleven_Dashboard Products Group by ASIN_01_07_2025-31_07_2025_(08_44_44_695).xlsx
        pattern = r'(\d{2}_\d{2}_\d{4})-(\d{2}_\d{2}_\d{4})'
        match = re.search(pattern, filename)
        
        if match:
            start_date_str = match.group(1)  # 01_07_2025
            end_date_str = match.group(2)    # 31_07_2025
            
            # Extract month from start date (format: DD_MM_YYYY)
            month = int(start_date_str.split('_')[1])
            year = int(start_date_str.split('_')[2])
            
            # Calculate quarter
            quarter = (month - 1) // 3 + 1
            
            return month, quarter, year
        else:
            return None, None, None
    except Exception as e:
        st.error(f"❌ Lỗi extract ngày từ filename: {str(e)}")
        return None, None, None

def extract_date_from_brand_analytics_filename(filename):
    """Extract month and year from Brand Analytics filename"""
    try:
        # Pattern: US_Search_Catalog_Performance_Simple_Month_2025_07_31
        pattern = r'(\d{4})_(\d{2})_(\d{2})\.csv?$'
        match = re.search(pattern, filename)
        
        if match:
            year = int(match.group(1))    # 2025
            month = int(match.group(2))   # 07
            day = int(match.group(3))     # 31
            
            # Calculate quarter
            quarter = (month - 1) // 3 + 1
            
            return month, quarter, year
        else:
            return None, None, None
    except Exception as e:
        st.error(f"❌ Lỗi extract ngày từ Brand Analytics filename: {str(e)}")
        return None, None, None

def add_month_quarter_columns(df, month, quarter):
    """Add Month and Quarter columns to dataframe"""
    try:
        df_copy = df.copy()
        
        # Add Month and Quarter columns at the beginning
        df_copy.insert(0, 'Quarter', quarter)
        df_copy.insert(1, 'Month', month)
        
        return df_copy
    except Exception as e:
        st.error(f"❌ Lỗi thêm cột Month/Quarter: {str(e)}")
        return None

def get_existing_data_count(worksheet):
    """Get number of existing rows in worksheet (excluding header)"""
    try:
        all_values = worksheet.get_all_values()
        # Subtract 1 for header row, return 0 if only header exists
        return max(0, len(all_values) - 1)
    except Exception as e:
        st.error(f"❌ Lỗi đếm dữ liệu hiện có: {str(e)}")
        return 0

def append_to_sheet(worksheet, new_data):
    """Append new data to existing sheet nhanh hơn"""
    try:
        # Lấy header hiện tại
        existing_headers = worksheet.row_values(1)
        if not existing_headers:
            # Nếu sheet rỗng thì ghi luôn cả header + data
            set_with_dataframe(worksheet, new_data.fillna(""), include_index=False)
            return True

        # Đảm bảo thứ tự cột theo sheet
        reordered_data = pd.DataFrame()
        for header in existing_headers:
            if header in new_data.columns:
                reordered_data[header] = new_data[header]
            else:
                reordered_data[header] = ""

        # Thêm các cột mới (nếu có)
        for col in new_data.columns:
            if col not in existing_headers:
                reordered_data[col] = new_data[col]

        # Xử lý NaN
        reordered_data = reordered_data.fillna("")

        # Lấy số dòng hiện có (trừ header)
        existing_rows = len(worksheet.get_all_values()) - 1

        # Ghi dữ liệu mới bắt đầu từ dòng tiếp theo
        set_with_dataframe(
            worksheet,
            reordered_data,
            include_index=False,
            include_column_header=False,  # ✅ Bỏ header
            row=existing_rows + 1         # chỉ +1 vì không còn header
        )

        return True
    except Exception as e:
        st.error(f"❌ Lỗi append dữ liệu: {str(e)}")
        return False

def validate_file_format(uploaded_file, expected_format):
    """Validate uploaded file format"""
    if expected_format == "txt":
        return uploaded_file.name.endswith('.txt')
    elif expected_format == "csv":
        return uploaded_file.name.endswith('.csv')
    elif expected_format == "xlsx":
        return uploaded_file.name.endswith('.xlsx') or uploaded_file.name.endswith('.xls')
    return False

def process_inventory_file(uploaded_file):
    """Process inventory .txt file"""
    try:
        # Read text file
        content = uploaded_file.read().decode('utf-8')
        lines = content.strip().split('\n')
        
        # Convert to DataFrame (assuming tab-separated or comma-separated)
        data = []
        for line in lines:
            # Try different separators
            if '\t' in line:
                data.append(line.split('\t'))
            elif ',' in line:
                data.append(line.split(','))
            else:
                data.append([line])
        
        df = pd.DataFrame(data)
        
        # If first row looks like headers, use it
        if len(df) > 1:
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
        
        return df
    except Exception as e:
        st.error(f"❌ Lỗi xử lý file Inventory: {str(e)}")
        return None

def process_excel_file(uploaded_file):
    """Process Excel file (.xlsx)"""
    try:
        df = pd.read_excel(uploaded_file)
        return df
    except Exception as e:
        st.error(f"❌ Lỗi đọc file Excel: {str(e)}")
        return None
    
def process_csv_file(uploaded_file):
    """Process Excel file (.xlsx)"""
    try:
        df = pd.read_csv(uploaded_file, skiprows=1)
        return df
    except Exception as e:
        st.error(f"❌ Lỗi đọc file Excel: {str(e)}")
        return None

def get_existing_columns(worksheet):
    """Get existing column headers from worksheet"""
    try:
        headers = worksheet.row_values(1)
        return [header for header in headers if header.strip()]  # Remove empty headers
    except Exception as e:
        st.error(f"❌ Lỗi lấy headers từ sheet: {str(e)}")
        return []

def filter_and_reorder_data(df, existing_columns):
    """Filter dataframe to only include columns that exist in the target sheet and reorder them"""
    try:
        # Find matching columns (case-insensitive comparison)
        df_columns_lower = {col.lower(): col for col in df.columns}
        existing_columns_lower = {col.lower(): col for col in existing_columns}
        
        matching_columns = []
        for existing_col_lower, existing_col in existing_columns_lower.items():
            if existing_col_lower in df_columns_lower:
                matching_columns.append((existing_col, df_columns_lower[existing_col_lower]))
        
        if not matching_columns:
            st.error("❌ Không tìm thấy cột nào trùng khớp với sheet gốc!")
            return None, [], []
        
        # Create filtered dataframe with correct column order
        filtered_df = pd.DataFrame()
        matched_sheet_columns = []
        matched_file_columns = []
        
        for sheet_col, file_col in matching_columns:
            filtered_df[sheet_col] = df[file_col]
            matched_sheet_columns.append(sheet_col)
            matched_file_columns.append(file_col)
        
        return filtered_df, matched_sheet_columns, matched_file_columns
        
    except Exception as e:
        st.error(f"❌ Lỗi filter và reorder data: {str(e)}")
        return None, [], []


# Main app
def main():
    st.markdown('<h1 class="main-header">📊 Data Update Manager</h1>', unsafe_allow_html=True)
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Cấu hình")
        
        # Google Sheet ID input
        sheet_id = st.text_input(
            "Google Sheet ID", 
            value="11HBMec3fPMt-8_dxaC0VcyKT0yPQVfsURnZgAKkxwKg",
            help="ID của Google Sheet cần update"
        )
        
        # Authentication button
        if st.button("🔐 Kết nối Google Sheets"):
            with st.spinner("Đang kết nối..."):
                client = authenticate_google_sheets()
                if client:
                    st.session_state.client = client
                    st.session_state.authenticated = True
                    st.success("✅ Kết nối thành công!")
                else:
                    st.session_state.authenticated = False
        
        # Connection status
        if st.session_state.authenticated:
            st.success("🟢 Đã kết nối Google Sheets")
        else:
            st.warning("🟡 Chưa kết nối Google Sheets")
    
    # Main content tabs - Added tab5 for Brand Analytics
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📦 Update Inventory", "🏷️ Update T. ASIN", "🚀 Update T. Launching", "📈 Data SellerBoard", "🔍 Data Brand Analytics"])
    
    # Tab 1: Update Inventory
    with tab1:
        st.markdown('<h2 class="tab-header">📦 Update Inventory</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown('<div class="info-box">📁 Upload file Inventory (.txt)</div>', unsafe_allow_html=True)
            
            inventory_file = st.file_uploader(
                "Chọn file Inventory",
                type=['txt'],
                key="inventory_uploader"
            )
            
            if inventory_file is not None:
                if validate_file_format(inventory_file, "txt"):
                    # Process file
                    df = process_inventory_file(inventory_file)
                    
                    if df is not None:
                        st.success(f"✅ Đã đọc file thành công! ({len(df)} dòng dữ liệu)")
                        
                        # Preview data
                        st.subheader("👀 Preview dữ liệu:")
                        st.dataframe(df.head(10), use_container_width=True)
                        
                        # Update button
                        if st.session_state.authenticated:
                            if st.button("🔄 Update Inventory", key="update_inventory"):
                                with st.spinner("Đang cập nhật..."):
                                    worksheet = get_google_sheet(st.session_state.client, sheet_id, "Inventory")
                                    if worksheet:
                                        if update_sheet_data(worksheet, df):
                                            st.markdown('<div class="success-box">✅ Cập nhật Inventory thành công!</div>', unsafe_allow_html=True)
                                            st.balloons()
                                        else:
                                            st.markdown('<div class="error-box">❌ Cập nhật thất bại!</div>', unsafe_allow_html=True)
                        else:
                            st.warning("⚠️ Vui lòng kết nối Google Sheets trước!")
                else:
                    st.error("❌ File không đúng định dạng .txt")
        
        with col2:
            st.info("""
            **📋 Hướng dẫn:**
            1. Chọn file .txt chứa dữ liệu Inventory
            2. Kiểm tra preview dữ liệu
            3. Nhấn "Update Inventory" để cập nhật
            
            **📝 Lưu ý:**
            - File phải có định dạng .txt
            - Dữ liệu phân cách bằng tab hoặc comma
            """)
    
    # Tab 2: Update T. ASIN
    with tab2:
        st.markdown('<h2 class="tab-header">🏷️ Update T. ASIN</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown('<div class="info-box">📊 Upload file T. ASIN (.xlsx)</div>', unsafe_allow_html=True)
            
            asin_file = st.file_uploader(
                "Chọn file T. ASIN",
                type=['xlsx', 'xls'],
                key="asin_uploader"
            )
            
            if asin_file is not None:
                if validate_file_format(asin_file, "xlsx"):
                    # Process file
                    df = process_excel_file(asin_file)
                    
                    if df is not None:
                        st.success(f"✅ Đã đọc file thành công! ({len(df)} dòng dữ liệu)")
                        
                        # Preview data
                        st.subheader("👀 Preview dữ liệu:")
                        st.dataframe(df.head(10), use_container_width=True)
                        
                        # Update button
                        if st.session_state.authenticated:
                            if st.button("🔄 Update T. ASIN", key="update_asin"):
                                with st.spinner("Đang cập nhật..."):
                                    worksheet = get_google_sheet(st.session_state.client, sheet_id, "T. ASIN")
                                    if worksheet:
                                        if update_sheet_data(worksheet, df):
                                            st.markdown('<div class="success-box">✅ Cập nhật T. ASIN thành công!</div>', unsafe_allow_html=True)
                                            st.balloons()
                                        else:
                                            st.markdown('<div class="error-box">❌ Cập nhật thất bại!</div>', unsafe_allow_html=True)
                        else:
                            st.warning("⚠️ Vui lòng kết nối Google Sheets trước!")
                else:
                    st.error("❌ File không đúng định dạng .xlsx/.xls")
        
        with col2:
            st.info("""
            **📋 Hướng dẫn:**
            1. Chọn file .xlsx chứa dữ liệu T. ASIN
            2. Kiểm tra preview dữ liệu
            3. Nhấn "Update T. ASIN" để cập nhật
            
            **📝 Lưu ý:**
            - File phải có định dạng .xlsx hoặc .xls
            - Dữ liệu sẽ ghi đè worksheet "T. ASIN"
            """)
    
    # Tab 3: Update T. Launching
    with tab3:
        st.markdown('<h2 class="tab-header">🚀 Update T. Launching</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown('<div class="info-box">🎯 Upload file T. Launching (.xlsx)</div>', unsafe_allow_html=True)
            
            launching_file = st.file_uploader(
                "Chọn file T. Launching",
                type=['xlsx', 'xls'],
                key="launching_uploader"
            )
            
            if launching_file is not None:
                if validate_file_format(launching_file, "xlsx"):
                    # Process file
                    df = process_excel_file(launching_file)
                    
                    if df is not None:
                        st.success(f"✅ Đã đọc file thành công! ({len(df)} dòng dữ liệu)")
                        
                        # Preview data
                        st.subheader("👀 Preview dữ liệu:")
                        st.dataframe(df.head(10), use_container_width=True)
                        
                        # Update button
                        if st.session_state.authenticated:
                            if st.button("🔄 Update T. Launching", key="update_launching"):
                                with st.spinner("Đang cập nhật..."):
                                    worksheet = get_google_sheet(st.session_state.client, sheet_id, "T. Launching")
                                    if worksheet:
                                        if update_sheet_data(worksheet, df):
                                            st.markdown('<div class="success-box">✅ Cập nhật T. Launching thành công!</div>', unsafe_allow_html=True)
                                            st.balloons()
                                        else:
                                            st.markdown('<div class="error-box">❌ Cập nhật thất bại!</div>', unsafe_allow_html=True)
                        else:
                            st.warning("⚠️ Vui lòng kết nối Google Sheets trước!")
                else:
                    st.error("❌ File không đúng định dạng .xlsx/.xls")
        
        with col2:
            st.info("""
            **📋 Hướng dẫn:**
            1. Chọn file .xlsx chứa dữ liệu T. Launching
            2. Kiểm tra preview dữ liệu
            3. Nhấn "Update T. Launching" để cập nhật
            
            **📝 Lưu ý:**
            - File phải có định dạng .xlsx hoặc .xls
            - Dữ liệu sẽ ghi đè worksheet "T. Launching"
            """)
    
    # Tab 4: Data SellerBoard
    with tab4:
        st.markdown('<h2 class="tab-header">📈 Data SellerBoard</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown('<div class="info-box">📊 Upload file SellerBoard (.xlsx)</div>', unsafe_allow_html=True)
            
            sellerboard_file = st.file_uploader(
                "Chọn file SellerBoard",
                type=['xlsx', 'xls'],
                key="sellerboard_uploader",
                help="File format: NewEleven_Dashboard Products Group by ASIN_DD_MM_YYYY-DD_MM_YYYY_(timestamp).xlsx"
            )
            
            if sellerboard_file is not None:
                if validate_file_format(sellerboard_file, "xlsx"):
                    # Extract date info from filename
                    month, quarter, year = extract_date_from_filename(sellerboard_file.name)
                    
                    if month and quarter and year:
                        st.success(f"✅ Detected: Tháng {month}/{year} - Quarter {quarter}")
                        
                        # Process file
                        df = process_excel_file(sellerboard_file)
                        
                        if df is not None:
                            st.success(f"✅ Đã đọc file thành công! ({len(df)} dòng dữ liệu)")
                            
                            # Add Month and Quarter columns
                            df_with_metadata = add_month_quarter_columns(df, month, quarter)
                            
                            if df_with_metadata is not None:
                                # Preview data with new columns
                                st.subheader("👀 Preview dữ liệu (với cột Month & Quarter):")
                                st.dataframe(df_with_metadata.head(10), use_container_width=True)
                                
                                # Show column info
                                st.info(f"📋 Tổng cộng: {len(df_with_metadata.columns)} cột, {len(df_with_metadata)} dòng")
                                
                                # Update button
                                if st.session_state.authenticated:
                                    # Check existing data count
                                    worksheet = get_google_sheet(st.session_state.client, sheet_id, "SB_US_2025")
                                    if worksheet:
                                        existing_count = get_existing_data_count(worksheet)
                                        st.info(f"📊 Dữ liệu hiện tại trong sheet SB_US_2025: {existing_count} dòng")
                                    
                                    if st.button("📈 Append to SB_US_2025", key="append_sellerboard"):
                                        with st.spinner("Đang append dữ liệu..."):
                                            if worksheet:
                                                if append_to_sheet(worksheet, df_with_metadata):
                                                    new_count = get_existing_data_count(worksheet)
                                                    added_rows = new_count - existing_count
                                                    st.markdown(f'<div class="success-box">✅ Append SellerBoard thành công!<br>📊 Đã thêm {added_rows} dòng dữ liệu<br>📈 Tổng dữ liệu hiện tại: {new_count} dòng</div>', unsafe_allow_html=True)
                                                    st.balloons()
                                                else:
                                                    st.markdown('<div class="error-box">❌ Append thất bại!</div>', unsafe_allow_html=True)
                                else:
                                    st.warning("⚠️ Vui lòng kết nối Google Sheets trước!")
                    else:
                        st.error("❌ Không thể detect tháng/năm từ tên file. Vui lòng kiểm tra format tên file!")
                        st.info("📝 Format đúng: NewEleven_Dashboard Products Group by ASIN_DD_MM_YYYY-DD_MM_YYYY_(timestamp).xlsx")
                else:
                    st.error("❌ File không đúng định dạng .xlsx/.xls")
        
        with col2:
            st.info("""
            **📋 Hướng dẫn:**
            1. Chọn file .xlsx SellerBoard
            2. Hệ thống tự động detect tháng/quarter
            3. Thêm cột Month & Quarter
            4. Append vào sheet SB_US_2025
            
            **📝 Format tên file:**
            ```
            NewEleven_Dashboard Products Group by ASIN_
            01_07_2025-31_07_2025_
            (08_44_44_695).xlsx
            ```
            
            **🔢 Quarter mapping:**
            - Q1: Tháng 1,2,3
            - Q2: Tháng 4,5,6  
            - Q3: Tháng 7,8,9
            - Q4: Tháng 10,11,12
            
            **⚠️ Lưu ý:**
            - Dữ liệu sẽ được append vào cuối sheet
            - Thứ tự cột sẽ được maintain theo sheet gốc
            - Cột Month và Quarter sẽ được thêm vào cuối
            """)

    # Tab 5: Data Brand Analytics

    with tab5:
        st.markdown('<h2 class="tab-header">🔍 Data Brand Analytics</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown('<div class="info-box">📊 Upload file Brand Analytics (.csv)</div>', unsafe_allow_html=True)
            
            brand_analytics_file = st.file_uploader(
                "Chọn file Brand Analytics",
                type=['csv'],
                key="brand_analytics_uploader",
                help="File format: US_Search_Catalog_Performance_Simple_Month_YYYY_MM_DD.csv"
            )
            
            if brand_analytics_file is not None:
                if validate_file_format(brand_analytics_file, "csv"):
                    # 1. Extract date info from filename
                    month, quarter, year = extract_date_from_brand_analytics_filename(brand_analytics_file.name)
                    
                    if month and quarter and year:
                        st.success(f"✅ Detected: Tháng {month}/{year} - Quarter {quarter}")
                        
                        # 2. Process CSV file
                        df = process_csv_file(brand_analytics_file)
                        
                        if df is not None:
                            st.success(f"✅ Đã đọc file thành công! ({len(df)} dòng dữ liệu)")
                            st.info(f"📊 Columns trong file: {len(df.columns)} cột")
                            
                            # Check authentication before getting sheet data
                            if st.session_state.authenticated:
                                # 3. Get existing columns from BA_US_2025 sheet
                                worksheet = get_google_sheet(st.session_state.client, sheet_id, "BA_US_2025")
                                if worksheet:
                                    existing_columns = get_existing_columns(worksheet)
                                    existing_count = get_existing_data_count(worksheet)
                                    
                                    if existing_columns:
                                        st.info(f"📋 Columns trong sheet BA_US_2025: {len(existing_columns)} cột")
                                        st.info(f"📊 Dữ liệu hiện tại trong sheet: {existing_count} dòng")
                                        
                                        # 4. Column Matching - Filter and reorder data
                                        filtered_df, matched_sheet_cols, matched_file_cols = filter_and_reorder_data(df, existing_columns)
                                        
                                        if filtered_df is not None:
                                            st.success(f"✅ Matched {len(matched_sheet_cols)} cột với sheet gốc")
                                            
                                            # Show matched columns in expandable table
                                            with st.expander("📋 Chi tiết cột matched"):
                                                match_info = pd.DataFrame({
                                                    'Sheet Column': matched_sheet_cols,
                                                    'File Column': matched_file_cols
                                                })
                                                st.dataframe(match_info, use_container_width=True)
                                            
                                            # 5. Add Month and Quarter columns
                                            df_with_metadata = add_month_quarter_columns(filtered_df, month, quarter)
                                            
                                            if df_with_metadata is not None:
                                                # Data Preview with filtered data
                                                st.subheader("👀 Preview dữ liệu đã filter (với cột Month & Quarter):")
                                                st.dataframe(df_with_metadata.head(10), use_container_width=True)
                                                
                                                # Show final column info
                                                st.info(f"📋 Dữ liệu cuối cùng: {len(df_with_metadata.columns)} cột, {len(df_with_metadata)} dòng")
                                                
                                                # 6. Append button - only compatible columns
                                                if st.button("🔍 Append to BA_US_2025", key="append_brand_analytics"):
                                                    with st.spinner("Đang append dữ liệu..."):
                                                        if append_to_sheet(worksheet, df_with_metadata):
                                                            new_count = get_existing_data_count(worksheet)
                                                            added_rows = new_count - existing_count
                                                            st.markdown(f'<div class="success-box">✅ Append Brand Analytics thành công!<br>📊 Đã thêm {added_rows} dòng dữ liệu<br>📈 Tổng dữ liệu hiện tại: {new_count} dòng</div>', unsafe_allow_html=True)
                                                            st.balloons()
                                                        else:
                                                            st.markdown('<div class="error-box">❌ Append thất bại!</div>', unsafe_allow_html=True)
                                        else:
                                            st.error("❌ Không có cột nào trùng khớp với sheet BA_US_2025!")
                                    else:
                                        st.error("❌ Không thể lấy thông tin columns từ sheet BA_US_2025!")
                                else:
                                    st.error("❌ Không thể kết nối đến sheet BA_US_2025!")
                            else:
                                st.warning("⚠️ Vui lòng kết nối Google Sheets trước để kiểm tra columns!")
                    else:
                        st.error("❌ Không thể detect tháng/năm từ tên file. Vui lòng kiểm tra format tên file!")
                        st.info("📝 Format đúng: US_Search_Catalog_Performance_Simple_Month_2025_07_31.csv")
                else:
                    st.error("❌ File không đúng định dạng .csv")
        with col2:
            st.info("""
            **📋 Hướng dẫn:**
            1. Chọn file .csv Brand Analytics
            2. Hệ thống tự động detect tháng/quarter
            3. Thêm cột Month & Quarter
            4. Append vào sheet BA_US_2025
            
            **📝 Format tên file:**
            ```
            US_Search_Catalog_Performance_
            Simple_Month_2025_07_31.csv
            ```
                    
            **🔢 Quarter mapping:**
            - Q1: Tháng 1,2,3
            - Q2: Tháng 4,5,6  
            - Q3: Tháng 7,8,9
            - Q4: Tháng 10,11,12
            
            **⚠️ Lưu ý:**
            - Dữ liệu sẽ được append vào cuối sheet
            - Thứ tự cột sẽ được maintain theo sheet gốc
            - Cột Month và Quarter sẽ được thêm vào cuối
            """)    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #7f8c8d; margin-top: 2rem;">
        📊 Data Update Manager | Powered by Streamlit & Google Sheets API
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()