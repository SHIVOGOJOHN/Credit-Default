import streamlit as st

# Global Styling
st.markdown("""
<style>
body {
    background-color: #f5f7fa;
}

.login-container {
    text-align: center;
    padding: 30px 0;
}

.bank-logo {
    font-size: 60px;
}

.bank-name {
    font-size: 28px;
    font-weight: 700;
    letter-spacing: 2px;
}

.portal-subtitle {
    font-size: 14px;
    color: gray;
}

.divider-line {
    height: 2px;
    background: #1f4e79;
    margin: 15px auto;
    width: 60%;
}

.field-label {
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 3px;
}

.login-footer {
    text-align: center;
    font-size: 11px;
    margin-top: 30px;
    color: gray;
}

.sidebar-header {
    text-align: center;
    padding: 10px 0;
}

.sidebar-logo {
    font-size: 30px;
}

.sidebar-bank {
    font-weight: bold;
    margin-top: 5px;
}

.sidebar-user {
    font-size: 12px;
    color: gray;
    margin-top: 5px;
}
</style>
""", unsafe_allow_html=True)


import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging
import os
import pandas as pd
import traceback
import re

load_dotenv()

# Configure logging for secure error tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app_username = os.getenv("APP_USER")
app_password = os.getenv("APP_PASSWORD")

db_config = {
        'host': os.getenv("DB_HOST"),
        'user': os.getenv("DB_USER"),
        'password': os.getenv("DB_PASSWORD"),
        'database': os.getenv("DB_NAME"),
        'port' : os.getenv("DB_PORT"),
        'charset' : "utf8mb4",
        'connection_timeout' : 10,
    }

def get_connection():

    return mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                port = db_config['port'],
                database=db_config['database'],
                charset =db_config['charset'],
                connection_timeout=db_config['connection_timeout']
            )

# Infer MySQL types from pandas
def infer_mysql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "TEXT"
    
# create table from dataframe
def create_table_from_df(conn, df, table_name):
    try:
        cursor = conn.cursor()
        columns = []
        for col in df.columns:
            mysql_type = infer_mysql_type(df[col].dtype)
            if col == "applicant_id":
                columns.append(f"`{col}` {mysql_type} PRIMARY KEY")  # ← set as PK
            else:
                columns.append(f"`{col}` {mysql_type}")

        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)}
        )
        """
        cursor.execute(create_query)
        conn.commit()
        cursor.close()
        logger.info(f"Table '{table_name}' created or already exists")
    except mysql.connector.Error as e:
        logger.error(f"Database error creating table '{table_name}': {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating table '{table_name}': {str(e)}", exc_info=True)
        raise

# Insert the data in bulk
def insert_dataframe(conn, df, table_name):
    try:
        cursor = conn.cursor()

        # Replace NaN/NaT with None so MySQL receives proper NULL
        df = df.where(pd.notnull(df), None)

        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f"`{col}`" for col in df.columns])

        insert_query = f"""
        INSERT IGNORE INTO `{table_name}` ({columns})
        VALUES ({placeholders})
        """

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        cursor.executemany(insert_query, data)
        conn.commit()
        cursor.close()
        logger.info(f"Inserted {len(data)} rows into table '{table_name}'")
    except mysql.connector.Error as e:
        logger.error(f"Database error inserting data into '{table_name}': {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting data into '{table_name}': {str(e)}", exc_info=True)
        raise

# Registry Functions

def ensure_registry(conn):
    """Create the _uploaded_tables tracking table if it doesn't exist"""
    try:
        cursor = conn.cursor()
        create_registry_query = """
        CREATE TABLE IF NOT EXISTS `_uploaded_tables` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `table_name` VARCHAR(255) NOT NULL,
            `uploaded_by` VARCHAR(255) NOT NULL,
            `uploaded_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `row_count` INT NOT NULL
        )
        """
        cursor.execute(create_registry_query)
        conn.commit()
        cursor.close()
        logger.info("Registry table ensured")
    except mysql.connector.Error as e:
        logger.error(f"Database error creating registry table: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating registry table: {str(e)}", exc_info=True)
        raise

def register_table(conn, table_name, uploader, row_count):
    """Record every upload with table name, uploader, timestamp and row count"""
    try:
        cursor = conn.cursor()
        register_query = """
        INSERT INTO `_uploaded_tables` (`table_name`, `uploaded_by`, `row_count`)
        VALUES (%s, %s, %s)
        """
        cursor.execute(register_query, (table_name, uploader, row_count))
        conn.commit()
        cursor.close()
        logger.info(f"Registered table '{table_name}' uploaded by '{uploader}' with {row_count} rows")
    except mysql.connector.Error as e:
        logger.error(f"Database error registering table: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error registering table: {str(e)}", exc_info=True)
        raise

def get_registered_tables(conn):
    """Fetch only app-registered tables, ignoring everything else in the DB"""
    try:
        cursor = conn.cursor()
        query = """
        SELECT DISTINCT `table_name` FROM `_uploaded_tables` ORDER BY `uploaded_at` DESC
        """
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    except mysql.connector.Error as e:
        logger.error(f"Database error fetching registered tables: {str(e)}", exc_info=True)
        return []

def get_registry_data(conn):
    """Fetch the full registry table data for display"""
    try:
        cursor = conn.cursor()
        query = """
        SELECT `table_name`, `uploaded_by`, `uploaded_at`, `row_count` 
        FROM `_uploaded_tables` 
        ORDER BY `uploaded_at` DESC
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        return columns, data
    except mysql.connector.Error as e:
        logger.error(f"Database error fetching registry data: {str(e)}", exc_info=True)
        return [], []

def get_table_columns(conn, table_name):
    """Get all existing columns in a table"""
    try:
        cursor = conn.cursor()
        query = f"DESCRIBE `{table_name}`"
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns
    except mysql.connector.Error as e:
        logger.error(f"Database error getting columns for '{table_name}': {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting columns for '{table_name}': {str(e)}", exc_info=True)
        return None

def validate_and_append_data(conn, df, table_name):
    """
    Validate data before insertion. If table exists, only keep columns that already exist.
    If table doesn't exist, create it with all columns.
    Returns the validated dataframe or None if validation fails.
    """
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        check_query = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
        cursor.execute(check_query, (table_name,))
        table_exists = cursor.fetchone()
        cursor.close()
        
        if table_exists:
            # Table exists - only keep columns that already exist
            existing_columns = get_table_columns(conn, table_name)
            if existing_columns:
                # Filter DF to only include existing columns
                columns_to_keep = [col for col in df.columns if col in existing_columns]
                if not columns_to_keep:
                    return None, "No matching columns found in existing table."
                df = df[columns_to_keep]
        
        return df, None
    except Exception as e:
        logger.error(f"Error validating data for table {table_name}: {str(e)}", exc_info=True)
        cursor.close()
        return None, "Unable to validate table structure. Please try again."

def sanitize_error_message(error_str):
    """
    Remove sensitive information from error messages.
    Masks database credentials, hosts, and internal details.
    """
    if not error_str:
        return "An unexpected error occurred. Please try again."
    
    sanitized = str(error_str)
    
    # Hide IP addresses and hostnames
    sanitized = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[HOST]', sanitized)
    sanitized = re.sub(r'localhost|127\.0\.0\.1', '[HOST]', sanitized, flags=re.IGNORECASE)
    
    # Hide database names and user info
    sanitized = re.sub(r"(database|user|password|host|port)\s*[=:][^,;\)\n]*", r'\1=[REDACTED]', sanitized, flags=re.IGNORECASE)
    
    # Hide SQL queries (truncate to generic message)
    if 'SQL' in sanitized.upper() or 'SELECT' in sanitized.upper() or 'INSERT' in sanitized.upper():
        sanitized = "Database operation failed. Please check your data and try again."
    
    # Hide file paths
    sanitized = re.sub(r'[C-Z]:\\[^\s]*', '[PATH]', sanitized)
    sanitized = re.sub(r'/[^\s]*\.[^\s]+', '[PATH]', sanitized)
    
    return sanitized if sanitized.strip() else "An unexpected error occurred. Please try again."

st.set_page_config(
    page_title="Deta Bank | Data Portal", 
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Authentication
def authenticate(username, password):
    """Authenticate user credentials without exposing them in any logs"""
    if username == app_username and password == app_password:
        logger.info(f"Successful authentication for user: '{username}'")
        return True
    else:
        logger.warning(f"Failed authentication attempt for user: '{username}'")
        return False

def logout():
    """Securely logout user"""
    logger.info(f"User '{st.session_state.username}' logged out")
    st.session_state.authenticated = False
    st.session_state.username = None
    
def show_login():
    st.title("Deta Bank - Data Upload Portal")
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("""
        <div class="login-container">
            <div class="bank-logo">🏦</div>
            <div class="bank-name">DETA BANK</div>
            <div class="portal-subtitle">Data Portal</div>
            <div class="divider-line"></div>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form", clear_on_submit=False):

            st.markdown('<p class="field-label">USERNAME</p>', unsafe_allow_html=True)
            username = st.text_input(
                "", placeholder="Enter your username",
                key="username_input", label_visibility="collapsed"
            )

            st.markdown('<p class="field-label">PASSWORD</p>', unsafe_allow_html=True)
            password = st.text_input(
                "", placeholder="Enter your password",
                type="password", key="password_input",
                label_visibility="collapsed"
            )

            st.markdown("<br>", unsafe_allow_html=True)
            submitted = st.form_submit_button("SIGN IN →", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.error("Please enter both username and password.")
                elif authenticate(username, password):
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.rerun()
                else:
                    st.error("⛔ Invalid credentials. Access denied.")

        st.markdown("""
        <div class="login-footer">
            Authorized personnel only · Deta Bank Internal Systems
        </div>
        """, unsafe_allow_html=True)

def show_app():

    with st.sidebar:
        st.markdown(f"""
        <div class="sidebar-header">
            <div class="sidebar-logo">🏦</div>
            <div class="sidebar-bank">DETA BANK</div>
            <div class="sidebar-user">👤 {st.session_state.username}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("**Navigation**")

        page = st.radio(
            "",
            ["📤 Upload Customer Data", "📊 View Uploaded Data"],
            label_visibility="collapsed"
        )

        st.markdown("---")

        if st.button("🚪 Sign Out", use_container_width=True):
            logout()
            st.rerun()

    # Upload Page
    if page == "📤 Upload Customer Data":
        st.title("Upload Customer Default Dataset")

        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            df.columns = df.columns.str.lower().str.strip()

            st.subheader("Preview")
            st.dataframe(df.head())

            # Split into two columns
            col1, col2 = st.columns(2)
            
            with col1:
                table_name = st.text_input("Target Table Name")
            
            with col2:
                uploader_name = st.text_input("Your Full Name")

            if st.button("Upload to MySQL"):
                try:
                    # Validate required fields
                    if not uploader_name or uploader_name.strip() == "":
                        st.error("❌ Your Full Name is required. Please provide your name to upload.")
                    elif not table_name or table_name.strip() == "":
                        st.error("❌ Target Table Name is required.")
                    elif "applicant_id" not in df.columns:
                        st.error("❌ CSV must contain an 'applicant_id' column to use as primary key.")
                    else:
                        # Drop rows with missing applicant_id — PK cannot be null
                        before = len(df)
                        df = df.dropna(subset=["applicant_id"])
                        df["applicant_id"] = df["applicant_id"].astype(int)
                        dropped = before - len(df)

                        if dropped > 0:
                            st.warning(f"⚠️ Dropped {dropped} rows with missing applicant_id.")

                        # Replace remaining NaNs with None for MySQL NULL
                        df = df.where(pd.notnull(df), None)

                        conn = get_connection()
                        
                        # Ensure registry exists
                        ensure_registry(conn)
                        
                        # Validate and filter columns if table exists
                        df_filtered, error_msg = validate_and_append_data(conn, df, table_name)
                        
                        if error_msg:
                            st.error(f"❌ {error_msg}")
                        else:
                            # Create table if it doesn't exist, otherwise it's already validated
                            create_table_from_df(conn, df_filtered, table_name)
                            insert_dataframe(conn, df_filtered, table_name)
                            
                            # Register the upload
                            register_table(conn, table_name, uploader_name, len(df_filtered))
                            
                            conn.close()

                            st.success(f"✅ Uploaded {len(df_filtered):,} rows to `{table_name}` by {uploader_name}")
                            logger.info(f"Upload successful: table='{table_name}', uploader='{uploader_name}', rows={len(df_filtered)}")

                except mysql.connector.Error as e:
                    logger.error(f"Database error during upload: {str(e)}", exc_info=True)
                    st.error("❌ Unable to connect to database. Please check your network and try again.")
                except Exception as e:
                    logger.error(f"Unexpected error during upload: {str(e)}", exc_info=True)
                    safe_msg = sanitize_error_message(str(e))
                    st.error(f"❌ {safe_msg}")

    # View Uploaded Tables
    elif page == "📊 View Uploaded Data":
        st.title("View Uploaded Tables")

        try:
            conn = get_connection()
            
            # Ensure registry exists
            ensure_registry(conn)
            
            # Display the full Upload Registry table at the top
            st.subheader("📋 Upload Registry")
            registry_columns, registry_data = get_registry_data(conn)
            
            if registry_data:
                registry_df = pd.DataFrame(registry_data, columns=registry_columns)
                st.dataframe(registry_df, use_container_width=True, hide_index=True)
            else:
                st.info("No uploads recorded yet.")
            
            st.markdown("---")
            
            # Get registered tables only
            tables = get_registered_tables(conn)
            
            if tables:
                st.subheader("📊 Preview Table Data")
                selected_table = st.selectbox("Select Table to Preview", tables)
                
                if st.button("Load Table"):
                    # Get metadata for selected table
                    cursor = conn.cursor()
                    metadata_query = """
                    SELECT `uploaded_by`, `uploaded_at`, `row_count` 
                    FROM `_uploaded_tables` 
                    WHERE `table_name` = %s 
                    ORDER BY `uploaded_at` DESC 
                    LIMIT 1
                    """
                    cursor.execute(metadata_query, (selected_table,))
                    metadata = cursor.fetchone()
                    cursor.close()
                    
                    if metadata:
                        uploader, upload_date, row_count = metadata
                        caption = f"Uploaded by: **{uploader}** | Date: **{upload_date}** | Total Rows: **{row_count}**"
                        st.markdown(caption)
                    
                    # Load and display table data
                    df = pd.read_sql(f"SELECT * FROM `{selected_table}` LIMIT 1000", conn)
                    st.dataframe(df, use_container_width=True)
                    
                    if len(df) >= 1000:
                        st.info(f"Showing first 1000 rows out of {row_count} total rows.")
                    
                    logger.info(f"User '{st.session_state.username}' viewed table '{selected_table}'")
            else:
                st.info("No tables have been uploaded yet.")
            
            conn.close()

        except mysql.connector.Error as e:
            logger.error(f"Database error retrieving tables: {str(e)}", exc_info=True)
            st.error("❌ Unable to retrieve table data. Please check your network and try again.")
        except Exception as e:
            logger.error(f"Unexpected error retrieving tables: {str(e)}", exc_info=True)
            safe_msg = sanitize_error_message(str(e))
            st.error(f"❌ {safe_msg}")
# Run the app
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = None

if not st.session_state.authenticated:
    show_login()
else:
    show_app()           