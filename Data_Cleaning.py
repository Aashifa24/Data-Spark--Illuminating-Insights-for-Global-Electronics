# Load csv file
def detect_and_load_csv(file_path):
    try:
        encoding = detect_encoding(file_path)
        print(f"Detected encoding for {file_path}: {encoding}")
        df = pd.read_csv(file_path, encoding=encoding)
        return df
    except FileNotFoundError:
        print(f"File {file_path} not found. Please check the file path.")
    except Exception as e:
        print(f"An error occurred while loading {file_path}: {e}")
    return None

# Convert date format 
def convert_date_format(date_str):
    # Check if the date_str is NaN or not a string
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None  # Return None for NaN or non-string values
    try:
        # Convert the date format
        return datetime.strptime(date_str, "%m/%d/%Y").strftime('%Y-%m-%d')
    except ValueError:
        return None  # Return None if date cannot be parsed