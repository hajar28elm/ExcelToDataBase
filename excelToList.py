# import pandas as pd
# file_path = 'C:/Users/HP PRO/OneDrive/Bureau/excelToDataBase/test.xlsx'
# data_frame = pd.read_excel(file_path)

# data_list = data_frame.values.tolist()

# print(data_list)
# import pandas as pd
# from sqlalchemy import create_engine

# excel_file = 'test.xlsx'  # Excel file name is 'test.xlsx'
# database_uri = 'mysql+mysqlconnector://root:@localhost:3306/excel'

# df = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')

# # Create a MySQL connection using SQLAlchemy
# engine = create_engine(database_uri)

# # Iterate through each sheet in the Excel file
# for sheet_name, sheet_data in df.items():
#     # Write the data to the database with the table name as the Excel sheet name
#     sheet_data.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
#     print(f"Table '{sheet_name}' created with data from Excel sheet '{sheet_name}'.")

# engine.dispose()
# print("Data inserted successfully.")
import pandas as pd
from sqlalchemy import create_engine, inspect

excel_file = 'test.xlsx'
database_uri = 'mysql+mysqlconnector://root:@localhost:3306/excel'

# Load data from Excel file
df = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')

# Create a MySQL connection using SQLAlchemy
engine = create_engine(database_uri)

# Iterate through each sheet in the Excel file
for sheet_name, sheet_data in df.items():
    inspector = inspect(engine)
    if not inspector.has_table(sheet_name):
        sheet_data.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
        print(f"Table '{sheet_name}' created in the database.")

    # Validate date format and range
    date_column = 'DateNaissance'
    valid_dates = pd.to_datetime(sheet_data[date_column], errors='coerce')
    invalid_dates = sheet_data[valid_dates.isnull()]

    # Check for invalid dates based on your specific criteria
    invalid_dates = invalid_dates[
        ~invalid_dates[date_column].str.match(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}$') |
        ~invalid_dates[date_column].str.contains(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}$') |
        (invalid_dates[date_column].str.split('/').str[-1].astype(str).str.contains(r'\d{5}')) |
        (invalid_dates[date_column].str.contains(r'/-|-\d')) |
        (invalid_dates[date_column].str.split('/').str[0].astype(float).fillna(-1).astype(int) > 31) |
        (invalid_dates[date_column].str.split('/').str[1].str.split('-').str[0].astype(float).fillna(-1).astype(int) > 12) |
        (invalid_dates[date_column].str.split('/').str[2].astype(float).fillna(-1).astype(int) > 2023)
    ]

    # Check for duplicates within the sheet
    duplicate_rows = sheet_data[sheet_data.duplicated(subset='Numero', keep=False)]

    # Handle duplicate rows and invalid dates
    if not duplicate_rows.empty:
        first_occurrence = ~sheet_data['Numero'].duplicated(keep='first')
        first_rows = sheet_data[first_occurrence]
        error_rows = sheet_data[~first_occurrence]
        # Insert first occurrences into the database
        first_rows.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
        print(f"First occurrences inserted into '{sheet_name}' successfully.")
        # Set the 'Error' column for duplicate row errors using .loc with .copy()
        error_rows = error_rows.copy()
        error_rows.loc[:, 'Error'] = 'Duplicate Row Error'

        # Set the 'Error' column for invalid date errors using .loc with .copy()
        invalid_dates = invalid_dates.copy()
        invalid_dates.loc[:, 'Error'] = 'Invalid Date Error'
        # Combine both types of errors into a single DataFrame
        combined_error = pd.concat([error_rows, invalid_dates], ignore_index=True)
    
        # Create an error Excel file with all errors
        error_file_name = f"{sheet_name}_error.xlsx"
        combined_error.to_excel(error_file_name, index=False)
        print(f"Error file '{error_file_name}' created with rows containing errors.")
    intersection_data = sheet_data[(first_occurrence) & (~sheet_data.index.isin(invalid_dates.index))]
    if not invalid_dates.empty:
        intersection_data.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
        print(f"Intersection of non-duplicated and valid data inserted into '{sheet_name}' successfully.")
    else:
        # Insert all data into the database
        sheet_data.to_sql(sheet_name, con=engine, index=False, if_exists='append')
        print(f"Data inserted into '{sheet_name}' successfully.")

engine.dispose()
print("Process completed.")
