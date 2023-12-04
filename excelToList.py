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

excel_file = 'test.xlsx'  # Excel file name is 'test.xlsx'
database_uri = 'mysql+mysqlconnector://root:@localhost:3306/excel'

# Load data from Excel file
df = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')

# Create a MySQL connection using SQLAlchemy
engine = create_engine(database_uri)

# Iterate through each sheet in the Excel file
for sheet_name, sheet_data in df.items():
    inspector = inspect(engine)
    # Check if the table exists in the database
    if not inspector.has_table(sheet_name):
        # Create the table in the database
        sheet_data.to_sql(sheet_name, con=engine, index=False,if_exists='replace')
        print(f"Table '{sheet_name}' created in the database.")

    # Check for duplicates within the sheet
    duplicate_rows = sheet_data[sheet_data.duplicated(subset='Numero', keep=False)]

    # If there are duplicates, insert the first occurrence and create an error Excel file for the rest
    if not duplicate_rows.empty:
        first_occurrence = ~sheet_data['Numero'].duplicated(keep='first')
        first_rows = sheet_data[first_occurrence]
        error_rows = sheet_data[~first_occurrence]

        # Insert first occurrences into the database
        first_rows.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
        print(f"First occurrences inserted into '{sheet_name}' successfully.")

        # Create an error Excel file for duplicate occurrences
        error_file_name = f"{sheet_name}_duplicate_rows_error.xlsx"
        error_rows.to_excel(error_file_name, index=False)
        print(f"Error file '{error_file_name}' created with duplicate rows that already exist in the database.")
    else:
        # If no duplicates, insert all data into the database
        sheet_data.to_sql(sheet_name, con=engine, index=False, if_exists='append')
        print(f"Data inserted into '{sheet_name}' successfully.")

engine.dispose()
print("Process completed.")
