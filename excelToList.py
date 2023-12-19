import pandas as pd
from sqlalchemy import create_engine, inspect
import re
import os
#excel_file = 'test.xlsx'
database_uri = 'mysql+mysqlconnector://root:1234@localhost:3306/excel'


def process_excel(excel_file, status_label):
    try:
        # Load data from Excel file
        df = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')

        # Create a MySQL connection using SQLAlchemy
        engine = create_engine(database_uri)

        # Initialize combined_error DataFrame before the loop
        combined_error = pd.DataFrame()

        # Iterate through each sheet in the Excel file
        for sheet_name, sheet_data in df.items():
            inspector = inspect(engine)
            if not inspector.has_table(sheet_name):
                sheet_data.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
                status_label.config(text=f"Table '{sheet_name}' created in the database.")

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



        # Check for valid email addresses
            valid_emails = sheet_data['Email'].apply(lambda x: bool(re.match(r'^[\w\.-]+@[a-zA-Z\d\.-]+\.[a-zA-Z]{2,}$', str(x))))
            invalid_email_rows = sheet_data[~valid_emails]

            # Handle invalid email addresses
            if not invalid_email_rows.empty:
                # Set the 'Error' column for invalid email errors using .loc with .copy()
                invalid_email_rows = invalid_email_rows.copy()
                invalid_email_rows.loc[:, 'Error'] = 'Invalid Email Error'
                # Combine both types of errors into a single DataFrame
                combined_error = pd.concat([combined_error, invalid_email_rows], ignore_index=True)

                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors, including invalid emails.")
            else:
                status_label.config(text="No invalid email addresses found.")

            sheet_data = sheet_data[valid_emails]


            # Check for valid telephone numbers
            valid_telephones = sheet_data['Telephone'].apply(lambda x: bool(re.match(r'^[0-9]{9}$', str(x))))
            invalid_telephone_rows = sheet_data[~valid_telephones]

            # Handle invalid telephone numbers
            if not invalid_telephone_rows.empty:
                # Set the 'Error' column for invalid telephone errors using .loc with .copy()
                invalid_telephone_rows = invalid_telephone_rows.copy()
                invalid_telephone_rows.loc[:, 'Error'] = 'Invalid Telephone Error'
                # Combine both types of errors into a single DataFrame
                combined_error = pd.concat([combined_error, invalid_telephone_rows], ignore_index=True)

                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors, including invalid telephones.")
                
                # Exclude rows with invalid telephone numbers from the data to be inserted into the database
                sheet_data = sheet_data[valid_telephones]



                # Check for valid 'Nom' and 'Prenom' columns
            valid_nom_prenom = sheet_data[['Nom', 'Prenom']].apply(lambda x: all(isinstance(val, str) and len(str(val)) <= 15 for val in x), axis=1)
            invalid_nom_prenom_rows = sheet_data[~valid_nom_prenom]

            # Handle invalid 'Nom' and 'Prenom' values
            if not invalid_nom_prenom_rows.empty:
                # Set the 'Error' column for invalid 'Nom' and 'Prenom' errors using .loc with .copy()
                invalid_nom_prenom_rows = invalid_nom_prenom_rows.copy()
                invalid_nom_prenom_rows.loc[:, 'Error'] = 'too long Nom or Prenom Error'
                # Combine both types of errors into a single DataFrame
                combined_error = pd.concat([combined_error, invalid_nom_prenom_rows], ignore_index=True)

                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors, including invalid 'Nom' or 'Prenom' values.")
                
                # Exclude rows with invalid 'Nom' or 'Prenom' values from the data to be inserted into the database
                sheet_data = sheet_data[valid_nom_prenom]


            # Check for duplicates within the sheet for 'Numero', 'Email', and 'Telephone' columns
            duplicate_rows_numero = sheet_data[sheet_data.duplicated(subset=sheet_data.columns[0], keep=False)]
            duplicate_rows_email = sheet_data[sheet_data.duplicated(subset=sheet_data.columns[4], keep=False)]
            duplicate_rows_telephone = sheet_data[sheet_data.duplicated(subset=sheet_data.columns[3], keep=False)]

            # Handle duplicate rows and invalid dates
            first_occurrence_numero = ~sheet_data[sheet_data.columns[0]].duplicated(keep='first')
            first_occurrence_email = ~sheet_data[sheet_data.columns[4]].duplicated(keep='first')
            first_occurrence_telephone = ~sheet_data[sheet_data.columns[3]].duplicated(keep='first')

            if not duplicate_rows_numero.empty:
                # Handle duplicate rows for 'Numero'
                first_rows_numero = sheet_data[first_occurrence_numero]
                error_rows_numero = sheet_data[~first_occurrence_numero]
                # Insert first occurrences into the database
                first_rows_numero.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
                status_label.config(text=f"First occurrences of 'Numero' inserted into '{sheet_name}' successfully.")
                # Set the 'Error' column for duplicate row errors using .loc with .copy()
                error_rows_numero = error_rows_numero.copy()
                error_rows_numero.loc[:, 'Error'] = 'Duplicate Numero Error'
                # Combine both types of errors into a single DataFrame
                combined_error = pd.concat([combined_error, error_rows_numero], ignore_index=True)
            
                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors.")
            
            if not duplicate_rows_email.empty:
                # Handle duplicate rows for 'Email'
                first_rows_email = sheet_data[first_occurrence_email]
                error_rows_email = sheet_data[~first_occurrence_email]
                # Insert first occurrences into the database
                first_rows_email.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
                status_label.config(text=f"First occurrences of 'Email' inserted into '{sheet_name}' successfully.")
                # Set the 'Error' column for duplicate row errors using .loc with .copy()
                error_rows_email = error_rows_email.copy()
                error_rows_email.loc[:, 'Error'] = 'Duplicate Email Error'
                # Combine both types of errors into a single DataFrame
                combined_error = pd.concat([combined_error, error_rows_email], ignore_index=True)
            
                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors.")

            if not duplicate_rows_telephone.empty:
                # Handle duplicate rows for 'Telephone'
                first_rows_telephone = sheet_data[first_occurrence_telephone]
                error_rows_telephone = sheet_data[~first_occurrence_telephone]
                # Insert first occurrences into the database
                first_rows_telephone.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
                status_label.config(text=f"First occurrences of 'Telephone' inserted into '{sheet_name}' successfully.")
                # Set the 'Error' column for duplicate row errors using .loc with .copy()
                error_rows_telephone = error_rows_telephone.copy()
                error_rows_telephone.loc[:, 'Error'] = 'Duplicate Telephone Error'
                # Combine both types of errors into a single DataFrame
                combined_error = pd.concat([combined_error, error_rows_telephone], ignore_index=True)
            
                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors.")


            # Set the 'Error' column for invalid date errors using .loc with .copy()
            invalid_dates = invalid_dates.copy()
            invalid_dates.loc[:, 'Error'] = 'Invalid Date Error'
            # Combine both types of errors into a single DataFrame
            combined_error = pd.concat([combined_error, invalid_dates], ignore_index=True)
            
            # Create an error Excel file with all errors
            error_file_name = f"{sheet_name}_error.xlsx"
            combined_error.to_excel(error_file_name, index=False)
            status_label.config(text=f"Error file '{error_file_name}' created with rows containing errors.")
            
            # Check for empty values in 'Telephone,' 'Email,' and 'Numero' columns
            empty_values = sheet_data[sheet_data[[sheet_data.columns[3], sheet_data.columns[4], sheet_data.columns[0]]].isnull().any(axis=1)]

            # Handle empty values
            if not empty_values.empty:
                # Set the 'Error' column for empty value errors using .loc with .copy()
                empty_values = empty_values.copy()
                empty_values.loc[:, 'Error'] = 'Empty Value Error'
            
                # Combine empty value errors with other types of errors
                combined_error = pd.concat([combined_error, empty_values], ignore_index=True)

                # Create an error Excel file with all errors
                error_file_name = f"{sheet_name}_error.xlsx"
                combined_error.to_excel(error_file_name, index=False)
                status_label.config(text=f"Error file '{error_file_name}' updated with rows containing errors, including empty values.")
            else:
                status_label.config(text="No empty values found.")
            
            # Define intersection_data inside the for loop to avoid the NameError
            intersection_data = sheet_data[(first_occurrence_numero) & (~sheet_data.index.isin(invalid_dates.index))]
            
            if not invalid_dates.empty:
                intersection_data.to_sql(sheet_name, con=engine, index=False, if_exists='replace')
                status_label.config(text=f"Intersection of non-duplicated and valid data inserted into '{sheet_name}' successfully.")
            else:
                # Insert all data into the database
                sheet_data.to_sql(sheet_name, con=engine, index=False, if_exists='append')
                status_label.config(text=f"Data inserted into '{sheet_name}' successfully.")

        engine.dispose()
        status_label.config(text="Process completed.")
        #open the error file
        os.startfile(error_file_name)
    except Exception as e:
        # Handle exceptions and update the status label with the error message
        status_label.config(text=f"Error: {str(e)}")

