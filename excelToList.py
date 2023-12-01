import pandas as pd
file_path = 'C:/Users/HP PRO/OneDrive/Bureau/excelToDataBase/test.xlsx'
data_frame = pd.read_excel(file_path)

data_list = data_frame.values.tolist()

print(data_list)