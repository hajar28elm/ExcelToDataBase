import tkinter as tk
from tkinter import filedialog
import threading
from excelToList import process_excel  # Import the function from the other file

def import_and_process_excel():
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])

    if file_path:
        # Update the status label
        status_label.config(text="Processing Excel file...")

        # Run the processing script in a separate thread
        threading.Thread(target=process_excel, args=(file_path, status_label), daemon=True).start()

# Create the main window
root = tk.Tk()
root.title("Excel Processor")

# Create and place the import button
import_button = tk.Button(root, text="Import and Process Excel", command=import_and_process_excel)
import_button.pack(pady=20)

# Create and place the status label
status_label = tk.Label(root, text="Ready to import.")
status_label.pack(pady=10)

# Run the Tkinter event loop
root.mainloop()
