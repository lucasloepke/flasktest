"""Utilities for SAC Monitoring Scripts"""

import os
import csv
import tkinter as tk
from tkinter.filedialog import askopenfilename

def read_file(file_name):
    file = []
    directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(directory, file_name)
    with open(file_path) as f:
        file =  f.read()
    return file

def read_csv_file(file_name):
    file = []
    directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(directory, file_name)
    with open(file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, dialect=csv.excel)
        for row in csv_reader:
            file.append(row)
    return file

def write_csv_file(file_name, data):
    directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(directory, file_name)
    keys = data[0].keys()
    with open(file_path, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)      

def file_select():
    tk.Tk().withdraw() # part of the import if you are not using other tkinter functions
    fn = askopenfilename()
    return fn

