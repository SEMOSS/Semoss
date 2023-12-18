import fitz # pip install PyMuPDF
from pathlib import Path
from transformers import pipeline
import os
import torch
import csv
import pandas as pd

# just give the source file and all set

class PDFUtil:

  def __init__(self, source_file=None, target_folder=None, detect_tables=False):
    assert source_file is not None 
    self.pdf = source_file
    pdf_path = Path(self.pdf)
    self.doc_name = pdf_path.stem
    self.doc = fitz.open(self.pdf) # open a document
    if target_folder is None:
      self.target_folder = pdf_path.parent.absolute()
    else:
      self.target_folder = target_folder
    # load the microsoft object detection to check if it is a table
    self.detector = None
    # we do need a way to load gaas_gpt_model locally so that way we dont need to switch to remote later
    if detect_tables:
      device = "cpu"
      if torch.cuda.is_available():
        device="cuda"
      self.detector = pipeline("object-detection", model="microsoft/table-transformer-detection", device=device)
      
    self.extract_images()
    self.extract_tables()
      
  def extract_images(self):
    # extracts the images and indexes it into the target_folder as table_img_<page_imagenum>
    for page_index in range(len(self.doc)):
      page = self.doc[page_index]
      image_list = page.get_images()
      
      # process each image
      for image_index,img in enumerate(image_list, start=1):
        xref = img[0]
        pix = fitz.Pixmap(self.doc, xref)
        
        if pix.n - pix.alpha > 3:
          pix = fitz.Pixmap(fitz.csRGB, pix)
        
        file_name = f'{self.target_folder}/{self.doc_name}__p_{page_index}__i_{image_index}'
        pix.save(f"{file_name}.png")
        
        if self.is_table(f"{file_name}.png"):
          #os.rename(f'{file_name}.png',f'{file_name}.table') 
          self.convert_image_to_pdf(f"{file_name}.png")
          
        pix = None  
  
  def is_table(self, png_file):
    if self.detector is None:
      return True
    else:
      table_def = self.detector(png_file)
      if table_def is not None and isinstance(table_def, list):
        #print(f"{png_file} is a table")
        return len(table_def) > 0
      else:
        return false
      

  def extract_tables(self):
    for page_index in range(len(self.doc)):
      page = self.doc[page_index]
      tabs = page.find_tables()
      
      for tab_index, tab in enumerate(tabs, start=1):

        print(f"Processing page {page_index}")
        #print(f"Page {page_index} >> Table {tab_index}")
        file_name = f'{self.target_folder}/{self.doc_name}__p_{page_index + 1}__t_{tab_index}.png'
        
        #fwriter = csv.writer(f, delimiter=',', escapechar=' ', quoting=csv.QUOTE_NONE)
        # if the headers are not empty
        if not self.is_list_empty(tab.header.names):
          try:
            bbox = tab.bbox
            table_rect = fitz.Rect(bbox)
            pix = page.get_pixmap(matrix=fitz.Matrix(3,3), clip=table_rect)
            
            if pix.n - pix.alpha > 3:
              pix = fitz.Pixmap(fitz.csRGB, pix)
            
            pix.save(f"{file_name}")
            self.convert_image_to_pdf(file_name)
          except:
            print(f"Failed .. {file_name}")
            pass        
  
  def is_list_empty(self, input_list):
    empty = True
    for thing in input_list:
      empty = empty and (len(thing) == 0)
    return empty
    
  def fill_header(self, input_list):
    count = 0
    output_list = []
    for item in input_list:
      if len(item) == 0:
        output_list.append(f"H{count}")
        count = count +1
      else:
        output_list.append(item)
    return output_list
  
  def convert_image_to_pdf(self, file_name):
    pdf_img = fitz.open(file_name)
    pdf_bytes = pdf_img.convert_to_pdf()
    image_pdf_file = open(f'{file_name}.pdf', 'wb')
    image_pdf_file.write(pdf_bytes)
    image_pdf_file.close()
  
  