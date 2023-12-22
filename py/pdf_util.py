import fitz # pip install PyMuPDF
from pathlib import Path
from transformers import pipeline
import os
import torch
import csv
import pandas as pd
import table_util



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
    self.detector = table_util.TableUtil()
    # we do need a way to load gaas_gpt_model locally so that way we dont need to switch to remote later
    
    self.extract_images()
    self.extract_tables()
      
  def extract_images(self):
    # extracts the images and indexes it into the target_folder as table_img_<page_imagenum>
    for page_index in range(len(self.doc)):
      page = self.doc[page_index]
      image_list = page.get_images()
      print(f"Processing Page for Image {page_index}")
      
      # process each image
      for image_index,img in enumerate(image_list, start=1):
        xref = img[0]
        pix = fitz.Pixmap(self.doc, xref)
        
        if pix.n - pix.alpha > 3:
          pix = fitz.Pixmap(fitz.csRGB, pix)
          
        # we can directly move this to PIL
        # data = pix.getImageData("format")
        # todo - https://github.com/pymupdf/PyMuPDF/issues/322
        
        file_name = f'{self.target_folder}/{self.doc_name}__p_{page_index}__i_{image_index}.png'
        pix.save(file_name)

        # only process if > 5kb
        size = (os.stat(file_name).st_size)/1024

        if size > 5 and self.is_table(file_name):
          #os.rename(f'{file_name}.png',f'{file_name}.table') 
          # self.convert_image_to_pdf(file_name)
          #os.remove(f"{file_name}.png")
          #process this table 
          #print("parsing image to table")
          self.detector.parse_table_to_csv(pil_image=file_name)
        pix = None  
  
  def is_table(self, png_file):
    if self.detector is None:
      return True # forcing it to be 1
    else:
      return self.detector.is_table(png_file)

  def extract_tables(self):
    for page_index in range(len(self.doc)):
      page = self.doc[page_index]
      tabs = page.find_tables()
      print(f"Processing Page for Table {page_index}")
      
      for tab_index, tab in enumerate(tabs, start=1):

        #print(f"Page {page_index} >> Table {tab_index}")
        file_name = f'{self.target_folder}/{self.doc_name}__p_{page_index + 1}__t_{tab_index}.png'
        
        #fwriter = csv.writer(f, delimiter=',', escapechar=' ', quoting=csv.QUOTE_NONE)
        # if the headers are not empty
        if not self.is_list_empty(tab.header.names):
          try:
            bbox = tab.bbox
            # need to pad the bbox
            new_bbox = (bbox[0] - self.detector.left, bbox[1] - self.detector.bottom, bbox[2] + self.detector.right, bbox[3]+self.detector.top)
            table_rect = fitz.Rect(bbox)
            pix = page.get_pixmap(matrix=fitz.Matrix(3,3), clip=table_rect)
            
            if pix.n - pix.alpha > 3:
              pix = fitz.Pixmap(fitz.csRGB, pix)
            
            pix.save(f"{file_name}")
            self.detector.parse_table_to_csv(pil_image=file_name)
            #self.convert_image_to_pdf(file_name)
            #os.remove(file_name)
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
  
  