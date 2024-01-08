
# main class responsible for giving all table specific operations
from paddleocr import PaddleOCR
import gaas_gpt_model as ggm
import pytesseract as tess
from PIL import Image
from pathlib import Path
import pandas as pd
import random
import string
import numpy as np
import os
import cv2
from paddleocr import PPStructure,save_structure_res
from paddleocr.ppstructure.recovery.recovery_to_doc import sorted_layout_boxes, convert_info_docx
import torch
import numpy
import json


class TableUtil:

  def __init__(self, target_folder=None):
    import gaas_gpt_model as ggm
    self.structure_recognizer = ggm.ModelEngine(local=True, engine_id="microsoft/table-transformer-structure-recognition", pipeline_type="object-detection")
    self.target_folder = target_folder
    self.ocr = PaddleOCR(use_angle_cls=True, lang="en")
    
    #self.detector = pipeline("object-detection", model="microsoft/table-transformer-detection", device=device)
    self.detector = ggm.ModelEngine(pipeline_type="object-detection", engine_id="microsoft/table-transformer-detection", local=True)
    self.top = 50
    self.bottom = 90
    self.left = 40
    self.right = 40
    self.table_engine = PPStructure(recovery=True, lang='en')

  def parse_table_to_csv(self, pil_image=None, target_folder=None):
    return self.parse_table_to_csv_paddle(pil_image, target_folder)

    
  def parse_table_to_csv_ms(self, pil_image=None, target_folder=None):
    # passes the table through the pipeline_type
    #
    if target_folder is None:
      target_folder = self.target_folder
    image_name = self.generate_random()
    # see this is an image file
    if "PIL." not in str(type(pil_image)):
      image_path = Path(pil_image)
      image_name = image_path.stem
      pil_image = Image.open(pil_image)
      if target_folder is None:
        target_folder = image_path.parent.absolute()
    
    # resize the image a little
    width, height = pil_image.size
    new_width = width + self.right + self.left
    new_height = height + self.top + self.bottom
    color = (255, 255, 255)
    result = Image.new(pil_image.mode, (new_width, new_height), color)
    result.paste(pil_image, (self.left, self.top))
    pil_image = result
    assert target_folder is not None
    
    raw_output = self.structure_recognizer.model(pil_image)
    #print(raw_output)
    rows = [entry for entry in raw_output if entry['label'] == 'table row']
    columns = [entry for entry in raw_output if entry['label'] == 'table column']
    
    rows.sort(key=lambda x: x['box']['ymin'])
    columns.sort(key=lambda x: x['box']['xmin'])
    
    cell_coordinates = []
    
    for row in rows:
      row_cells = []
      for column in columns:
        xmin = self.adjust_padding(column['box']['xmin'], -20)
        ymin = self.adjust_padding(row['box']['ymin'], -30)
        xmax = self.adjust_padding(column['box']['xmax'])
        ymax = self.adjust_padding(row['box']['ymax'],40)
      
        cell_bbox = [xmin, ymin, xmax, ymax]
        row_cells.append({'column': column['box'], 'cell': cell_bbox})
      
      # Sort cells in the row by X coordinate
      row_cells.sort(key=lambda x: x['column']['xmin'])
      
      # Append row information to cell_coordinates
      cell_coordinates.append({'row': row['box'], 'cells': row_cells, 'cell_count': len(row_cells)})

    # Sort rows from top to bottom
    cell_coordinates.sort(key=lambda x: x['row']['ymin'])
    #print(cell_coordinates)

    # we now have a semblance of a table but with coordinates
    # OCR this and convert it into cells 
    # this will be done as a list of lists
    csv = []
    header = []
    check_size = True
    for row in cell_coordinates:
      csv_row = []
      for column in row['cells']:
        # crop the image and generate the data
        #print(f"Column.. {column}")
        bbox = column['cell']
        print(f"Cropper.. {bbox}")
        cell_image = pil_image.crop(bbox)
        
        # get the data from it 
        # this is pytess
        #data = tess.image_to_string(cell_image)
        
        #this is paddleocr
        data_output = self.ocr.ocr(np.asarray(cell_image), cls=True)
        print(data_output)
        if data_output is not None and data_output[0] is not None and data_output[0][0] is not None and data_output[0][0][1] is not None and data_output[0][0][1][0] is not None:
          data = data_output[0][0][1][0]
        else:
          data = ""
        print(f"Value.. {data}")
        csv_row.append(data)
      if len(header) == 0:
        header = csv_row
      else:
        check_size = check_size and len(header) == len(csv_row)
        csv.append(csv_row)
   
    frame = pd.DataFrame(columns=header,data=csv, index=None)
    frame.to_csv(f"{target_folder}/{image_name}.csv")
    return frame, csv
        
  
  def parse_table_to_csv_paddle(self, pil_image=None, target_folder=None):
    print(f"Extracting Table From .. {pil_image}")
    if target_folder is None:
      target_folder = self.target_folder
    image_name = self.generate_random()
    # see this is an image file
    if "PIL." not in str(type(pil_image)):
      image_path = Path(pil_image)
      image_name = image_path.stem
      pil_image = Image.open(pil_image)
      if target_folder is None:
        target_folder = image_path.parent.absolute()
    
    # resize the image a little
    width, height = pil_image.size
    new_width = width + self.right + self.left
    new_height = height + self.top + self.bottom
    #color = (255, 255, 255)
    #result = Image.new(pil_image.mode, (new_width, new_height))
    #result.paste(pil_image, (self.left, self.top))
    #pil_image = result
    #img = cv2.imread(img_path)
    img = cv2.cvtColor(numpy.array(pil_image), cv2.COLOR_RGB2BGR)
    result = self.table_engine(img)
    save_structure_res(result, target_folder, image_name)
    # try to get the headers and save the column names as metadata
    file_name = f"{target_folder}/{image_name}/{result[0]['bbox']}_0.xlsx"
    new_file_name = f"{target_folder}/{image_name}/{image_name}.xlsx"
    if os.path.isfile(file_name):
      if os.path.exists(new_file_name):
        # Delete the existing file
        os.remove(new_file_name)

      os.rename(file_name, new_file_name)
      file_name = new_file_name
      pickle_name = f"{target_folder}/{image_name}/{image_name}.pickle"
      meta_name = f"{target_folder}/{image_name}/{image_name}.meta"
      sent_name = f"{target_folder}/{image_name}/{image_name}.txt"
      
      # capture all of the metadata
      #try:
      df = self.write_frame_meta(new_file_name, meta_name)
      sentences = self.make_csv_to_sentences(df)
      # eventually we should write this all to one file
      with open(sent_name, "w") as f:
        for s in sentences:
          f.write(s)
          
      return sent_name
  
    else:
      return None
      #except :
      #  print("Failed to capture metadata")
      #  pass
    
      # rename the file
      
    
  # should we be ffilling ?
  def write_frame_meta(self, file_name, meta_name):
    df = pd.read_excel(file_name) #.fillna(method='ffill')
    meta = {}
    details = []
    columns = df.columns.values.tolist()
    count = df.count().tolist()
    types = df.dtypes.tolist()
    meta.update({'column_list': columns})
    for column in columns:
      thiscol = {}
      idx = columns.index(column)
      thiscol.update({'name':column})
      thiscol.update({'count': count[idx]})
      thiscol.update({'type': str(types[idx])})
      details.append(thiscol)
    meta.update({'details':details})
    with open(meta_name, "w") as f:
      f.write(json.dumps(meta))
      
    return df
      
  def make_csv_to_sentences(self, df=None, prefix=""):
    if df is None:
      return
    #takes the dataframe headers
    # creates sentences in this format
    # <prefix> column_name with value <row value>
    columns = df.columns.values.tolist()
    all_sent = []
    
    for index, row in df.iterrows():
      row_sentence = prefix
      this_row = list(row)
      for column in list(row):
        if not pd.isna(column):
          #print(column)
          idx = this_row.index(column)
          header_name = columns[idx]
          if len(row_sentence) == 0:
            row_sentence = row_sentence + "  " + header_name + "  has value " + str(column)
          else:
            row_sentence = row_sentence + " and  " + header_name + "  has value " + str(column)
        row_sentence = row_sentence + "."    
      all_sent.append(row_sentence)  
    return all_sent
  
  def adjust_padding(self, input, padding=10):
    input = input + padding
    if input < 0:
      input = 0
    return input
    
  def generate_random(self, size=5):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(size)) 
    
    
  def is_table(self, table_image_file):
    # finds if this is a table
    # if so will also crop it
    table_def = self.detector.model(table_image_file)
    #print(table_def)
    if table_def is not None and isinstance(table_def, list) and len(table_def) > 0:
      #print(f"{png_file} is a table")
      cur_table = Image.open(table_image_file)
      # [{'score': 0.9999748468399048, 'label': 'table', 'box': {'xmin': 85, 'ymin': 9, 'xmax': 1392, 'ymax': 434}}]
      box = table_def[0]['box']
      box = [box['xmin']-self.left, box['ymin'] - self.bottom, box['xmax'] + self.right, box['ymax'] + self.top]
      new_table = cur_table.crop(box)
      cur_table.close()
      new_table.save(table_image_file)
      return True
    else:
      return False
      