from typing import (
    List,
    Any,
    Optional
)

import fitz # pip install PyMuPDF
from pathlib import Path
import os
import pandas as pd
from dataclasses import dataclass, asdict

import table_util

@dataclass
class ExtractedItem:
    """
    """    
    source: str
    modality: str
    divider: int
    part: int
    content: str
    
class ExtractedContent:
    
    def __init__(
        self
    ):
        self.rows = []
        
    def append(
        self,
        extracted_item: ExtractedItem
    ) -> None:
        self.rows.append(extracted_item)
        
    def to_pandas(
        self
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            [asdict(row) for row in self.rows]
        )
        
        # Capitalize the first letter of each column header
        df.rename(columns=lambda x: x.capitalize(), inplace=True)
        
        return df
        
# just give the source file and all set    
class PDFUtil:

    def __init__(
        self, 
        source_file: str, 
        target_folder: Optional[str] = None, 
        detect_tables: bool = False
    ):

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

        
    def close_document(
        self
    ) -> None:
        self.doc.close()
        
    def extract_items_from_pdf(
        self
    ) -> List:
        extracted_content = ExtractedContent()
                
        for page_index in range(len(self.doc)):
            page = self.doc[page_index]
            
            if page_index == 54 or page_index == 55:
                print(page_index)
                
            text_from_page = self.extract_text(page=page)
            extracted_content.append(
                ExtractedItem(
                    source=self.doc_name,
                    modality='text',
                    divider=page_index+1,
                    part=0,
                    content = text_from_page
                )
            )
            
            image_data = self.extract_images(page=page)
            for i in range(len(image_data)):
                with open(image_data[i], "r") as f:
                    extracted_content.append(
                        ExtractedItem(
                            source=self.doc_name,
                            modality='image',
                            divider=page_index+1,
                            part=i,
                            content = f.read()
                        )
                    )
                    
            table_data = self.extract_tables(page=page)
            for i in range(len(table_data)):
                with open(table_data[i], "r") as f:
                    extracted_content.append(
                        ExtractedItem(
                            source=self.doc_name,
                            modality='table',
                            divider=page_index+1,
                            part=i,
                            content = f.read()
                        )
                    )
        
        return extracted_content
        
    def extract_text(
        self,
        page: fitz.fitz.Page
    ) -> str:
        return page.get_text("text")
      
    def extract_images(
        self,
        page: fitz.fitz.Page
    ) -> None:
        # extracts the images and indexes it into the target_folder as table_img_<page_imagenum>
        # for page_index in range(len(self.doc)):
        #     page = self.doc[page_index]
        #     image_list = page.get_images()
        #     print(f"Processing Page for Image {page_index}")
        extracted_items = []
        print(f"Processing Page for Image {page}")
        image_list = page.get_images()        

        # process each image
        for image_index,img in enumerate(image_list, start=1):
            xref = img[0]
            pix = fitz.Pixmap(self.doc, xref)
            
            if pix.n - pix.alpha > 3:
                pix = fitz.Pixmap(fitz.csRGB, pix)
            
            # we can directly move this to PIL
            # data = pix.getImageData("format")
            # todo - https://github.com/pymupdf/PyMuPDF/issues/322
            
            file_name = f'{self.target_folder}/{self.doc_name}__p_{page.number}__i_{image_index}.png'
            pix.save(file_name)

            # only process if > 5kb
            size = (os.stat(file_name).st_size)/1024

            if size > 5 and self.is_table(file_name):
                #os.rename(f'{file_name}.png',f'{file_name}.table') 
                # self.convert_image_to_pdf(file_name)
                #os.remove(f"{file_name}.png")
                #process this table 
                #print("parsing image to table")
                extracted_text = self.detector.parse_table_to_csv(pil_image=file_name)
                extracted_items.append(extracted_text) if extracted_text is not None else None

            pix = None
        
        return extracted_items
  
    def is_table(
        self, 
        png_file:str
    ) -> bool:
        if self.detector is None:
            return True # forcing it to be 1
        else:
            return self.detector.is_table(png_file)

    def extract_tables(
        self,
        page: fitz.fitz.Page
    ) -> None:
        # for page_index in range(len(self.doc)):
        #     page = self.doc[page_index]
            
        try:
            tabs = page.find_tables()
        except AttributeError as e:
            if f"{e}" == "'Page' object has no attribute 'find_tables'":
                return []
            else:
                raise e
        
        print(f"Processing Page for Table {page.number}")
        extracted_items = []
        for tab_index, tab in enumerate(tabs, start=1):

            #print(f"Page {page_index} >> Table {tab_index}")
            file_name = f'{self.target_folder}/{self.doc_name}__p_{page.number + 1}__t_{tab_index}.png'
            
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
                    extracted_text = self.detector.parse_table_to_csv(pil_image=file_name)
                    extracted_items.append(extracted_text) if extracted_text is not None else None
                    #self.convert_image_to_pdf(file_name)
                    #os.remove(file_name)
                except:
                    print(f"Failed .. {file_name}")
                    pass        
                
        return extracted_items
  
    def is_list_empty(
        self, 
        input_list:List
    ) -> bool:
        empty = True
        for thing in input_list:
            empty = empty and (len(thing) == 0)
        return empty
    
    def fill_header(
        self, 
        input_list: List
    ) -> List[Any]:
        count = 0
        output_list = []
        for item in input_list:
            if len(item) == 0:
                output_list.append(f"H{count}")
                count = count +1
            else:
                output_list.append(item)
                
        return output_list
  
    def convert_image_to_pdf(
        self, 
        file_name:str
    ) -> None:
        pdf_img = fitz.open(file_name)
        pdf_bytes = pdf_img.convert_to_pdf()
        image_pdf_file = open(f'{file_name}.pdf', 'wb')
        image_pdf_file.write(pdf_bytes)
        image_pdf_file.close()