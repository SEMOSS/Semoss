#from wand.image import Image
import os
import io

from typing import Dict, List, Optional
from pdf2image import convert_from_path
from PIL import Image
from pypdf import PdfReader

import pytesseract
import cv2
import numpy as np

class TesseractOcrClient():

    def __init__(
            self, 
            tesseract_path: Optional[str],
            poppler_path: Optional[str],
            language = 'eng',
            clean_results = False
        ):
        self.tesseract_path = tesseract_path
        self.poppler_path = poppler_path
        self.language = language
        self.clean_results = clean_results

    def ocr_document(self, file_path: str, preprocessing = False) -> List[str]:

        # set the tesseract path manually if provided
        if (self.tesseract_path is not None):
            pytesseract.pytesseract.tesseract_cmd = self.tesseract_path
        
        # set the poppler path manually if provided and convert pdf to imager
        if (self.poppler_path is not None):
            images = convert_from_path(file_path, poppler_path=self.poppler_path)
        else:
            images = convert_from_path(file_path)

        # preprocess converted images
        if preprocessing is True:
            images = self.preprocess_images(images)

        results = []

        # do the actual ocr step on each image converted from the pdf and append results
        for i, image in enumerate(images):
            current = {}
            text = pytesseract.image_to_string(image, lang=self.language)

            if self.clean_results:
                text = self.clean_text(text)

            current['text'] = text
            current['page_number'] = str(i + 1)
            results.append(current)
        
        return results
    
    def preprocess_images(self, pil_images, grayscale=True, threshholding=True) -> List:
        numpy_images = [np.array(image) for image in pil_images]

        result = []
        for image in numpy_images:

            # convert to opencv format
            opencv_image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)

            # convert to grayscale image
            if grayscale:
                opencv_image = cv2.cvtColor(opencv_image, cv2.COLOR_BGR2GRAY)

            # apply thresholding to convert image to a binary image
            if threshholding:
                _, opencv_image = cv2.threshold(opencv_image, 150, 255, cv2.THRESH_BINARY)

            result.append(opencv_image)

        return result
    
    # clean the text of unwanted characters
    def clean_text(self, text: str):
        cleaned_text = text.replace("\n", "").replace("\r", "")
        
        return cleaned_text
    
    # TODO: Implement method to identify and split up images from a pdf:
    def identify_images_single_page(self, file_path: str):

        reader = PdfReader(file_path)

        page = reader.pages[0]

        # extract images and write them to a different file
        # TODO: need to figure out what format this is writing in
        for count, image_file_object in enumerate(page.images):
            with open(str(count) + image_file_object.name, "wb") as fp:
                fp.write(image_file_object.data)

    def extract_images_from_pdf(self, file_path: str, output_path: str, preprocessing = False):
        reader = PdfReader(file_path)
        image_count = 0
        image_file_dict = {}
        extracted_text = []

        if output_path is None:
            raise ValueError("Invalid output_path: output path is not defined")
        
        os.makedirs(output_path, exist_ok=True)

        for page_num, page in enumerate(reader.pages):
            if "/XObject" in page["/Resources"]:
                xObject = page["/Resources"]["/XObject"].get_object()
                for obj in xObject:
                    if xObject[obj]["/Subtype"] == "/Image":
                        image_count += 1
                        width = xObject[obj]["/Width"]
                        height = xObject[obj]["/Height"]
                        data = xObject[obj].get_data()
                        # JPEG
                        if xObject[obj]["/Filter"] == "/DCTDecode":
                            extension = "jpg"
                            image = Image.open(io.BytesIO(data))
                            image_filename = f"image_page{page_num+1}_{image_count}.{extension}"
                            image_file_path = os.path.join(output_path, image_filename)
                            image.save(image_file_path)
                            image_file_dict[image_file_path] = page_num

                        # FlateDecode (raw data)
                        elif xObject[obj]["/Filter"] == "/FlateDecode":
                            extension = "png"
                            image = Image.frombytes("L", (width, height), data)
                            image_filename = f"image_page{page_num+1}_{image_count}.{extension}"
                            image_file_path = os.path.join(output_path, image_filename)
                            image.save(image_file_path)
                            image_file_dict[image_file_path] = page_num
        
        extracted_text = self.ocr_image(image_file_dict, preprocessing)
        self.delete_temp_images(image_file_dict.keys())

        return extracted_text



    # TODO: Implement method to ocr images extracted
    def ocr_image(self, file_paths: Dict[str, int], preprocessing = False):

        # set the tesseract path manually if provided
        if (self.tesseract_path is not None):
            pytesseract.pytesseract.tesseract_cmd = self.tesseract_path

        results = []

        for file_path, page_num in file_paths.items():
            # file_path = "/Users/michaemoore/Documents/SEMOSS/workspace/Semoss/temp/ocr_images/image_page1_1.jpg"
            current = {}
            image = Image.open(file_path)

            if preprocessing:
                images = [image]
                images = self.preprocess_images(images)
                image = images[0]

            text = pytesseract.image_to_string(image, lang=self.language)

            if self.clean_results:
                text = self.clean_text(text)

            current['text'] = text
            current['page_number'] = str(page_num + 1)
            results.append(current)

        return results
    
    def delete_temp_images(self, file_paths: List[str]):
        for path in file_paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                print(f"File not found: {path}")
            except Exception as e:
                print(f"Error occured deleting {path}: {e}")