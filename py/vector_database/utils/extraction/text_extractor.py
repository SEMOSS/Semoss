import logging
import os

logger = logging.getLogger(__name__)

def extract_text(
    source_file_name: str,
    target_folder: str,
    output_file_name: str
) -> int:
    from .pdf_util import PDFUtil
    
    pdf_extractor = PDFUtil(
        source_file=source_file_name,
        target_folder=target_folder
    )
    
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    
    extacted_content = pdf_extractor.extract_items_from_pdf()
    
    df = extacted_content.to_pandas()
    
    df.to_csv(output_file_name, index = False)
    
    # Filter out rows with NaN or empty strings in the 'Content' column
    df = df[df['Content'] != '']
    
    # Replace empty strings with NaN in the 'Content' column
    df = df.dropna(subset=['Content'])
    df.reset_index(inplace=True, drop=True) # reset the index to make it a clean df
    
    return df.shape[0]