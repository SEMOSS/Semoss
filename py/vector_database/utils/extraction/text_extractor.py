import logging
import os

logger = logging.getLogger(__name__)

def extract_text(
    source_file_name: str,
    target_folder: str,
    output_file_name: str
) -> int:
    '''
    Extracts text content from a PDF file and saves it to a CSV file.

    Args:
        source_file_name (`str`): Path to the source PDF file.
        target_folder (`str`): Path to the folder where extracted items will be saved.
        output_file_name (`str`): Name of the output CSV file.

    Returns:
        `int`: Number of rows (text entries) saved to the CSV file.
    '''
    
    from .pdf_util import PDFUtil
    
    pdf_extractor = PDFUtil(
        source_file=source_file_name,
        target_folder=target_folder
    )
    
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    
    extacted_content = pdf_extractor.extract_items_from_pdf()
    
    df = extacted_content.to_pandas()
        
    # Filter out rows with NaN or empty strings in the 'Content' column
    df = df[df['Content'] != '']
    
    # Replace empty strings with NaN in the 'Content' column
    df = df.dropna(subset=['Content'])
    df.reset_index(inplace=True, drop=True) # reset the index to make it a clean df
    
    df.to_csv(output_file_name, index = False)
    
    return df.shape[0]