package prerna.reactor.vector;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.PixelPlanner;
import prerna.reactor.codeexec.LoadPyFromFileReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import net.sourceforge.tess4j.Tesseract; 
//import net.sourceforge.tess4j.TesseractException; 

public class OcrDocumentImagesReactor extends AbstractReactor {
	
	// TODO: NEED TO OVERRIDE GETREACTORDESCRIPTION
	
	private static final Logger classLogger = LogManager.getLogger(OcrDocumentImagesReactor.class);
	
	//private static final String FILE_PATH_KEY = "file_path";
	private static final String PREPROCESSING_KEY = "preprocessing";
	private static final String FULL_DOCUMENT_KEY = "fullDocument";
	private static final String POPPLER_PATH = "/opt/homebrew/Cellar/poppler/25.04.0/bin";
	private static final String TESSERACT_PATH = "/opt/homebrew/Cellar/tesseract/5.5.0_1/bin/tesseract";
	
	
	public OcrDocumentImagesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), FULL_DOCUMENT_KEY, PREPROCESSING_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		PixelPlanner planner = this.getPixelPlanner();
		
		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		
		Boolean preprocessing = false;
		if (this.keyValue.containsKey(PREPROCESSING_KEY)) {
			preprocessing = Boolean.valueOf(this.keyValue.get(PREPROCESSING_KEY));
		}
		
		Boolean fullDocument = false;
		if (this.keyValue.containsKey(FULL_DOCUMENT_KEY)) {
			fullDocument = Boolean.valueOf(this.keyValue.get(FULL_DOCUMENT_KEY));
		}
		
		if (filePath == null || filePath.trim().isEmpty()) {
			throw new SemossPixelException("Invalid file_path given");
		}

        String outputPath = "/Users/michaemoore/Documents/SEMOSS/workspace/Semoss/temp_new" + DIR_SEPARATOR + "ocr_images";
        
        ArrayList<HashMap<String, String>> extractedText = null;
        
        if (fullDocument) {
        	extractedText = ocrDocument(filePath, preprocessing, this.insight, planner);
        } else {
        	extractedText = ocrDocumentImages(filePath, outputPath, preprocessing, this.insight, planner);
        }
		
		return new NounMetadata(extractedText, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	private static ArrayList<HashMap<String, String>> ocrDocumentImages(String filePath, String outputPath, Boolean preprocessing, Insight insight, PixelPlanner pixelPlanner) {
		PyTranslator pyt = insight.getPyTranslator();
		
		String varName = "ocr";
		StringBuilder callMaker = new StringBuilder("from vector_database import TesseractOcrClient\n");
		callMaker.append(varName + "= TesseractOcrClient(");
		callMaker.append("poppler_path = " + PyUtils.determineStringType(POPPLER_PATH) + ", ");
		callMaker.append("tesseract_path = " + PyUtils.determineStringType(TESSERACT_PATH) + ")\n");
		
		callMaker.append(varName + ".extract_images_from_pdf(");	
		if (filePath != null) {
			callMaker.append("file_path")
					 .append("=")
					 .append(PyUtils.determineStringType(filePath));
		} else {
			throw new IllegalArgumentException("No file path given");
		}

        if (outputPath != null) {
			callMaker.append(", output_path")
					 .append("=")
					 .append(PyUtils.determineStringType(outputPath));
		} else {
			throw new IllegalArgumentException("No output path given");
		}
		
		if (preprocessing != null) {
			callMaker.append(", preprocessing")
					 .append("=")
					 .append(PyUtils.determineStringType(preprocessing));
		}
		
		callMaker.append(")");
		classLogger.debug("Running >>>" + callMaker.toString());
		
		Object output = pyt.runScript(callMaker.toString());
		
		if (output == null) {
			return null;
		}
		
		return (ArrayList<HashMap<String, String>>) output;
	}
	
	private static ArrayList<HashMap<String, String>> ocrDocument(String filePath, Boolean preprocessing, Insight insight, PixelPlanner pixelPlanner) {
		PyTranslator pyt = insight.getPyTranslator();
		
		String varName = "ocr";
		StringBuilder callMaker = new StringBuilder("from vector_database import TesseractOcrClient\n");
		callMaker.append(varName + "= TesseractOcrClient(");
		callMaker.append("poppler_path = " + PyUtils.determineStringType(POPPLER_PATH) + ", ");
		callMaker.append("tesseract_path = " + PyUtils.determineStringType(TESSERACT_PATH) + ")\n");
		
		callMaker.append(varName + ".ocr_document(");	
		if (filePath != null) {
			callMaker.append("file_path")
					 .append("=")
					 .append(PyUtils.determineStringType(filePath));
		} else {
			throw new IllegalArgumentException("No file path given");
		}
		
		if (preprocessing != null) {
			callMaker.append(", preprocessing")
					 .append("=")
					 .append(PyUtils.determineStringType(preprocessing));
		}
		
		callMaker.append(")");
		classLogger.debug("Running >>>" + callMaker.toString());
		
		Object output = pyt.runDirectPy(callMaker.toString());
		
		if (output == null) {
			return null;
		}
		
		return (ArrayList<HashMap<String, String>>) output;
	}

}
