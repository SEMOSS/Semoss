package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.reactor.vector.OcrDocumentImagesReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class PDFProcessor extends AbstractFileProcessor {

	private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);
	private static final String PREPROCESSING_KEY = "preprocessing";

	public PDFProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		super(filePath, writer);
	}

	@Override
	public void process() throws IOException {
		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(new File(this.filePath)))) {
			String source = getSource(this.filePath);
			PDFTextStripper pdfStripper = new PDFTextStripper();
			for (int pageIndex = 1; pageIndex <= document.getNumberOfPages(); pageIndex++) {
				// stripper is 1 based
				
				// NOTES: this is going page by page and putting that page into a row of the csv
				pdfStripper.setStartPage(pageIndex);
				pdfStripper.setEndPage(pageIndex);
				String parsedText = pdfStripper.getText(document);
				this.writer.writeRow(source, pageIndex+"", parsedText);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	public void processWithImages(Insight insight) throws IOException {
		this.process();
		this.extractImages(insight);
	}
	
	@SuppressWarnings("unchecked")
	private void extractImages(Insight insight) {
		ArrayList<Map<String, String>> extractedText = null;
		String source = getSource(this.filePath);
		
		NounStore embeddingsNs = new NounStore(ReactorKeysEnum.ALL.getKey());
		GenRowStruct genrws = embeddingsNs.makeNoun("filePaths");
		genrws.add(filePath, PixelDataType.CONST_STRING);
		embeddingsNs.makeNoun(ReactorKeysEnum.FILE_PATH.getKey()).addLiteral(this.filePath);
		embeddingsNs.makeNoun(PREPROCESSING_KEY).addBoolean(false);;

		OcrDocumentImagesReactor ocrReactor = new OcrDocumentImagesReactor();
		ocrReactor.setInsight(insight);
		ocrReactor.setNounStore(embeddingsNs);
		ocrReactor.In();
		
		try {
			NounMetadata ocrResults = ocrReactor.execute();
			extractedText = (ArrayList<Map<String, String>>) ocrResults.getValue();
			System.out.println("OCR RESULTS: " + extractedText);
		} catch (Exception e) {
			classLogger.error("Error occured extracting text from images in document: " + this.filePath);
			e.printStackTrace();
		}
		
		try {
			for (Map<String, String> chunk : extractedText) {
				System.out.println("TEXT HEREEEEE: " + chunk.get("text"));
				this.writer.writeRow(source, chunk.get("page_number"), chunk.get("text"));
			}
		} catch (Exception e) {
			classLogger.error("Error occured saving extracted text from images for document: " + e.getMessage());
		}
	}

}
