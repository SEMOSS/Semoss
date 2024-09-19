package prerna.reactor.export;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.StandardProtectionPolicy;

import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;

public class EncryptPdfReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EncryptPdfReactor.class);
	
	private static final String ALLOW_PRINT = "allowPrint";
	private static final String ALLOW_COPY = "allowCopy";
	private static final String ALLOW_MODIFY = "allowModify";
	private static final String ALLOW_ASSEMBLE = "allowAssemble";
	private static final String READ_ONLY = "readOnly";

	public EncryptPdfReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.FILE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.PASSWORD.getKey(),
				ALLOW_PRINT, 
				ALLOW_COPY,
				ALLOW_MODIFY,
				ALLOW_ASSEMBLE,
				READ_ONLY
			};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if(password == null || (password=password.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define a password for the file");
		}
		boolean noPrint = !Boolean.parseBoolean(this.keyValue.get(ALLOW_PRINT)+"");
		boolean noCopy = !Boolean.parseBoolean(this.keyValue.get(ALLOW_COPY)+"");
		boolean noModify = !Boolean.parseBoolean(this.keyValue.get(ALLOW_MODIFY)+"");
		boolean noAssemble = !Boolean.parseBoolean(this.keyValue.get(ALLOW_ASSEMBLE)+"");
		boolean readOnly = Boolean.parseBoolean(this.keyValue.get(READ_ONLY)+"");
		
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File f = new File(filePath);

		String encryptedFilePath = FilenameUtils.removeExtension(filePath)+"-encrypted.pdf";
		
		PDDocument doc = null;
		try {
			doc = PDDocument.load(f);
			
			// Define the length of the encryption key.
			// Possible values are 40, 128 or 256.
			int keyLength = 256;

			AccessPermission ap = new AccessPermission();
			if(noPrint) {
				// printing
				ap.setCanPrint(false);
				ap.setCanPrintDegraded(false);
			}
			if(noCopy) {
				// copying
				ap.setCanExtractContent(false);
			}
			if(noModify) {
				// modify
				ap.setCanModify(false);
				// modify annotations
				ap.setCanModifyAnnotations(false);
			}
			if(noAssemble) {
				// can assemble
				ap.setCanAssembleDocument(false);
			}
			if(readOnly) {
				// set the pdf as read only
				ap.setReadOnly();
			}
			
			String randomOwner = UUID.randomUUID().toString();
			StandardProtectionPolicy spp = new StandardProtectionPolicy(randomOwner, password, ap);
			spp.setEncryptionKeyLength(keyLength);

			//Apply protection
			doc.protect(spp);

			doc.save(encryptedFilePath);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to encrypt the file. Error message = " + e.getMessage());
		} finally {
			if(doc != null) {
				try {
					doc.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(encryptedFilePath);

		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the csv file"));
		return retNoun;
	}

	
}
