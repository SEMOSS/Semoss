package prerna.reactor.export;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;

public class PdfToImageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PdfToImageReactor.class);
	private static final String CLASS_NAME = PdfToImageReactor.class.getName();

	public PdfToImageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}

		// get base asset folder
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if (filePath != null && filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false).replace("\\", "/");

		File pdfFilePath = null;
		if (filePath != null) {
			pdfFilePath = new File(assetFolder + "/" + filePath);
		} else {
			pdfFilePath = new File(assetFolder);
		}

		List<String> imagesCreated = new ArrayList<>();
		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(pdfFilePath))) {
			PDFRenderer pdfRenderer = new PDFRenderer(document);

			int pageCount = document.getNumberOfPages();
			for (int page = 0; page < pageCount; page++) {
				logger.info("Converting page {} of {} to png", page + 1, pageCount);
				// Render page to image at 300 DPI
				BufferedImage image = pdfRenderer.renderImageWithDPI(page, 300);

				// Create output filename: <pdfPath>_page_<num>.png
				String outputFileName = pdfFilePath.getAbsolutePath() + "_page_" + (page + 1) + ".png";
				imagesCreated.add(outputFileName);
				ImageIO.write(image, "png", new File(outputFileName));
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		NounMetadata retNoun = new NounMetadata(imagesCreated, PixelDataType.CONST_STRING);
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return """
				Generate a separate image for each page in a pdf file.
				The names of the files generated will be the pdfFileName + "_page_" + pageNumber + ".png"
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path of the PDF file";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		}
		return super.getDescriptionForKey(key);
	}

}
