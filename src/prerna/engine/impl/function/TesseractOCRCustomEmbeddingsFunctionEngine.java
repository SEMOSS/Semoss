/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.function;

import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import com.sun.jna.NativeLibrary;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.util.Utility;

public class TesseractOCRCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(TesseractOCRCustomEmbeddingsFunctionEngine.class);

	private static final String TESSDATA_PATH = "TESSDATA_PATH";
	private static final String NATIVE_LIB_PATH = "NATIVE_LIB_PATH";
	private static final String LANGUAGE = "LANGUAGE";
	private static final String DPI = "DPI";
	private static final String PAGE_SEGMENTATION_MODE = "PAGE_SEGMENTATION_MODE";

	private static final String DEFAULT_LANGUAGE = "eng";
	private static final int DEFAULT_DPI = 300;
	private static final int MIN_DPI = 72;
	private static final int MAX_DPI = 1200;
	private static final int DEFAULT_PSM = 3;  // fully automatic page segmentation, no OSD
	private static final int DEFAULT_OEM = 1;  // LSTM neural net engine
	private static final int MAX_PAGE_COUNT = 500;

	// Tesseract language codes: e.g. "eng", "fra", "deu+eng", "chi_sim+eng"
	private static final Pattern VALID_LANGUAGE_PATTERN = Pattern.compile("^[a-zA-Z_]+(\\+[a-zA-Z_]+)*$");

	private static final String PDF_MIME_TYPE = "application/pdf";
	private static final String TESSERACT_LIB_NAME = "tesseract";

	// tessdata locations to probe when not configured via SMSS, RDF_Map, or TESSDATA_PREFIX env var
	private static final String[] KNOWN_TESSDATA_PATHS = {
			"/usr/share/tesseract-ocr/5/tessdata",
			"/usr/share/tesseract-ocr/4.00/tessdata",
			"/usr/share/tessdata",
			"/opt/homebrew/share/tessdata",
			"/usr/local/share/tessdata"
	};

	private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES = Set.of(
			"image/png", "image/jpeg", "image/tiff", "image/bmp", "image/gif");

	private String tessdataPath;
	private String language;
	private int dpi;
	private int pageSegmentationMode;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these so the admin doesn't need to define them
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY,
				"Tesseract OCR Custom Embeddings - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY,
				"Execute local Tesseract OCR on scanned PDFs and images for vector database embeddings");

		super.open(smssProp);

		this.tessdataPath = this.smssProp.getProperty(TESSDATA_PATH);
		if (this.tessdataPath != null) {
			this.tessdataPath = this.tessdataPath.trim();
		}
		if (this.tessdataPath == null || this.tessdataPath.isEmpty()) {
			this.tessdataPath = resolveTessdataPath();
		}
		if (this.tessdataPath != null) {
			classLogger.info("Using tessdata path: {}", this.tessdataPath);
		} else {
			classLogger.warn("No tessdata path configured or detected. "
					+ "Tesseract will attempt to use its compiled-in default.");
		}

		// Native library path for platforms where Tess4J doesn't bundle libs (e.g. macOS ARM)
		String nativeLib = this.smssProp.getProperty(NATIVE_LIB_PATH);
		if (nativeLib != null && !(nativeLib = nativeLib.trim()).isEmpty()) {
			File nativeLibDir = new File(nativeLib);
			if (!nativeLibDir.isDirectory()) {
				throw new IllegalArgumentException("NATIVE_LIB_PATH does not exist or is not a directory: " + nativeLib);
			}
			addTesseractNativeLibSearchPath(nativeLib);
			classLogger.info("Added Tesseract native library search path: {}", nativeLib);
		}

		String lang = this.smssProp.getProperty(LANGUAGE);
		this.language = (lang != null && !(lang = lang.trim()).isEmpty()) ? lang : DEFAULT_LANGUAGE;
		if (!VALID_LANGUAGE_PATTERN.matcher(this.language).matches()) {
			throw new IllegalArgumentException(
					"Invalid LANGUAGE value '" + this.language + "'. "
					+ "Must be a valid Tesseract language code (e.g. eng, fra, deu+eng).");
		}

		this.dpi = parseIntProperty(DPI, DEFAULT_DPI);
		if (this.dpi < MIN_DPI || this.dpi > MAX_DPI) {
			classLogger.warn("DPI {} is outside safe range [{}-{}], clamping to default {}",
					this.dpi, MIN_DPI, MAX_DPI, DEFAULT_DPI);
			this.dpi = DEFAULT_DPI;
		}
		this.pageSegmentationMode = parseIntProperty(PAGE_SEGMENTATION_MODE, DEFAULT_PSM);
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException(
				"This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		String mimeType = detectMimeType(fileToProcess);
		if (mimeType == null) {
			return false;
		}

		// standalone image files are always processable
		if (SUPPORTED_IMAGE_MIME_TYPES.contains(mimeType.toLowerCase())) {
			return true;
		}

		// PDFs are only processable if they contain images (i.e. scanned PDFs)
		if (mimeType.equalsIgnoreCase(PDF_MIME_TYPE)) {
			try {
				return PDFUtility.pdfContainsImages(fileToProcess.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error("Failed to check if PDF contains images: {}", fileToProcess.getName(), e);
			}
		}

		return false;
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		String mimeType = detectMimeType(fileToProcess);
		if (mimeType == null) {
			throw new IllegalArgumentException("Unable to determine mime type for file " + fileToProcess.getName());
		}

		String source = fileToProcess.getName();
		try (VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath)) {
			if (mimeType.equalsIgnoreCase(PDF_MIME_TYPE)) {
				processPdf(fileToProcess, source, writer);
			} else if (SUPPORTED_IMAGE_MIME_TYPES.contains(mimeType.toLowerCase())) {
				processImage(fileToProcess, source, writer);
			} else {
				throw new IllegalArgumentException("Unsupported file type for Tesseract OCR: " + mimeType);
			}

			return writer.getRowsInCsv();
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to process document with Tesseract OCR: {}", source, e);
			throw new IllegalArgumentException("Tesseract OCR processing failed for " + source, e);
		}
	}

	private void processPdf(File pdfFile, String source, VectorDatabaseCSVWriter writer) throws IOException {
		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(pdfFile))) {
			PDFRenderer renderer = new PDFRenderer(document);
			int numPages = document.getNumberOfPages();
			if (numPages > MAX_PAGE_COUNT) {
				throw new IllegalArgumentException(
						"PDF " + source + " has " + numPages + " pages, exceeding the maximum of " + MAX_PAGE_COUNT);
			}
			classLogger.info("Starting Tesseract OCR on {} ({} pages, {} DPI)", source, numPages, this.dpi);

			for (int pageIndex = 0; pageIndex < numPages; pageIndex++) {
				long pageStart = System.currentTimeMillis();

				BufferedImage pageImage = renderer.renderImageWithDPI(pageIndex, this.dpi, ImageType.RGB);
				BufferedImage grayscaleImage = convertToGrayscale(pageImage);
				// allow the full-color image to be GC'd immediately
				pageImage = null;

				String ocrText = doOcr(grayscaleImage);

				long elapsed = System.currentTimeMillis() - pageStart;
				classLogger.info("OCR page {}/{} of {} completed in {}ms", pageIndex + 1, numPages, source, elapsed);

				if (ocrText != null && !ocrText.isBlank()) {
					writer.writeRow(source, String.valueOf(pageIndex + 1), ocrText);
				}
			}

			classLogger.info("Completed Tesseract OCR on {} - {} rows extracted", source, writer.getRowsInCsv());
		}
	}

	private void processImage(File imageFile, String source, VectorDatabaseCSVWriter writer) throws IOException {
		BufferedImage image = ImageIO.read(imageFile);
		if (image == null) {
			classLogger.warn("Unable to read image file: {}", source);
			return;
		}

		BufferedImage grayscaleImage = convertToGrayscale(image);
		image = null;

		String ocrText = doOcr(grayscaleImage);
		if (ocrText != null && !ocrText.isBlank()) {
			writer.writeRow(source, "1", ocrText);
		}
	}

	// new instance per call — Tess4J's Tesseract is not thread-safe but creation is cheap
	private String doOcr(BufferedImage image) {
		Tesseract tesseract = new Tesseract();
		if (this.tessdataPath != null && !this.tessdataPath.isEmpty()) {
			tesseract.setDatapath(this.tessdataPath);
		}
		tesseract.setLanguage(this.language);
		tesseract.setPageSegMode(this.pageSegmentationMode);
		tesseract.setOcrEngineMode(DEFAULT_OEM);

		try {
			return tesseract.doOCR(image);
		} catch (TesseractException e) {
			classLogger.error("Tesseract OCR failed: {}", e.getMessage(), e);
			return null;
		}
	}

	private BufferedImage convertToGrayscale(BufferedImage original) {
		ColorConvertOp op = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
		return op.filter(original, null);
	}

	// uses Tika for mime type detection — consistent with AbstractFileProcessor
	private String detectMimeType(File file) {
		TikaConfig config = TikaConfig.getDefaultConfig();
		Detector detector = config.getDetector();
		Metadata metadata = new Metadata();
		metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, file.getName());
		try (TikaInputStream stream = TikaInputStream.get(new FileInputStream(file))) {
			return detector.detect(stream, metadata).toString();
		} catch (IOException e) {
			classLogger.error("Failed to detect mime type for file: {}", file.getName(), e);
			return null;
		}
	}

	private int parseIntProperty(String propertyKey, int defaultValue) {
		String value = this.smssProp.getProperty(propertyKey);
		if (value != null && !(value = value.trim()).isEmpty()) {
			try {
				return Integer.parseInt(value);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid {} value '{}', using default {}", propertyKey, value, defaultValue);
			}
		}
		return defaultValue;
	}

	/**
	 * Registers a filesystem directory as a native library search path for the
	 * Tesseract shared library only. Uses JNA's per-library search path API
	 * ({@link NativeLibrary#addSearchPath}) instead of mutating the JVM-global
	 * {@code jna.library.path} system property.
	 *
	 * <p>This is only needed on platforms where Tess4J does not bundle native
	 * libraries (e.g. macOS ARM64). On Linux, {@code apt-get install tesseract-ocr}
	 * places libs in standard linker paths so this is unnecessary.</p>
	 *
	 * @param path absolute path to the directory containing {@code libtesseract.dylib} or
	 *             {@code libtesseract.so} (e.g. {@code /opt/homebrew/lib})
	 */
	private void addTesseractNativeLibSearchPath(String path) {
		NativeLibrary.addSearchPath(TESSERACT_LIB_NAME, path);
	}

	/**
	 * Resolves the tessdata directory containing Tesseract's {@code .traineddata}
	 * language model files. Called only when the SMSS does not define a
	 * {@code TESSDATA_PATH} property.
	 *
	 * <p>Resolution order (first valid directory wins):
	 * <ol>
	 *   <li>{@code TESSDATA_PATH} in {@code RDF_Map.prop} — server-wide admin config</li>
	 *   <li>{@code TESSDATA_PREFIX} environment variable — standard Tesseract env var</li>
	 *   <li>Well-known filesystem paths — Debian/Ubuntu, Fedora, macOS Homebrew</li>
	 * </ol>
	 *
	 * <p>This mirrors the {@code PYTHONHOME} resolution pattern in
	 * {@code PyUtils.java} where RDF_Map.prop is checked alongside env vars.</p>
	 *
	 * @return absolute path to the tessdata directory, or {@code null} if not found
	 */
	private String resolveTessdataPath() {
		// 1. RDF_Map.prop — server-wide admin config
		String path = Utility.getDIHelperProperty(TESSDATA_PATH);
		if (path != null && !(path = path.trim()).isEmpty() && new File(path).isDirectory()) {
			classLogger.info("Resolved tessdata from RDF_Map: {}", path);
			return path;
		}

		// 2. TESSDATA_PREFIX — standard Tesseract env var (set in Docker, systemd, etc.)
		path = System.getenv("TESSDATA_PREFIX");
		if (path != null && new File(path).isDirectory()) {
			classLogger.info("Resolved tessdata from TESSDATA_PREFIX env var: {}", path);
			return path;
		}

		// 3. probe well-known install locations
		for (String candidate : KNOWN_TESSDATA_PATHS) {
			if (new File(candidate).isDirectory()) {
				classLogger.info("Auto-detected tessdata at: {}", candidate);
				return candidate;
			}
		}

		return null;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.TESSERACT_OCR_CUSTOM_EMBEDDINGS.name();
	}

	@Override
	public void close() throws IOException {
		// Tesseract instances are created per-call, nothing to close
	}
}
