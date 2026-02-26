package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;

public abstract class AbstractFileHandler implements IFileHandler {
	
	private static final Logger classLogger = LogManager.getLogger();

	public abstract boolean supportsFile(File file);

	public abstract IFileProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer);
	
	public int handleProcessing(File file, VectorDatabaseCSVWriter writer) throws Exception {

		    IFileProcessor subprocessor = getFileProcessor(file, writer);

		    if (subprocessor != null) {
		      subprocessor.process();
		    }

		    return writer.getRowsInCsv();
	  }
	
	protected String getMimeType(File file) {
		  String mimeType = null;

			// using tika for mime type check since it is more consistent across env + rhel
			// OS and macOS
			TikaConfig config = TikaConfig.getDefaultConfig();
			Detector detector = config.getDetector();
			Metadata metadata = new Metadata();
			metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, file.getName());
			try (TikaInputStream stream = TikaInputStream.get(new FileInputStream(file))) {
				mimeType = detector.detect(stream, metadata).toString();
			} catch (IOException e) {
				classLogger.error("Error determining mime type: ", e);
			}

			if (mimeType == null) {
				throw new NullPointerException("Unable to determine the mimType for file " + file.getName());
			}
			
			return mimeType;
	  }
	
}
