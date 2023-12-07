package prerna.io.connector.antivirus.tika;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import prerna.io.connector.antivirus.IVirusScanner;
import prerna.util.Constants;

public class ApacheTikaScannerUtils implements IVirusScanner {

	private static final Logger classLogger = LogManager.getLogger(ApacheTikaScannerUtils.class);

	public ApacheTikaScannerUtils() {

	}

	@Override
	public Map<String, Collection<String>> getViruses(String name, InputStream is) {
		Map<String, Collection<String>> retMap = new HashMap<>();

		Tika tika = new Tika();
		Metadata metadata = new Metadata();
		try {
			String detectedType = tika.detect(is, metadata);
			classLogger.info("Predicted " + name + " has type " + detectedType);
			if(isSubtypeOfMsDownload(detectedType)) {
				Collection<String> allIssues = new TreeSet<>();
				retMap.put(name, allIssues);
				allIssues.add(detectedType);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return retMap;
	}

	private static boolean isSubtypeOfMsDownload(String mimeType) {
		MediaType mediaType = MediaType.parse(mimeType);
		MediaType baseType = mediaType.getBaseType();
		return baseType.equals(MediaType.parse("application/x-msdownload"));
	}

}
