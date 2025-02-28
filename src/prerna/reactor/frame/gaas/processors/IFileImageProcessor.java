package prerna.reactor.frame.gaas.processors;

import java.util.Map;

public interface IFileImageProcessor extends IFileProcessor {

	/*
	 * Get the image map for the ID to the base64 encoding of the image
	 */
	Map<String, String> getImageMap();
}
