package prerna.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.fileupload.FileItem;

public class FileAnalyzer {

	private static final List<Charset> COMMON_ENCODINGS = Arrays.asList(
			StandardCharsets.UTF_8,
			StandardCharsets.ISO_8859_1, // same as latin1
			Charset.forName("Windows-1252") // same as cp1252
			);

	private FileItem item;
	private Charset charset = null;

	public FileAnalyzer(FileItem item) {
		this.item = item;
	}

	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean isTextContent() throws IOException {
		for (Charset charset : COMMON_ENCODINGS) {
			try (InputStream is = item.getInputStream(); 
					InputStreamReader isr = new InputStreamReader(is, charset);
					BufferedReader reader = new BufferedReader(isr)) {
				char[] buffer = new char[4096];
				int charsRead = reader.read(buffer);
				if (charsRead == -1) {
					return false; // Empty file
				}
				String contentSnippet = new String(buffer, 0, charsRead);
				if (isLikelyText(contentSnippet)) {
					this.charset = charset;
					return true;
				}
			} catch (IOException e) {
				// Ignore and try the next encoding
			}
		}
		return false;
	}

	/**
	 * 
	 * @param contentSnippet
	 * @return
	 */
	private boolean isLikelyText(String contentSnippet) {
		// Check for non-text characters and common text patterns
		boolean hasNonTextCharacters = contentSnippet.chars().anyMatch(c ->
		!(Character.isWhitespace(c) || Character.isISOControl(c) || (c >= 32 && c <= 126) || (c >= 128 && c <= 255))
				);
		if (hasNonTextCharacters) {
			return false;
		}
		return contentSnippet.contains("\n") || contentSnippet.contains("\r") ||
				contentSnippet.contains(",") || contentSnippet.contains("\t");
	}

	/**
	 * 
	 * @return
	 */
	public Charset getCharset() {
		return charset;
	}
}
