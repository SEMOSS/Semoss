package prerna.reactor.agent.mcp.tools;

import prerna.util.files.SemossParsedFile;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Searches extracted room files and returns matches with surrounding context
 * lines.
 */
public class SearchRoomFilesWithContextReactor extends AbstractReactor {

	private static final int DEFAULT_CONTEXT_LINES = 2;
	private static final int DEFAULT_CONTEXT_WORDS = 15;
	private static final int DEFAULT_MAX_MATCHES = 25;
	private static final double DEFAULT_FUZZY_THRESHOLD = 0.88;
	private static final Pattern WORD_PATTERN = Pattern.compile("\\S+");

	public SearchRoomFilesWithContextReactor() {
		this.keysToGet = new String[] { "searchTerm", "contextLines", "contextWords", "maxMatches", "caseSensitive",
				"fuzzy", "fuzzyThreshold" };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String searchTerm = this.keyValue.get("searchTerm");
		if (searchTerm == null || searchTerm.trim().isEmpty()) {
			throw new IllegalArgumentException("searchTerm is required");
		}

		int contextLines = getOptionalInt("contextLines", DEFAULT_CONTEXT_LINES);
		int contextWords = getOptionalInt("contextWords", -1);
		if (contextWords <= 0) {
			contextWords = Math.max(DEFAULT_CONTEXT_WORDS, contextLines);
		}
		int maxMatches = getOptionalInt("maxMatches", DEFAULT_MAX_MATCHES);
		boolean caseSensitive = getOptionalBoolean("caseSensitive", false);
		boolean fuzzy = getOptionalBoolean("fuzzy", false);
		double fuzzyThreshold = getOptionalDouble("fuzzyThreshold", DEFAULT_FUZZY_THRESHOLD);

		String termForMatch = caseSensitive ? searchTerm : searchTerm.toLowerCase(Locale.ROOT);
		JaroWinklerSimilarity similarity = new JaroWinklerSimilarity();
		int termWordCount = countWords(termForMatch);

		File roomFolder = new File(insight.getInsightFolder());
		File[] files = roomFolder.listFiles();
		if (files == null) {
			return new NounMetadata(new ArrayList<>(), PixelDataType.MAP);
		}

		List<Map<String, Object>> results = new ArrayList<>();
		int matchCount = 0;

		for (File file : files) {
			if (!file.isFile()) {
				continue;
			}

			String extractedContent;
			String extractedPath;
			try {
				SemossParsedFile SemossParsedFile = new SemossParsedFile(file);
				extractedContent = SemossParsedFile.getExtractedContents();
				extractedPath = SemossParsedFile.getExtractedContentsFilePath();
			} catch (IOException e) {
				continue;
			}

			if (extractedContent == null) {
				continue;
			}

			String normalized = normalizeWhitespace(extractedContent);
			if (normalized.isEmpty()) {
				continue;
			}

			String contentForMatch = caseSensitive ? normalized : normalized.toLowerCase(Locale.ROOT);
			List<TokenSpan> tokens = tokenize(normalized);
			if (tokens.isEmpty()) {
				continue;
			}

			if (fuzzy) {
				if (termWordCount <= 1) {
					for (int i = 0; i < tokens.size(); i++) {
						String tokenText = tokens.get(i).text;
						String tokenMatch = caseSensitive ? tokenText : tokenText.toLowerCase(Locale.ROOT);
						double score = similarity.apply(termForMatch, tokenMatch);
						if (score >= fuzzyThreshold) {
							addMatch(results, file.getName(), i, contextWords, tokens, score);
							matchCount++;
							if (matchCount >= maxMatches) {
								return new NounMetadata(results, PixelDataType.MAP);
							}
						}
					}
				} else {
					for (int i = 0; i <= tokens.size() - termWordCount; i++) {
						String window = joinTokens(tokens, i, i + termWordCount - 1);
						String windowMatch = caseSensitive ? window : window.toLowerCase(Locale.ROOT);
						double score = similarity.apply(termForMatch, windowMatch);
						if (score >= fuzzyThreshold) {
							addMatch(results, file.getName(), i, contextWords, tokens, score);
							matchCount++;
							if (matchCount >= maxMatches) {
								return new NounMetadata(results, PixelDataType.MAP);
							}
						}
					}
				}
			} else {
				int fromIndex = 0;
				while (fromIndex < contentForMatch.length()) {
					int idx = contentForMatch.indexOf(termForMatch, fromIndex);
					if (idx < 0) {
						break;
					}
					int tokenIndex = findTokenIndex(tokens, idx);
					if (tokenIndex >= 0) {
						addMatch(results, file.getName(), tokenIndex, contextWords, tokens, null);
						matchCount++;
						if (matchCount >= maxMatches) {
							return new NounMetadata(results, PixelDataType.MAP);
						}
					}
					fromIndex = idx + termForMatch.length();
				}
			}
		}

		return new NounMetadata(results, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Searches extracted room files and returns matching word snippets with surrounding" + " context.";
	}

	private int getOptionalInt(String key, int defaultValue) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object value = grs.get(0);
			if (value instanceof Number) {
				return ((Number) value).intValue();
			}
			return Integer.parseInt(value.toString());
		}
		if (this.keyValue.containsKey(key)) {
			return Integer.parseInt(this.keyValue.get(key));
		}
		return defaultValue;
	}

	private double getOptionalDouble(String key, double defaultValue) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object value = grs.get(0);
			if (value instanceof Number) {
				return ((Number) value).doubleValue();
			}
			return Double.parseDouble(value.toString());
		}
		if (this.keyValue.containsKey(key)) {
			return Double.parseDouble(this.keyValue.get(key));
		}
		return defaultValue;
	}

	private boolean getOptionalBoolean(String key, boolean defaultValue) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object value = grs.get(0);
			if (value instanceof Boolean) {
				return (Boolean) value;
			}
			return Boolean.parseBoolean(value.toString());
		}
		if (this.keyValue.containsKey(key)) {
			return Boolean.parseBoolean(this.keyValue.get(key));
		}
		return defaultValue;
	}

	private String normalizeWhitespace(String content) {
		return content == null ? "" : content.replaceAll("\\s+", " ").trim();
	}

	private List<TokenSpan> tokenize(String content) {
		List<TokenSpan> tokens = new ArrayList<>();
		Matcher matcher = WORD_PATTERN.matcher(content);
		while (matcher.find()) {
			tokens.add(new TokenSpan(matcher.start(), matcher.end(), matcher.group()));
		}
		return tokens;
	}

	private int countWords(String text) {
		if (text == null || text.trim().isEmpty()) {
			return 0;
		}
		Matcher matcher = WORD_PATTERN.matcher(text.trim());
		int count = 0;
		while (matcher.find()) {
			count++;
		}
		return count;
	}

	private int findTokenIndex(List<TokenSpan> tokens, int charIndex) {
		for (int i = 0; i < tokens.size(); i++) {
			TokenSpan token = tokens.get(i);
			if (charIndex >= token.start && charIndex < token.end) {
				return i;
			}
		}
		return -1;
	}

	private String joinTokens(List<TokenSpan> tokens, int start, int end) {
		StringBuilder builder = new StringBuilder();
		for (int i = start; i <= end && i < tokens.size(); i++) {
			if (i > start) {
				builder.append(" ");
			}
			builder.append(tokens.get(i).text);
		}
		return builder.toString();
	}

	private void addMatch(List<Map<String, Object>> results, String fileName, int tokenIndex, int contextWords,
			List<TokenSpan> tokens, Double similarityScore) {
		int start = Math.max(0, tokenIndex - contextWords);
		int end = Math.min(tokens.size() - 1, tokenIndex + contextWords);
		String snippet = joinTokens(tokens, start, end);

		Map<String, Object> match = new HashMap<>();
		match.put("fileName", fileName);
		match.put("wordIndex", tokenIndex + 1);
		match.put("snippet", snippet);
		if (similarityScore != null) {
			match.put("similarity", similarityScore);
		}
		results.add(match);
	}

	private static class TokenSpan {
		private final int start;
		private final int end;
		private final String text;

		private TokenSpan(int start, int end, String text) {
			this.start = start;
			this.end = end;
			this.text = text;
		}
	}
}