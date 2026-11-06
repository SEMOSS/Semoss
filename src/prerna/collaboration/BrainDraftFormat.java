package prerna.collaboration;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities;
import org.jsoup.nodes.Node;
import org.jsoup.safety.Safelist;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.util.Utility;

/** How drafts written for the owner look in Outlook: their font, single spaced lines, and their signature. */
public final class BrainDraftFormat {

	private static final Logger classLogger = LogManager.getLogger(BrainDraftFormat.class);

	public static final String DEFAULT_FONT = "Aptos";
	public static final int DEFAULT_SIZE = 12;
	static final int MIN_SIZE = 8;
	static final int MAX_SIZE = 24;
	static final int MAX_SIGNATURE = 20000;
	private static final int SENT_TO_READ = 15;
	private static final int WINDOW_DAYS = 90;
	private static final Pattern FONT_NAME = Pattern.compile("[A-Za-z0-9 _-]{1,64}");
	private static final Pattern FAMILY = Pattern.compile("font-family\\s*:\\s*([^;]+)", Pattern.CASE_INSENSITIVE);
	private static final Pattern SIZE = Pattern.compile("font-size\\s*:\\s*([0-9.]+)\\s*(pt|px)",
			Pattern.CASE_INSENSITIVE);
	// where Outlook and Gmail put the signature they insert; mobile "Get Outlook" lines are not one
	private static final Pattern FORWARD = Pattern.compile("^\\s*(fw|fwd)\\s*:", Pattern.CASE_INSENSITIVE);
	private static final String SIGNATURE = "#Signature, [id^=x_Signature], div.gmail_signature";

	private BrainDraftFormat() {
	}

	/** Font and signature to draft with. */
	public record Format(String font, int size, String signatureHtml) {
	}

	/**
	 * The Collaboration owner's draft look; null when Collaboration is off, the user has no Brain settings or they
	 * cannot be read, so the mail connectors keep their own formatting for everyone else.
	 */
	public static Format forOwner(User user) {
		try {
			if (user == null || !Utility.isCollaborationDatabaseEnabled() || !CollaborationDbUtils.isInitalized()) {
				return null;
			}
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			Format format = CollaborationDbUtils.queryOne("SELECT DRAFT_FONT, DRAFT_FONT_SIZE, DRAFT_SIGNATURE_HTML, "
					+ "DRAFT_SIGNATURE_ON FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
						String font = CollaborationDbUtils.getString(rs, "DRAFT_FONT");
						Integer size = CollaborationDbUtils.getInteger(rs, "DRAFT_FONT_SIZE");
						boolean on = !Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "DRAFT_SIGNATURE_ON"));
						String signature = CollaborationDbUtils.getString(rs, "DRAFT_SIGNATURE_HTML");
						return new Format(font == null || font.isBlank() ? DEFAULT_FONT : font,
								size == null ? DEFAULT_SIZE : size, on && signature != null && !signature.isBlank()
										? signature : null);
					}, owner.getValue0(), owner.getValue1());
			return format;
		} catch (Exception e) {
			// a draft is still written, as the connector writes it
			classLogger.warn("Could not read the draft format settings: {}", e.getMessage());
			return null;
		}
	}

	/** Collaboration's own drafts: the owner's look, or the default one before they have Brain settings. */
	public static String html(User user, String text) {
		Format format = forOwner(user);
		return html(format == null ? new Format(DEFAULT_FONT, DEFAULT_SIZE, null) : format, text);
	}

	/** A draft body, plain or editor HTML, as HTML in the given look. */
	public static String body(Format format, String body, boolean html) {
		return html ? htmlBody(format, body) : html(format, body);
	}

	/** Plain text as Outlook-style HTML: one div per line (no paragraph gaps), the owner's font, their signature. */
	public static String html(Format format, String text) {
		if (format.signatureHtml() != null) {
			text = stripTyped(text, signatureKeys(format.signatureHtml()));
		}
		List<String> lines = new ArrayList<>(List.of((text == null ? "" : text).replace("\r\n", "\n").split("\n", -1)));
		while (!lines.isEmpty() && lines.get(lines.size() - 1).isBlank()) {
			lines.remove(lines.size() - 1);
		}
		StringBuilder html = new StringBuilder("<div style=\"font-family: ").append(cssFamily(format.font()))
				.append("; font-size: ").append(format.size()).append("pt;\">");
		for (String line : lines) {
			html.append(line.isBlank() ? "<div><br></div>" : "<div>" + Entities.escape(line) + "</div>");
		}
		if (format.signatureHtml() != null) {
			html.append(lines.isEmpty() ? "" : "<div><br></div>").append("<div id=\"Signature\">")
					.append(format.signatureHtml()).append("</div>");
		}
		return html.append("</div>").toString();
	}

	/**
	 * HTML from the editor in the owner's look: no paragraph gaps (Outlook's own mail has none), their font, and their
	 * signature unless it is already there.
	 */
	public static String htmlBody(Format format, String html) {
		Document doc = Jsoup.parseBodyFragment(html == null ? "" : html);
		doc.outputSettings().prettyPrint(false);
		if (format.signatureHtml() != null && doc.selectFirst("#Signature") == null) {
			stripTyped(doc, signatureKeys(format.signatureHtml()));
		}
		for (Element block : doc.body().select("p")) {
			if (!block.attr("style").contains("margin")) {
				block.attr("style", ("margin: 0; " + block.attr("style")).strip());
			}
		}
		StringBuilder out = new StringBuilder("<div style=\"font-family: ").append(cssFamily(format.font()))
				.append("; font-size: ").append(format.size()).append("pt;\">").append(doc.body().html());
		if (format.signatureHtml() != null && doc.selectFirst("#Signature") == null) {
			out.append("<div><br></div><div id=\"Signature\">").append(format.signatureHtml()).append("</div>");
		}
		return out.append("</div>").toString();
	}

	/** The text without a signature typed at its end, when the owner's saved one is added anyway. */
	public static String withoutTypedSignature(User user, String text) {
		Format format = forOwner(user);
		return format == null || format.signatureHtml() == null ? text : stripTyped(text, signatureKeys(format.signatureHtml()));
	}

	// the signature's lines, letters and digits only, so "571-363-9720" matches "571 363 9720"
	static Set<String> signatureKeys(String html) {
		Document doc = Jsoup.parseBodyFragment(html);
		doc.select("br").after("\n");
		doc.select("p, div, tr, li").after("\n");
		Set<String> keys = new HashSet<>();
		for (String line : doc.body().wholeText().split("\n")) {
			String key = key(line);
			if (!key.isEmpty()) {
				keys.add(key);
			}
		}
		return keys;
	}

	private static String key(String line) {
		return line.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N}]", "");
	}

	// two or more trailing lines that repeat the signature are a typed copy of it
	static String stripTyped(String text, Set<String> keys) {
		if (text == null) {
			return null;
		}
		List<String> lines = new ArrayList<>(List.of(text.replace("\r\n", "\n").split("\n", -1)));
		int end = lines.size();
		int matched = 0;
		for (int i = lines.size() - 1; i >= 0; i--) {
			String k = key(lines.get(i));
			if (k.isEmpty()) {
				continue;
			}
			if (!keys.contains(k)) {
				break;
			}
			matched++;
			end = i;
		}
		if (matched < 2) {
			return text;
		}
		return String.join("\n", lines.subList(0, end)).stripTrailing();
	}

	private static void stripTyped(Document doc, Set<String> keys) {
		List<Element> blocks = doc.body().children();
		List<Element> typed = new ArrayList<>();
		int matched = 0;
		for (int i = blocks.size() - 1; i >= 0; i--) {
			Element block = blocks.get(i);
			String k = key(block.text());
			if (k.isEmpty()) {
				typed.add(block);
				continue;
			}
			if (!keys.contains(k)) {
				break;
			}
			matched++;
			typed.add(block);
		}
		if (matched >= 2) {
			// with the blank lines between it and the body; the signature brings its own gap
			typed.forEach(Element::remove);
		}
	}

	private static String cssFamily(String font) {
		String name = FONT_NAME.matcher(font).matches() ? font : DEFAULT_FONT;
		// Outlook names Aptos by its cloud font aliases too
		return DEFAULT_FONT.equals(name) ? "Aptos, Aptos_EmbeddedFont, Aptos_MSFontService, Calibri, Helvetica, sans-serif"
				: "'" + name + "', Calibri, Helvetica, sans-serif";
	}

	/** The owner's name and address, so a draft shows who it is from before it is sent; null when unknown. */
	public static String[] sender(User user) {
		AccessToken token = user == null ? null : user.getAccessToken(AuthProvider.MICROSOFT);
		if (token == null || token.getEmail() == null || token.getEmail().isBlank()) {
			return null;
		}
		return new String[] { token.getEmail(), token.getName() };
	}

	static boolean validFont(String font) {
		return font != null && FONT_NAME.matcher(font).matches();
	}

	/** Their own signature HTML, made safe to put back in a draft; null when nothing is left. */
	static String clean(String html) {
		if (html == null || html.isBlank()) {
			return null;
		}
		Safelist safe = Safelist.relaxed().addAttributes(":all", "style").addProtocols("img", "src", "data");
		Document doc = Jsoup.parseBodyFragment(Jsoup.clean(html, safe));
		// inline cid images do not travel with a copied signature
		doc.select("img:not([src])").remove();
		// Outlook's own stylesheet zeroes paragraph margins; inline it so lines do not spread apart
		for (Element block : doc.select("p, div")) {
			if (!block.attr("style").contains("margin")) {
				block.attr("style", ("margin: 0; " + block.attr("style")).strip());
			}
		}
		doc.outputSettings().prettyPrint(false);
		String out = doc.body().html().strip();
		if (out.isEmpty() || out.length() > MAX_SIGNATURE) {
			return null;
		}
		return out;
	}

	// ---- learning from sent mail ----

	/** What sent mail shows: the signature most used and the body font most used; empty fields when not found. */
	public static Map<String, Object> learn(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<String> sentFolders = BrainMailFolders.load(owner.getValue0(), owner.getValue1()).byId().values().stream()
				.filter(folder -> BrainMailFolders.SENT.equals(folder.kind())).map(BrainMailFolders.Folder::id)
				.toList();
		List<Object> params = new ArrayList<>(List.of(owner.getValue0(), owner.getValue1(),
				Timestamp.from(Instant.now().minus(WINDOW_DAYS, ChronoUnit.DAYS)), "%sentitems"));
		params.addAll(sentFolders);
		String inSent = sentFolders.isEmpty() ? ""
				: " OR FOLDER IN (" + String.join(",", Collections.nCopies(sentFolders.size(), "?")) + ")";
		List<String> ids = CollaborationDbUtils.query(
				CollaborationDbUtils.page("SELECT GRAPH_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND RECEIVED_AT >= ? AND (FOLDER LIKE ?" + inSent + ") ORDER BY RECEIVED_AT DESC",
						SENT_TO_READ, 0),
				rs -> rs.getString("GRAPH_ID"), params.toArray());
		Map<String, Integer> signatures = new LinkedHashMap<>();
		Map<String, String> signatureHtml = new HashMap<>();
		Map<String, Integer> fonts = new LinkedHashMap<>();
		Map<Integer, Integer> sizes = new LinkedHashMap<>();
		List<String> names = nameParts(user);
		int read = 0;
		for (BrainMessageSource.Fetched fetched : BrainMessageSource.current().fetchAll(user, "email", null, ids)) {
			Map<String, Object> message = fetched.message();
			// a forward carries someone else's signature; a routine's styled result mailed to the owner carries none
			if (message == null || FORWARD.matcher(String.valueOf(message.get("subject"))).find()
					|| BrainStyleLearning.toThemselves(message)) {
				continue;
			}
			// only what the owner wrote, never the quoted mail below it
			String html = htmlOf(message, "uniqueBody");
			if (html == null) {
				html = htmlOf(message, "body");
			}
			if (html == null) {
				continue;
			}
			read++;
			Document doc = Jsoup.parse(html);
			Element signature = doc.selectFirst(SIGNATURE);
			// theirs only: it has to carry their own name
			String found = signature != null && mentions(signature.text(), names) ? signature.html()
					: byName(doc, names);
			String cleaned = clean(found);
			if (cleaned != null) {
				String key = Jsoup.parseBodyFragment(cleaned).text().strip();
				signatures.merge(key, 1, Integer::sum);
				// newest wins for the same text
				signatureHtml.putIfAbsent(key, cleaned);
			}
			if (signature != null) {
				signature.remove();
			}
			// the first styled line they typed, before any quoted history
			for (Element styled : doc.select("[style*=font-family]")) {
				Matcher family = FAMILY.matcher(styled.attr("style"));
				if (styled.text().isBlank() || !family.find()) {
					continue;
				}
				String font = firstFamily(family.group(1));
				if (font != null) {
					fonts.merge(font, 1, Integer::sum);
				}
				Matcher size = SIZE.matcher(styled.attr("style"));
				if (size.find()) {
					double value = Double.parseDouble(size.group(1));
					int pt = (int) Math.round("px".equalsIgnoreCase(size.group(2)) ? value * 0.75 : value);
					if (pt >= MIN_SIZE && pt <= MAX_SIZE) {
						sizes.merge(pt, 1, Integer::sum);
					}
				}
				break;
			}
		}
		Map<String, Object> out = new LinkedHashMap<>();
		String signatureKey = most(signatures);
		out.put("signatureHtml", signatureKey == null ? "" : signatureHtml.get(signatureKey));
		String font = most(fonts);
		out.put("font", font == null ? "" : font);
		Integer size = most(sizes);
		out.put("fontSize", size);
		out.put("read", read);
		return out;
	}

	/** Copies what sent mail shows into the settings; with onlyMissing, keeps anything already set. */
	public static Map<String, Object> learnInto(User user, boolean onlyMissing) {
		Map<String, Object> learned = learn(user);
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		BrainProfileUtils.getSettings(owner.getValue0(), owner.getValue1());
		String signature = (String) learned.get("signatureHtml");
		String font = (String) learned.get("font");
		Integer size = (Integer) learned.get("fontSize");
		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		if (!signature.isEmpty()) {
			sets.add(onlyMissing ? "DRAFT_SIGNATURE_HTML = COALESCE(DRAFT_SIGNATURE_HTML, ?)" : "DRAFT_SIGNATURE_HTML = ?");
			params.add(signature);
		}
		if (validFont(font)) {
			sets.add(onlyMissing ? "DRAFT_FONT = COALESCE(DRAFT_FONT, ?)" : "DRAFT_FONT = ?");
			params.add(font);
		}
		if (size != null) {
			sets.add(onlyMissing ? "DRAFT_FONT_SIZE = COALESCE(DRAFT_FONT_SIZE, ?)" : "DRAFT_FONT_SIZE = ?");
			params.add(size);
		}
		if (!sets.isEmpty()) {
			// a version bump, so an open Settings page reloads before saving over it
			sets.add("VERSION = VERSION + 1");
			sets.add("UPDATED_AT = ?");
			params.add(CollaborationDbUtils.now());
			params.add(owner.getValue0());
			params.add(owner.getValue1());
			CollaborationDbUtils.update("UPDATE BRAIN_SETTINGS SET " + String.join(", ", sets)
					+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ?", params.toArray());
		}
		return learned;
	}

	// first and last name from the Microsoft account, "Last, First M" or "First Last"
	private static List<String> nameParts(User user) {
		AccessToken token = user == null ? null : user.getAccessToken(AuthProvider.MICROSOFT);
		String name = token == null ? null : token.getName();
		List<String> parts = new ArrayList<>();
		for (String part : (name == null ? "" : name).split("[^\\p{L}]+")) {
			if (part.length() >= 2) {
				parts.add(part.toLowerCase(Locale.ROOT));
			}
		}
		return parts;
	}

	/**
	 * A signature Outlook did not mark: the last short line with their whole name, and what follows it.
	 * The sign-off ("Kunal") is first name only, so it is never taken for the signature.
	 */
	private static String byName(Document doc, List<String> names) {
		if (names.size() < 2) {
			return null;
		}
		Element nameLine = null;
		for (Element block : doc.body().select("p, div, td, span, b, strong")) {
			String text = block.ownText().strip();
			if (!text.isEmpty() && text.length() <= 60 && names.stream().allMatch(text.toLowerCase(Locale.ROOT)::contains)) {
				nameLine = block;
			}
		}
		if (nameLine == null) {
			return null;
		}
		// keep the formatting wrapped around the name (bold, size)
		while (nameLine.parent() != null && nameLine.parent().is("span, font, b, strong, i, em")) {
			nameLine = nameLine.parent();
		}
		// a signature laid out in a table is the whole table
		Element start = nameLine.closest("table") != null ? nameLine.closest("table") : nameLine;
		// the name line and everything after it, in reading order, up to the end of what they wrote
		StringBuilder html = new StringBuilder();
		for (Node at = start; at != null && at != doc.body(); at = at.parent()) {
			for (Node next = at == start ? start : at.nextSibling(); next != null; next = next.nextSibling()) {
				html.append(next.outerHtml());
			}
		}
		String out = html.toString();
		// a long block is a quoted mail, not a signature
		return Jsoup.parseBodyFragment(out).text().length() > 400 ? null : out;
	}

	private static boolean mentions(String text, List<String> names) {
		String lower = text.toLowerCase(Locale.ROOT);
		return !names.isEmpty() && names.stream().anyMatch(lower::contains);
	}

	@SuppressWarnings("unchecked")
	private static String htmlOf(Map<String, Object> message, String field) {
		if (message == null || !(message.get(field) instanceof Map<?, ?> body)) {
			return null;
		}
		Map<String, Object> content = (Map<String, Object>) body;
		return "html".equalsIgnoreCase(String.valueOf(content.get("contentType")))
				&& content.get("content") instanceof String html ? html : null;
	}

	private static String firstFamily(String families) {
		for (String family : families.split(",")) {
			String name = family.replace("\"", "").replace("'", "").replace("&quot;", "").strip();
			// Outlook's Aptos aliases and generic families are not a choice
			if (name.startsWith("Aptos")) {
				return DEFAULT_FONT;
			}
			if (validFont(name) && !List.of("sans-serif", "serif", "monospace", "system-ui").contains(name)) {
				return name;
			}
		}
		return null;
	}

	private static <K> K most(Map<K, Integer> counts) {
		K best = null;
		int top = 0;
		for (Map.Entry<K, Integer> entry : counts.entrySet()) {
			if (entry.getValue() > top) {
				best = entry.getKey();
				top = entry.getValue();
			}
		}
		return best;
	}
}
