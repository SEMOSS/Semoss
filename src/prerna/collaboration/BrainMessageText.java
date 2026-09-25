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
package prerna.collaboration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

// GATE-03 v1: the subject and new body of one email, before classification.
// UNVALIDATED: fixed patterns tested only on the brain-mail-v1 fixture, not on real mail yet (TW-Q-SIG-001).
// This is the one place ingest gets clean text from; swap the inside for a model without touching callers.
public final class BrainMessageText {

	public static final String VERSION = "patterns-v1";

	// Outlook desktop and web, new Outlook, and Gmail mark quoted history and signatures in the HTML
	private static final String QUOTE_MARKERS = "#appendonsend, #divRplyFwdMsg, [id^=x_divRplyFwdMsg], "
			+ "#mail-editor-reference-message-container, div.gmail_quote, blockquote";
	private static final String SIGNATURE_MARKERS = "#Signature, [id^=x_Signature], div.gmail_signature";

	private static final Pattern SUBJECT_PREFIX = Pattern.compile("^\\s*(\\[(external|ext)\\]|(re|fw|fwd|aw|wg)\\s*:)\\s*",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern FORWARD_SUBJECT = Pattern.compile("^\\s*(\\[(external|ext)\\]\\s*)?(fw|fwd)\\s*:",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern BANNER = Pattern.compile("^\\s*(\\[(external|ext)( email)?\\]|caution:|external email:)",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern WROTE = Pattern.compile("^\\s*On .{5,250} wrote:\\s*$", Pattern.CASE_INSENSITIVE);
	private static final Pattern ORIGINAL = Pattern.compile("^\\s*-{2,}\\s*(original message|forwarded message)\\s*-{2,}\\s*$",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern HEADER_FROM = Pattern.compile("^\\s*\\*?from:\\*?\\s+\\S.*$", Pattern.CASE_INSENSITIVE);
	private static final Pattern HEADER_NEXT = Pattern.compile("^\\s*\\*?(sent|date|to|subject|cc):\\*?\\s.*$",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern RULER = Pattern.compile("^\\s*_{10,}\\s*$");
	private static final Pattern SIG_DELIMITER = Pattern.compile("^-- ?$");
	private static final Pattern MOBILE = Pattern.compile("^\\s*(sent from my \\w+|get outlook for \\w+|sent from outlook"
			+ "( for \\w+)?)\\b.*$", Pattern.CASE_INSENSITIVE);
	private static final Pattern DISCLAIMER = Pattern.compile("^\\s*(confidentiality notice|disclaimer|this (e-?mail|message)"
			+ " (and any attachments )?(is|are) (intended|for|confidential)|the information (contained )?in this (e-?mail|message))",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern UNSUBSCRIBE = Pattern.compile("(unsubscribe|email preferences|update your preferences)",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern SIGN_OFF = Pattern.compile("^\\s*(thanks|thank you|many thanks|thx|best|best regards|"
			+ "regards|kind regards|warm regards|cheers|sincerely|talk soon)[,.!]?\\s*$", Pattern.CASE_INSENSITIVE);

	// a sign-off counts only near the end, with a short name-and-title block under it
	private static final int SIGN_OFF_MAX_LINES = 6;
	private static final int SIGN_OFF_MAX_CHARS = 80;

	private BrainMessageText() {

	}

	// contentType is "html" or "text", as Graph returns it; uniqueBody wins when present
	public static Map<String, Object> extract(String subject, String uniqueBody, String uniqueType, String body,
			String bodyType) {
		boolean useUnique = uniqueBody != null && !uniqueBody.isBlank();
		String content = useUnique ? uniqueBody : body;
		String type = useUnique ? uniqueType : bodyType;
		boolean forward = subject != null && FORWARD_SUBJECT.matcher(subject).find();
		List<Map<String, Object>> cuts = new ArrayList<>();

		String text = "html".equalsIgnoreCase(type) ? htmlToText(content, forward, cuts) : content;
		text = cutText(text == null ? "" : text.replace("\r\n", "\n"), forward, cuts);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("subject", cleanSubject(subject));
		result.put("body", tidy(text));
		result.put("source", useUnique ? "uniqueBody" : "body");
		result.put("version", VERSION);
		result.put("cuts", cuts);
		return result;
	}

	static String cleanSubject(String subject) {
		if (subject == null) {
			return null;
		}
		String s = subject;
		String before;
		do {
			before = s;
			s = SUBJECT_PREFIX.matcher(s).replaceFirst("");
		} while (!s.equals(before));
		return s.trim();
	}

	// ---- html: cut by the markers mail clients write, then flatten to lines ----

	private static String htmlToText(String html, boolean forward, List<Map<String, Object>> cuts) {
		Document doc = Jsoup.parse(html == null ? "" : html);
		for (Element signature : doc.select(SIGNATURE_MARKERS)) {
			cut(cuts, "html-signature", signature.text());
			signature.remove();
		}
		// a forward keeps what was forwarded; only its header block is dropped, by the text rules below
		if (!forward) {
			Element quote = doc.selectFirst(QUOTE_MARKERS);
			if (quote != null) {
				StringBuilder removed = new StringBuilder();
				removeFrom(quote, doc.body(), removed);
				cut(cuts, "html-quote", removed.toString());
			}
		}
		StringBuilder out = new StringBuilder();
		appendText(doc.body(), out);
		return out.toString();
	}

	// removes the node and everything after it in the document: its later siblings, then each ancestor's
	private static void removeFrom(Node node, Element root, StringBuilder removed) {
		Element parent = node.parent() instanceof Element e ? e : null;
		removeSiblingsFrom(node, removed);
		for (Element a = parent; a != null && a != root; a = a.parent()) {
			if (a.nextSibling() != null) {
				removeSiblingsFrom(a.nextSibling(), removed);
			}
		}
	}

	private static void removeSiblingsFrom(Node first, StringBuilder removed) {
		for (Node n = first; n != null;) {
			Node next = n.nextSibling();
			removed.append(n instanceof Element e ? e.text() : n.toString()).append(' ');
			n.remove();
			n = next;
		}
	}

	private static void appendText(Node node, StringBuilder out) {
		for (Node child : node.childNodes()) {
			if (child instanceof TextNode t) {
				out.append(t.getWholeText().replace('\u00a0', ' ').replaceAll("\\s+", " "));
			} else if (child instanceof Element e) {
				String tag = e.normalName();
				if ("br".equals(tag)) {
					out.append('\n');
					continue;
				}
				boolean block = List.of("p", "div", "tr", "li", "table", "h1", "h2", "h3", "h4", "hr", "pre")
						.contains(tag);
				if (block) {
					out.append('\n');
				}
				appendText(e, out);
				if (block) {
					out.append('\n');
				}
			}
		}
	}

	// ---- text: the same cuts for plain-text mail and for what the html left ----

	private static String cutText(String text, boolean forward, List<Map<String, Object>> cuts) {
		List<String> lines = new ArrayList<>(List.of(text.split("\n", -1)));

		// the gateway banner at the top
		int first = firstNonBlank(lines, 0);
		if (first >= 0 && BANNER.matcher(lines.get(first)).find()) {
			int end = paragraphEnd(lines, first);
			cut(cuts, "banner", String.join("\n", lines.subList(first, end)));
			lines.subList(first, end).clear();
		}

		// quoted history, or a forward's header block
		for (int i = 0; i < lines.size(); i++) {
			String rule = quoteStart(lines, i);
			if (rule == null) {
				continue;
			}
			if (forward) {
				int end = headerEnd(lines, i);
				String author = forwardAuthor(lines.subList(i, end));
				cut(cuts, "forward-header", String.join("\n", lines.subList(i, end)));
				lines.subList(i, end).clear();
				// keep who wrote the forwarded part, so it is not read as the sender's words
				if (author != null) {
					lines.add(i, "Forwarded from " + author + ":");
				}
				break;
			}
			cut(cuts, rule, String.join("\n", lines.subList(i, lines.size())));
			lines.subList(i, lines.size()).clear();
			break;
		}
		List<String> quoted = new ArrayList<>();
		lines.removeIf(l -> {
			boolean q = l.startsWith(">");
			if (q) {
				quoted.add(l);
			}
			return q;
		});
		if (!quoted.isEmpty()) {
			cut(cuts, "quote-lines", String.join("\n", quoted));
		}

		// footers, each cuts to the end
		for (int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			String rule = SIG_DELIMITER.matcher(line).matches() ? "signature-delimiter"
					: MOBILE.matcher(line).matches() ? "mobile-footer"
							: DISCLAIMER.matcher(line).find() ? "disclaimer"
									: isUnsubscribeFooter(lines, i) ? "unsubscribe-footer" : null;
			if (rule != null) {
				cut(cuts, rule, String.join("\n", lines.subList(i, lines.size())));
				lines.subList(i, lines.size()).clear();
				break;
			}
		}

		// a sign-off with a short block under it, near the end
		for (int i = Math.max(0, lastNonBlank(lines) - SIGN_OFF_MAX_LINES - 2); i < lines.size(); i++) {
			if (SIGN_OFF.matcher(lines.get(i)).matches() && shortTail(lines, i + 1)) {
				cut(cuts, "sign-off", String.join("\n", lines.subList(i, lines.size())));
				lines.subList(i, lines.size()).clear();
				break;
			}
		}
		return String.join("\n", lines);
	}

	// the rule name when quoted history starts at this line
	private static String quoteStart(List<String> lines, int i) {
		String line = lines.get(i);
		if (WROTE.matcher(line).matches()
				|| i + 1 < lines.size() && WROTE.matcher(line + " " + lines.get(i + 1).trim()).matches()) {
			return "on-wrote";
		}
		if (ORIGINAL.matcher(line).matches()) {
			return "original-message";
		}
		if ((HEADER_FROM.matcher(line).matches() || RULER.matcher(line).matches()) && headerFollows(lines, i + 1)) {
			return "outlook-header";
		}
		return null;
	}

	// a From: line (or ruler) followed within a few lines by Sent:/Date:/To:/Subject:
	private static boolean headerFollows(List<String> lines, int from) {
		for (int j = from; j < Math.min(lines.size(), from + 4); j++) {
			if (HEADER_NEXT.matcher(lines.get(j)).matches()) {
				return true;
			}
		}
		return false;
	}

	private static int headerEnd(List<String> lines, int start) {
		int i = start + 1;
		while (i < lines.size() && (HEADER_NEXT.matcher(lines.get(i)).matches()
				|| HEADER_FROM.matcher(lines.get(i)).matches() || lines.get(i).isBlank())) {
			i++;
		}
		return i;
	}

	private static String forwardAuthor(List<String> header) {
		for (String line : header) {
			if (HEADER_FROM.matcher(line).matches()) {
				// "From: Priya Raman <priya@x>" gives the name, a bare address is kept as is
				String from = line.replaceFirst("(?i)^\\s*\\*?from:\\*?\\s+", "").trim();
				String name = from.replaceAll("\\s*<[^>]*>\\s*", "").replace("\"", "").trim();
				return name.isEmpty() ? from.replaceAll("[<>]", "") : name;
			}
		}
		return null;
	}

	private static boolean isUnsubscribeFooter(List<String> lines, int i) {
		// only in the last paragraphs, so a message about unsubscribing is kept
		return UNSUBSCRIBE.matcher(lines.get(i)).find() && i >= lastNonBlank(lines) - 3;
	}

	private static boolean shortTail(List<String> lines, int from) {
		int count = 0;
		for (int j = from; j < lines.size(); j++) {
			String l = lines.get(j).trim();
			if (l.isEmpty()) {
				continue;
			}
			if (++count > SIGN_OFF_MAX_LINES || l.length() > SIGN_OFF_MAX_CHARS) {
				return false;
			}
		}
		return true;
	}

	private static int firstNonBlank(List<String> lines, int from) {
		for (int i = from; i < lines.size(); i++) {
			if (!lines.get(i).isBlank()) {
				return i;
			}
		}
		return -1;
	}

	private static int lastNonBlank(List<String> lines) {
		for (int i = lines.size() - 1; i >= 0; i--) {
			if (!lines.get(i).isBlank()) {
				return i;
			}
		}
		return -1;
	}

	private static int paragraphEnd(List<String> lines, int start) {
		int i = start;
		while (i < lines.size() && !lines.get(i).isBlank()) {
			i++;
		}
		return i;
	}

	private static String tidy(String text) {
		StringBuilder out = new StringBuilder();
		int blanks = 0;
		for (String line : text.split("\n", -1)) {
			String l = line.strip();
			if (l.isEmpty()) {
				blanks++;
				continue;
			}
			if (out.length() > 0) {
				out.append(blanks > 0 ? "\n\n" : "\n");
			}
			out.append(l);
			blanks = 0;
		}
		return out.toString();
	}

	// what was removed and why; returned to the caller for review, never logged
	private static void cut(List<Map<String, Object>> cuts, String rule, String removed) {
		if (removed == null || removed.isBlank()) {
			return;
		}
		Map<String, Object> c = new LinkedHashMap<>();
		c.put("rule", rule);
		c.put("removed", removed.strip());
		cuts.add(c);
	}
}
