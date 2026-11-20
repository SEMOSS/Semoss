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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.poi.extractor.ExtractorFactory;
import org.apache.poi.extractor.POITextExtractor;
import org.apache.poi.sl.extractor.SlideShowExtractor;
import org.apache.poi.ss.extractor.ExcelExtractor;

import jakarta.mail.BodyPart;
import jakarta.mail.Multipart;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;
import prerna.io.connector.ms.MicrosoftMessageDisplay;

// Office and mail attachments as plain text, so a model that cannot take the file itself can
// still read it. PDFs and images are left alone: every provider takes them as they are.
final class BrainAttachmentText {

	// the same cap the Office app uses for chat uploads
	static final int MAX_CHARS = 60_000;
	private static final Set<String> OFFICE = Set.of("doc", "docx", "docm", "dot", "dotx", "xls", "xlsx", "xlsm",
			"ppt", "pptx", "pptm", "msg");

	record Extracted(String text, boolean truncated) {
	}

	private BrainAttachmentText() {

	}

	static boolean supports(String name) {
		String extension = extension(name);
		return OFFICE.contains(extension) || "eml".equals(extension);
	}

	// header then the file's text, cut at maxChars; null for a type this does not read
	static Extracted extract(Path file, String name, String header, int maxChars) throws Exception {
		String extension = extension(name);
		String body;
		if ("eml".equals(extension)) {
			body = mail(file);
		} else if (OFFICE.contains(extension)) {
			body = office(file);
		} else {
			return null;
		}
		body = tidy(body);
		boolean truncated = body.length() > maxChars;
		if (truncated) {
			body = body.substring(0, maxChars) + "\n\n[Cut at " + maxChars
					+ " characters. Open the original file for the rest.]";
		}
		return new Extracted(header + body, truncated);
	}

	// POI picks the reader from the content: Word, Excel and PowerPoint in either format, and Outlook .msg
	private static String office(Path file) throws Exception {
		try (POITextExtractor extractor = ExtractorFactory.createExtractor(file.toFile())) {
			if (extractor instanceof ExcelExtractor excel) {
				excel.setIncludeSheetNames(true);
				excel.setFormulasNotResults(false);
				excel.setIncludeCellComments(false);
			} else if (extractor instanceof SlideShowExtractor<?, ?> slides) {
				slides.setNotesByDefault(true);
			}
			return extractor.getText();
		}
	}

	private static String mail(Path file) throws Exception {
		try (InputStream in = Files.newInputStream(file)) {
			MimeMessage message = new MimeMessage(Session.getInstance(new Properties()), in);
			StringBuilder out = new StringBuilder();
			line(out, "From", message.getHeader("From", ", "));
			line(out, "Sent", message.getHeader("Date", null));
			line(out, "To", message.getHeader("To", ", "));
			line(out, "Subject", message.getSubject());
			out.append('\n');
			List<String> attached = new ArrayList<>();
			String text = body(message, attached);
			out.append(text == null ? "" : text);
			if (!attached.isEmpty()) {
				out.append("\n\nAttached to this email: ").append(String.join(", ", attached));
			}
			return out.toString();
		}
	}

	// the first plain text body, else the first HTML body as text; attached files are only named
	private static String body(Part part, List<String> attached) throws Exception {
		String fileName = part.getFileName();
		if (fileName != null && (Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition()) || !part.isMimeType("text/*"))) {
			attached.add(fileName);
			return null;
		}
		if (part.isMimeType("text/plain")) {
			return String.valueOf(part.getContent());
		}
		if (part.isMimeType("text/html")) {
			return html(String.valueOf(part.getContent()));
		}
		if (part.isMimeType("message/rfc822")) {
			attached.add("an attached email");
			return null;
		}
		if (!(part.getContent() instanceof Multipart multipart)) {
			return null;
		}
		boolean alternative = part.isMimeType("multipart/alternative");
		String found = null;
		String html = null;
		for (int i = 0; i < multipart.getCount(); i++) {
			BodyPart child = multipart.getBodyPart(i);
			if (alternative && child.isMimeType("text/html")) {
				html = html == null ? html(String.valueOf(child.getContent())) : html;
				continue;
			}
			String text = body(child, attached);
			if (found == null && text != null && !text.isBlank()) {
				found = text;
			}
		}
		return found != null ? found : html;
	}

	private static String html(String html) {
		return MicrosoftMessageDisplay.text(Map.of("body", Map.of("contentType", "html", "content", html)));
	}

	private static void line(StringBuilder out, String label, String value) {
		if (value != null && !value.isBlank()) {
			out.append(label).append(": ").append(value.strip()).append('\n');
		}
	}

	private static String tidy(String text) {
		if (text == null) {
			return "";
		}
		String out = text.replace("\r\n", "\n").replace('\r', '\n').replace("\u0000", "");
		out = out.replaceAll("[ \\t\\x0B\\f]+\\n", "\n");
		out = out.replaceAll("\\n{3,}", "\n\n");
		return out.strip();
	}

	private static String extension(String name) {
		if (name == null) {
			return "";
		}
		int dot = name.lastIndexOf('.');
		return dot < 0 ? "" : name.substring(dot + 1).toLowerCase(Locale.ROOT).strip();
	}
}
