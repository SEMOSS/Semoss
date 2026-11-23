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
package prerna.io.connector.mail;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The pictures an email shows inside its html body, which the body names by
 * content id, as in {@code <img src="cid:logo@example">}, read so a viewer can
 * show them without asking the mailbox for each one.
 *
 * <p>
 * Only pictures the body names come back, and only up to
 * {@value #MAX_IMAGE_BYTES} bytes each and {@value #MAX_TOTAL_BYTES} bytes in
 * all; the ones left out are counted, so a viewer can say some are missing.
 * </p>
 */
public final class MailInlineImages {

	private static final Logger classLogger = LogManager.getLogger(MailInlineImages.class);

	/** The largest one picture can be and still come back. */
	public static final long MAX_IMAGE_BYTES = 3L * 1024L * 1024L;

	/** The most the pictures of one email can come to. */
	public static final long MAX_TOTAL_BYTES = 10L * 1024L * 1024L;

	/** Where the pictures come back in a display body. */
	public static final String IMAGES = "inlineImages";

	/** How many pictures the body names that did not come back. */
	public static final String OMITTED = "inlineImagesOmitted";

	/**
	 * A {@code cid:} reference, up to the quote, bracket, or space that ends it.
	 */
	private static final Pattern CID_REFERENCE = Pattern.compile("cid:([^\"'\\s)>]+)", Pattern.CASE_INSENSITIVE);

	/** A media type a browser shows as a picture, without its parameters. */
	private static final Pattern IMAGE_TYPE = Pattern.compile("image/[a-z0-9.+-]+");

	private MailInlineImages() {

	}

	/**
	 * Reads the bytes of one picture.
	 */
	@FunctionalInterface
	public interface Reader {

		/**
		 * Read the picture.
		 *
		 * @return the picture, or null when the mailbox has no bytes for it
		 * @throws Exception when the mailbox cannot be read
		 */
		Picture read() throws Exception;
	}

	/**
	 * A picture as read from the mailbox.
	 *
	 * @param contentId the content id the body names it by, or null when only the
	 *                  candidate knew it
	 * @param bytes     the picture
	 */
	public record Picture(String contentId, byte[] bytes) {
	}

	/**
	 * A picture attached to an email that its body may show.
	 *
	 * @param contentId   the content id the body names it by, or null when the
	 *                    mailbox says only once it is read
	 * @param contentType its media type
	 * @param size        its size in bytes, when the mailbox says before it is read
	 * @param reader      reads it, which happens only when the body names it and it
	 *                    fits
	 */
	public record Candidate(String contentId, String contentType, Long size, Reader reader) {
	}

	/**
	 * Read the pictures an html body shows by content id.
	 *
	 * @param html       the body
	 * @param candidates the pictures attached to the email, in the order to read
	 *                   them
	 * @return {@value #IMAGES}, each with its {@code contentId} as a viewer matches
	 *         it, its {@code contentType}, and its bytes as base64 {@code data},
	 *         and {@value #OMITTED}, how many pictures the body names that were too
	 *         large or could not be read
	 */
	public static Map<String, Object> collect(String html, List<Candidate> candidates) {
		Set<String> wanted = referencedContentIds(html);
		List<Map<String, Object>> images = new ArrayList<>();
		int omitted = 0;
		long total = 0;
		for (Candidate candidate : candidates) {
			if (wanted.isEmpty()) {
				break;
			}
			String contentType = imageType(candidate.contentType());
			String knownId = normalize(candidate.contentId());
			if (contentType == null || (knownId != null && !wanted.contains(knownId))) {
				continue;
			}
			Long size = candidate.size();
			if (size != null && (size > MAX_IMAGE_BYTES || total + size > MAX_TOTAL_BYTES)) {
				omitted++;
				continue;
			}
			Picture picture;
			try {
				picture = candidate.reader().read();
			} catch (Exception e) {
				classLogger.warn("Could not read an inline picture of an email", e);
				omitted++;
				continue;
			}
			if (picture == null || picture.bytes() == null) {
				omitted++;
				continue;
			}
			String contentId = picture.contentId() == null ? knownId : normalize(picture.contentId());
			if (contentId == null || !wanted.contains(contentId)) {
				continue;
			}
			long read = picture.bytes().length;
			if (read > MAX_IMAGE_BYTES || total + read > MAX_TOTAL_BYTES) {
				omitted++;
				continue;
			}
			total += read;
			wanted.remove(contentId);
			Map<String, Object> image = new LinkedHashMap<>();
			image.put("contentId", contentId);
			image.put("contentType", contentType);
			image.put("data", Base64.getEncoder().encodeToString(picture.bytes()));
			images.add(image);
		}
		Map<String, Object> output = new LinkedHashMap<>();
		output.put(IMAGES, images);
		output.put(OMITTED, omitted);
		return output;
	}

	/**
	 * The content ids an html body names, the way {@link #normalize} writes them.
	 *
	 * @param html the body
	 * @return the content ids, in the order the body names them
	 */
	public static Set<String> referencedContentIds(String html) {
		Set<String> ids = new LinkedHashSet<>();
		if (html == null) {
			return ids;
		}
		Matcher matcher = CID_REFERENCE.matcher(html);
		while (matcher.find()) {
			String id = normalize(matcher.group(1));
			if (id != null) {
				ids.add(id);
			}
		}
		return ids;
	}

	/**
	 * A content id the way the body's references and the mailbox's parts are
	 * matched: without the angle brackets a {@code Content-ID} header carries, with
	 * the percent escapes a {@code cid:} url may have decoded, in lower case.
	 *
	 * @param contentId a content id, from a header or a {@code cid:} reference
	 * @return the content id, or null when there is none
	 */
	public static String normalize(String contentId) {
		if (contentId == null) {
			return null;
		}
		String id = contentId.trim();
		if (id.contains("%")) {
			try {
				id = URLDecoder.decode(id.replace("+", "%2B"), StandardCharsets.UTF_8).trim();
			} catch (IllegalArgumentException e) {
				classLogger.debug("Kept a content id with a malformed escape as it was: {}", id, e);
			}
		}
		if (id.startsWith("<") && id.endsWith(">")) {
			id = id.substring(1, id.length() - 1).trim();
		}
		return id.isEmpty() ? null : id.toLowerCase(Locale.ROOT);
	}

	/**
	 * The picture type a media type names, which is what a data address says the
	 * bytes are.
	 *
	 * @param contentType a media type, possibly with parameters such as a name
	 * @return the media type without its parameters, in lower case, or null when it
	 *         is not a picture
	 */
	private static String imageType(String contentType) {
		if (contentType == null) {
			return null;
		}
		String type = contentType.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
		return IMAGE_TYPE.matcher(type).matches() ? type : null;
	}
}
