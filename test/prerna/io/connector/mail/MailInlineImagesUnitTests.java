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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class MailInlineImagesUnitTests {

	private static final String HTML = "<p>Hi</p><img src=\"cid:Logo@Example\"><img src='cid:%3Csig%40example%3E'>"
			+ "<div style=\"background:url(cid:banner@example)\"></div>";

	@Test
	void namesEachReferencedContentIdOnceWithoutBracketsEscapesOrCase() {
		assertEquals(List.of("logo@example", "sig@example", "banner@example"),
				new ArrayList<>(MailInlineImages.referencedContentIds(HTML)));
		assertEquals("logo@example", MailInlineImages.normalize(" <Logo@Example> "));
		assertNull(MailInlineImages.normalize("<>"));
		assertNull(MailInlineImages.normalize(null));
	}

	@Test
	void returnsOnlyThePicturesTheBodyNamesAsBase64() {
		List<String> read = new ArrayList<>();
		Map<String, Object> collected = MailInlineImages.collect(HTML,
				List.of(candidate("<logo@example>", "image/PNG; name=logo.png", 4L, "logo", read),
						candidate("<unused@example>", "image/png", 4L, "unused", read),
						candidate("<banner@example>", "application/pdf", 4L, "banner", read),
						candidate("<banner@example>", "image/gif", 6L, "banner", read)));
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> images = (List<Map<String, Object>>) collected.get(MailInlineImages.IMAGES);
		assertEquals(List.of("logo", "banner"), read);
		assertEquals(2, images.size());
		assertEquals("logo@example", images.get(0).get("contentId"));
		assertEquals("image/png", images.get(0).get("contentType"));
		assertEquals(base64("logo"), images.get(0).get("data"));
		assertEquals("banner@example", images.get(1).get("contentId"));
		assertEquals(0, collected.get(MailInlineImages.OMITTED));
	}

	@Test
	void takesTheContentIdFromThePictureWhenOnlyReadingItSays() {
		Map<String, Object> collected = MailInlineImages.collect(HTML,
				List.of(new MailInlineImages.Candidate(null, "image/png", null,
						() -> new MailInlineImages.Picture("Logo@Example", bytes("logo"))),
						new MailInlineImages.Candidate(null, "image/png", null,
								() -> new MailInlineImages.Picture("elsewhere@example", bytes("other")))));
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> images = (List<Map<String, Object>>) collected.get(MailInlineImages.IMAGES);
		assertEquals(1, images.size());
		assertEquals("logo@example", images.get(0).get("contentId"));
		assertEquals(0, collected.get(MailInlineImages.OMITTED));
	}

	@Test
	void countsThePicturesLeftOutForSizeOrBecauseTheyCouldNotBeRead() {
		List<String> read = new ArrayList<>();
		Map<String, Object> collected = MailInlineImages.collect(HTML,
				List.of(candidate("<logo@example>", "image/png", MailInlineImages.MAX_IMAGE_BYTES + 1, "logo", read),
						new MailInlineImages.Candidate("<sig@example>", "image/png", 4L, () -> {
							throw new IllegalStateException("the mailbox is down");
						}), new MailInlineImages.Candidate("<banner@example>", "image/png", null, () -> null)));
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> images = (List<Map<String, Object>>) collected.get(MailInlineImages.IMAGES);
		assertFalse(read.contains("logo"));
		assertEquals(0, images.size());
		assertEquals(3, collected.get(MailInlineImages.OMITTED));
	}

	@Test
	void stopsAtTheTotalForOneEmail() {
		long size = MailInlineImages.MAX_IMAGE_BYTES;
		List<MailInlineImages.Candidate> candidates = new ArrayList<>();
		StringBuilder html = new StringBuilder();
		for (int i = 0; i < 4; i++) {
			html.append("<img src=\"cid:p").append(i).append("\">");
			candidates.add(new MailInlineImages.Candidate("p" + i, "image/png", size,
					() -> new MailInlineImages.Picture(null, new byte[(int) size])));
		}
		Map<String, Object> collected = MailInlineImages.collect(html.toString(), candidates);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> images = (List<Map<String, Object>>) collected.get(MailInlineImages.IMAGES);
		assertEquals(MailInlineImages.MAX_TOTAL_BYTES / size, images.size());
		assertEquals(4 - images.size(), collected.get(MailInlineImages.OMITTED));
	}

	private static MailInlineImages.Candidate candidate(String contentId, String contentType, Long size, String content,
			List<String> read) {
		return new MailInlineImages.Candidate(contentId, contentType, size, () -> {
			read.add(content);
			return new MailInlineImages.Picture(null, bytes(content));
		});
	}

	private static byte[] bytes(String content) {
		return content.getBytes(StandardCharsets.UTF_8);
	}

	private static String base64(String content) {
		return Base64.getEncoder().encodeToString(bytes(content));
	}
}
