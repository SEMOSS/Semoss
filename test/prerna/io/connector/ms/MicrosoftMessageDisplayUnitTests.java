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
package prerna.io.connector.ms;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MicrosoftMessageDisplayUnitTests {
    @Test void preservesFullHtmlAndAttachmentLabels() {
        String html = "<table><tr><td>Full email</td></tr></table><blockquote>Earlier email</blockquote>";
        Map<String,Object> display = MicrosoftMessageDisplay.body(Map.of("body", Map.of("contentType", "HTML", "content", html), "attachments", List.of(Map.of("name", "Card"))), "Clean context");
        assertEquals(html, display.get("content"));
        assertEquals("html", display.get("contentType"));
        assertEquals(false, display.get("isTruncated"));
        assertEquals(List.of(Map.of("name", "Card")), display.get("attachments"));
    }
    @Test void oversizedHtmlFallsBackWithoutSlicingMarkup() {
        String html = "<p>" + "x".repeat(MicrosoftMessageDisplay.MAX_DISPLAY_CHARS) + "</p>";
        Map<String,Object> display = MicrosoftMessageDisplay.body(Map.of("body", Map.of("contentType", "HTML", "content", html)), "Readable fallback");
        assertEquals("text", display.get("contentType"));
        assertEquals("Readable fallback", display.get("content"));
        assertEquals(true, display.get("isTruncated"));
    }
    @Test void keepsBoundaryAndLegacyPlainText() {
        String text = "x".repeat(MicrosoftMessageDisplay.MAX_DISPLAY_CHARS);
        assertEquals(false, MicrosoftMessageDisplay.body(Map.of("body", Map.of("contentType", "text", "content", text)), text).get("isTruncated"));
        assertEquals("Legacy", MicrosoftMessageDisplay.body(Map.of(), "Legacy").get("content"));
    }
    @Test void teamsKeepQuotesSignaturesAndCodeWhitespace() {
        String html = "<p>Hello <at>Pat</at></p><blockquote>From: Earlier<br>Quoted reply</blockquote><pre><code>  first\n    second</code></pre><p>Thanks,<br>Pat</p>";
        String text = MicrosoftMessageDisplay.text(Map.of("body", Map.of("contentType", "html", "content", html)));
        assertTrue(text.contains("From: Earlier\nQuoted reply"));
        assertTrue(text.contains("  first\n    second"));
        assertTrue(text.endsWith("Thanks,\nPat"));
    }
}
