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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.collaboration.BrainRulesGate.Rule;

class BrainMessageTextUnitTests {

	// Outlook desktop writes Word HTML: a paragraph per line, an empty line is an &nbsp; paragraph
	private static String word(String body) {
		return "<html xmlns:o=\"urn:schemas-microsoft-com:office:office\"><head><meta name=\"Generator\" "
				+ "content=\"Microsoft Word 15 (filtered medium)\"><style><!--p.MsoNormal{margin:0in;font-size:11.0pt;"
				+ "font-family:\"Calibri\",sans-serif;}--></style></head><body lang=\"EN-US\" link=\"#0563C1\" "
				+ "vlink=\"#954F72\" style=\"word-wrap:break-word\"><div class=\"WordSection1\">" + body
				+ "</div></body></html>";
	}

	private static String p(String... lines) {
		StringBuilder html = new StringBuilder();
		for (String line : lines) {
			html.append("<p class=\"MsoNormal\">").append(line.isEmpty() ? "<o:p>&nbsp;</o:p>" : line + "<o:p></o:p>")
					.append("</p>");
		}
		return html.toString();
	}

	// Outlook on the web and new Outlook: a div per line, an empty line is a div holding a br
	private static String owa(String body) {
		return "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"><style "
				+ "type=\"text/css\" style=\"display:none\">P {margin-top:0;margin-bottom:0;}</style></head>"
				+ "<body dir=\"ltr\">" + body + "</body></html>";
	}

	private static String divs(String... lines) {
		StringBuilder html = new StringBuilder();
		for (String line : lines) {
			html.append("<div class=\"elementToProof\" style=\"font-family: Aptos, Aptos_EmbeddedFont, "
					+ "Aptos_MSFontService, Calibri, Helvetica, sans-serif; font-size: 12pt; color: rgb(0, 0, 0);\">")
					.append(line.isEmpty() ? "<br>" : line).append("</div>");
		}
		return html.toString();
	}

	private static final String OUTLOOK_HEADER = "<div style=\"border:none;border-top:solid #E1E1E1 1.0pt;"
			+ "padding:3.0pt 0in 0in 0in\"><p class=\"MsoNormal\"><b>From:</b> Dana Smith &lt;dana@northwind.example&gt; "
			+ "<br><b>Sent:</b> Monday, November 3, 2026 10:00 AM<br><b>To:</b> Owner &lt;owner@contoso.example&gt;<br>"
			+ "<b>Subject:</b> Budget<o:p></o:p></p></div>";

	private static String owaReplyFrom(String address, String quoted) {
		return "<div id=\"appendonsend\"></div><hr style=\"display:inline-block;width:98%\" tabindex=\"-1\">"
				+ "<div id=\"divRplyFwdMsg\" dir=\"ltr\"><font face=\"Calibri, sans-serif\" style=\"font-size:11pt\" "
				+ "color=\"#000000\"><b>From:</b> Dana Smith &lt;" + address + "&gt;<br><b>Sent:</b> Monday, November 3, "
				+ "2026 10:00 AM<br><b>To:</b> Owner &lt;owner@contoso.example&gt;<br><b>Subject:</b> Budget</font>"
				+ "<div>&nbsp;</div></div>" + divs(quoted);
	}

	private static String body(String subject, String html, boolean keepHistory) {
		return (String) BrainMessageText.extract(subject, null, null, html, "html", keepHistory).get("body");
	}

	@Test
	void aThankYouAboveTheAskIsNotASignature() {
		assertEquals("Thanks!\n\nCan you send the signed copy today?\n\nAmy",
				body("Contract", word(p("Thanks!", "Can you send the signed copy today?", "", "Amy")), false));
		assertEquals("Hi Jo,\n\nThanks!\n\nOne more: can you add Mike to the invite?\n\nRaj", body("Offsite",
				owa(divs("Hi Jo,", "", "Thanks!", "One more: can you add Mike to the invite?", "", "Raj")), false));
		// a thank-you note is all message, so nothing is cut as its sign-off
		assertEquals("Thanks!\n\nAmy", body("Contract", word(p("Thanks!", "", "Amy")), false));
	}

	@Test
	void aMessageThatOpensLikeADisclaimerKeepsItsText() {
		assertEquals("This email is for everyone attending Thursday's offsite in Arlington.\n\n"
				+ "Please RSVP by Wednesday so we can order lunch.",
				body("Offsite", word(p("This email is for everyone attending Thursday's offsite in Arlington.",
						"Please RSVP by Wednesday so we can order lunch.")), false));
		assertEquals("Hi team,\n\nThis email is for everyone attending Thursday's offsite.\n\nThe bus leaves at 8:30."
				+ "\n\nPlease RSVP by Wednesday.",
				body("Offsite", word(p("Hi team,", "", "This email is for everyone attending Thursday's offsite.",
						"The bus leaves at 8:30.", "Please RSVP by Wednesday.", "", "Thanks,", "Jo")), false));
		// plain text: a sign-off under it shows the writer was still talking
		assertEquals("Hi all,\n\nThis email is for everyone attending the offsite. Please RSVP by Wednesday.",
				BrainMessageText.extract("Offsite", null, null, "Hi all,\n\nThis email is for everyone attending the "
						+ "offsite. Please RSVP by Wednesday.\n\nThanks,\nJo", "text").get("body"));
		// and with only a name under it: it says nothing of who may read it, so it is no notice
		assertEquals("Hi all,\n\nThis email is for everyone attending the offsite. Please RSVP by Wednesday.\n\nJo",
				BrainMessageText.extract("Offsite", null, null, "Hi all,\n\nThis email is for everyone attending the "
						+ "offsite. Please RSVP by Wednesday.\n\nJo", "text").get("body"));
	}

	@Test
	void aRealDisclaimerAndSignatureAreStillCut() {
		Map<String, Object> disclaimer = BrainMessageText.extract("Contract", null, null,
				word(p("Please review the attached contract before Friday.", "", "Thanks,", "Priya", "",
						"CONFIDENTIALITY NOTICE: This email and any attachments are confidential and intended solely for "
								+ "the addressee. If you are not the intended recipient, please notify the sender and "
								+ "delete it.")),
				"html");
		assertEquals("Please review the attached contract before Friday.", disclaimer.get("body"));
		assertTrue(BrainMessageText.footer(disclaimer).contains("CONFIDENTIALITY NOTICE"));

		assertEquals("Approved, go ahead with the order.\n\nJo", BrainMessageText.extract("Order", null, null,
				"Approved, go ahead with the order.\n\nJo\n\nThe information contained in this email is confidential "
						+ "and may be\nlegally privileged. It is intended solely for the addressee. If you\nare not the "
						+ "intended recipient, please delete it.\n",
				"text").get("body"));

		assertEquals("Approved, go ahead with the order.\n\nJo", BrainMessageText.extract("Order", null, null,
				"Approved, go ahead with the order.\n\nJo\n\nThis e-mail is intended only for the person to whom it\n"
						+ "is addressed. If you received it in error, please delete it.\n",
				"text").get("body"));

		Map<String, Object> signature = BrainMessageText.extract("Q3", null, null, word(p(
				"Can you send me the Q3 numbers by Friday?", "", "Best regards,", "Priya Raman",
				"Director of Finance | Acme Corp", "+1 555 010 0199")), "html");
		assertEquals("Can you send me the Q3 numbers by Friday?", signature.get("body"));
		assertTrue(BrainMessageText.footer(signature).contains("Director of Finance"));
	}

	@Test
	void quotedRepliesAreCutAndKeptHistoryNamesItsSender() {
		Map<String, String> replies = Map.of(
				"Outlook desktop", word(p("Yes, 3 works.", "") + OUTLOOK_HEADER + p("", "Can we meet at 3?")),
				"Outlook on the web", owa(divs("Yes, 3 works.") + "<div id=\"Signature\"><div>Olivia Owner<br>"
						+ "Program Lead | Contoso</div></div>" + owaReplyFrom("dana@northwind.example", "Can we meet at 3?")),
				"Gmail", "<div dir=\"ltr\">Yes, 3 works.</div><br><div class=\"gmail_quote gmail_quote_container\">"
						+ "<div dir=\"ltr\" class=\"gmail_attr\">On Mon, Nov 3, 2026 at 10:00 AM Dana Smith &lt;<a "
						+ "href=\"mailto:dana@northwind.example\">dana@northwind.example</a>&gt; wrote:<br></div>"
						+ "<blockquote class=\"gmail_quote\" style=\"margin:0px 0px 0px 0.8ex;border-left:1px solid "
						+ "rgb(204,204,204);padding-left:1ex\"><div dir=\"ltr\">Can we meet at 3?</div></blockquote></div>",
				"Apple Mail", "<html><head><meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\"></head>"
						+ "<body dir=\"auto\"><div dir=\"ltr\">Yes, 3 works.</div><div dir=\"ltr\"><br><blockquote "
						+ "type=\"cite\">On Nov 3, 2026, at 10:00 AM, Dana Smith &lt;dana@northwind.example&gt; wrote:"
						+ "<br><br></blockquote></div><blockquote type=\"cite\"><div dir=\"ltr\">Can we meet at 3?</div>"
						+ "</blockquote></body></html>");
		replies.forEach((client, html) -> {
			assertEquals("Yes, 3 works.", body("RE: Budget", html, false), client);
			Map<String, Object> kept = BrainMessageText.extract("RE: Budget", null, null, html, "html", true);
			assertTrue(((String) kept.get("body")).startsWith("Yes, 3 works."), client);
			assertTrue(((String) kept.get("body")).endsWith("Can we meet at 3?"), client);
			assertFalse(((String) kept.get("body")).contains("Olivia Owner"), client);
			assertEquals("dana@northwind.example", kept.get("historyFrom"), client);
		});
		// quoted history that is cut names nobody
		assertNull(BrainMessageText.extract("RE: Budget", null, null, replies.get("Gmail"), "html").get("historyFrom"));
	}

	@Test
	void anIndentIsPartOfTheMessageButAQuoteUnderOnWroteIsNot() {
		// Gmail's indent button
		assertEquals("Here is the plan for Friday:\n\nLoad-in at 9, doors at 10.\n\nCan you confirm the room by noon?",
				body("Friday", "<div dir=\"ltr\">Here is the plan for Friday:<blockquote style=\"margin:0px 0px 0px "
						+ "40px;border:none;padding:0px\"><div>Load-in at 9, doors at 10.</div></blockquote><div>Can you "
						+ "confirm the room by noon?</div><div><br></div><div>Thanks,</div><div>Sam</div></div>", false));
		String reply = "Sounds good, see you at 3.<br><br>On Mon, Nov 3, 2026 at 10:00 AM, Dana Smith &lt;"
				+ "dana@northwind.example&gt; wrote:<blockquote style=\"margin:0 0 0 .8ex;border-left:1px #ccc solid;"
				+ "padding-left:1ex\">Can we meet at 3?</blockquote>";
		assertEquals("Sounds good, see you at 3.", body("RE: Budget", reply, false));
		assertEquals("dana@northwind.example",
				BrainMessageText.extract("RE: Budget", null, null, reply, "html", true).get("historyFrom"));
	}

	@Test
	void aForwardCanLeaveItsHistoryOut() {
		String forward = "<div dir=\"ltr\">FYI, see Dana's note below.<br><br><div class=\"gmail_quote "
				+ "gmail_quote_container\"><div dir=\"ltr\" class=\"gmail_attr\">---------- Forwarded message ---------"
				+ "<br>From: <strong class=\"gmail_sendername\" dir=\"auto\">Dana Smith</strong> <span dir=\"auto\">&lt;"
				+ "<a href=\"mailto:dana@northwind.example\">dana@northwind.example</a>&gt;</span><br>Date: Mon, Nov 3, "
				+ "2026 at 10:00 AM<br>Subject: Salary review<br>To: Owner &lt;<a href=\"mailto:owner@contoso.example\">"
				+ "owner@contoso.example</a>&gt;<br></div><br><br><div dir=\"ltr\">My salary review notes are attached."
				+ "</div></div></div>";
		Map<String, Object> kept = BrainMessageText.extract("Fwd: Salary review", null, null, forward, "html");
		assertEquals("FYI, see Dana's note below.\n\nForwarded from Dana Smith, Mon, Nov 3, 2026 at 10:00 AM:\n"
				+ "My salary review notes are attached.", kept.get("body"));
		assertEquals("dana@northwind.example", kept.get("historyFrom"));

		Map<String, Object> left = BrainMessageText.extract("Fwd: Salary review", null, null, forward, "html", false,
				false);
		assertEquals("FYI, see Dana's note below.", left.get("body"));
		assertEquals(false, left.get("history"));
		assertNull(left.get("historyFrom"));
	}

	// a thread whose only held email is the owner's reply quoting Dana, whose own mail was never ingested
	private static Map<?, ?> readReply(List<Rule> rules) {
		Map<String, Object> reply = Map.of("subject", "RE: Salary review", "from",
				Map.of("emailAddress", Map.of("name", "Owner", "address", "owner@contoso.example")), "body",
				Map.of("contentType", "html", "content",
						owa(divs("Yes, 3 works for me.") + owaReplyFrom("Dana@Northwind.Example",
								"My salary review notes are attached."))),
				"uniqueBody", Map.of("contentType", "html", "content", owa(divs("Yes, 3 works for me."))));
		try (MockedStatic<CollaborationDbUtils> db = mockStatic(CollaborationDbUtils.class, invocation -> {
			String method = invocation.getMethod().getName();
			if (method.equals("queryOne")) {
				return new String[] { "email", "email:conversation", "false" };
			}
			if (method.equals("query")) {
				String sql = invocation.getArgument(0);
				if (!sql.contains("FROM BRAIN_MESSAGE")) {
					return List.of();
				}
				ResultSet rs = mock(ResultSet.class);
				when(rs.getString("MESSAGE_KEY")).thenReturn("m-1");
				when(rs.getString("GRAPH_ID")).thenReturn("g-1");
				when(rs.getString("DECISION")).thenReturn(BrainRulesGate.INGESTED);
				CollaborationDbUtils.RowMapper<?> mapper = invocation.getArgument(1);
				return List.of(mapper.map(rs));
			}
			if (method.equals("getTimestamp")) {
				return "2026-11-03T15:00:00Z";
			}
			return invocation.callRealMethod();
		}); MockedStatic<BrainRulesGate> gate = mockStatic(BrainRulesGate.class, invocation -> {
			String method = invocation.getMethod().getName();
			if (method.equals("activeRules")) {
				return rules;
			}
			if (method.equals("findPerson")) {
				return "dana@northwind.example".equals(invocation.getArgument(2)) ? "p-dana" : null;
			}
			return invocation.callRealMethod();
		})) {
			BrainMessageSource messages = (user, source, conversation, id) -> reply;
			Map<String, Object> read = BrainThreadMessages.read(null, "owner", "MICROSOFT", "thread-1", 20, messages);
			assertEquals(0, read.get("hiddenCount"));
			return (Map<?, ?>) ((List<?>) read.get("messages")).get(0);
		}
	}

	@Test
	void aReplyQuotingANeverIngestSenderLosesTheQuote() {
		for (Rule never : List.of(new Rule("r-1", "never_sender", "dana@northwind.example", null, null, null),
				new Rule("r-2", "exclude_everywhere", null, null, "p-dana", null),
				new Rule("r-3", "never_domain", "northwind.example", null, null, null))) {
			Map<?, ?> email = readReply(List.of(never));
			assertEquals("Yes, 3 works for me.", email.get("text"), never.kind());
			assertNull(email.get("history"), never.kind());
		}
		Map<?, ?> allowed = readReply(List.of(new Rule("r-4", "never_sender", "someone@else.example", null, null, null)));
		assertEquals("Yes, 3 works for me.\n\nEarlier message from Dana Smith, Monday, November 3, 2026 10:00 AM:\n"
				+ "My salary review notes are attached.", allowed.get("text"));
		assertEquals(true, allowed.get("history"));
	}
}
