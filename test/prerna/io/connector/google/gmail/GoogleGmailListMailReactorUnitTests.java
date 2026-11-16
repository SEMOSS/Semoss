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
package prerna.io.connector.google.gmail;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.query.parsers.MailSearchParser;

class GoogleGmailListMailReactorUnitTests {

	@Test
	void eachWordOfAFieldIsATermOfItsOwnAndPhrasesStayQuoted() {
		assertEquals(
				List.of("from:ada", "subject:budget", "report", "subject:\"next quarter\"", "\"due friday\"",
						"invoice"),
				GoogleGmailListMailReactor.searchTerms(MailSearchParser.parse(
						"from:ada subject:budget report subject:\"next quarter\" body:\"due friday\" body:invoice")));
	}

	@Test
	void wordsTypedForTheSubjectEachHaveToBeInIt() {
		assertEquals(List.of("subject:daily", "subject:brief"), GoogleGmailListMailReactor
				.searchTerms(MailSearchParser.parse(MailSearchParser.Field.SUBJECT, "daily brief")));
	}

	@Test
	void gmailSyntaxTypedAsTextIsSearchedAsWordsAndItsOperatorsPassThrough() {
		assertEquals(List.of("budget", "or", "q3", "-report", "has:attachment", "to:bob"), GoogleGmailListMailReactor
				.searchTerms(MailSearchParser.parse("(budget OR q3) -report has:attachment to:bob")));
	}

	@Test
	void excludedTermsKeepTheirMinusAndALoneMinusIsDropped() {
		assertEquals(List.of("-from:ada", "-\"weekly digest\"", "-label:promotions", "q3", "budget"),
				GoogleGmailListMailReactor.searchTerms(
						MailSearchParser.parse("-from:ada -\"weekly digest\" -label:promotions q3 - budget")));
	}

	@Test
	void gmailOperatorsKeepTheirDatesAndQuotedValues() {
		assertEquals(List.of("after:2026/10/01", "label:\"Client X\"", "older_than:2d"), GoogleGmailListMailReactor
				.searchTerms(MailSearchParser.parse("after:2026/10/01 label:\"Client X\" older_than:2d")));
	}

	@Test
	void aNameGmailDoesNotReadIsSearchedAsWords() {
		assertEquals(List.of("Re", "budget", "ticket", "123", "received", "10/01/2026", "size", "100"),
				GoogleGmailListMailReactor
						.searchTerms(MailSearchParser.parse("Re:budget ticket:123 received:10/01/2026 size>100")));
	}

	@Test
	void aMinusInsideTextForOneFieldIsNotAnExclusion() {
		assertEquals(List.of("subject:draft", "subject:notes"), GoogleGmailListMailReactor
				.searchTerms(MailSearchParser.parse(MailSearchParser.Field.SUBJECT, "-draft notes")));
	}
}
