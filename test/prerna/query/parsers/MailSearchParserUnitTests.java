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
package prerna.query.parsers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.query.parsers.MailSearchParser.Field;
import prerna.query.parsers.MailSearchParser.Operator;
import prerna.query.parsers.MailSearchParser.Term;

class MailSearchParserUnitTests {

	@Test
	void wordsMatchAnywhereAndFieldsNarrowTheWordAfterThem() {
		assertEquals(
				List.of(new Term(Field.FROM, "ada@example.com", false), new Term(Field.ANY, "budget", false),
						new Term(Field.ANY, "report", false)),
				MailSearchParser.parse("from:ada@example.com budget report"));
	}

	@Test
	void quotedPhrasesStayTogetherWithOrWithoutAField() {
		assertEquals(
				List.of(new Term(Field.SUBJECT, "budget report", true), new Term(Field.ANY, "next quarter", true),
						new Term(Field.FROM, "Ada Lovelace", true)),
				MailSearchParser.parse("subject:\"budget report\" \"next quarter\" FROM:\"Ada Lovelace\""));
	}

	@Test
	void aFieldFollowedByASpaceTakesWhatFollows() {
		assertEquals(List.of(new Term(Field.FROM, "ada", false), new Term(Field.BODY, "invoice due", true)),
				MailSearchParser.parse("from: ada body: \"invoice due\""));
	}

	@Test
	void everyFieldIsRead() {
		assertEquals(
				List.of(new Term(Field.TO, "bob", false), new Term(Field.CC, "carol", false),
						new Term(Field.SUBJECT, "plan", false), new Term(Field.BODY, "draft", false)),
				MailSearchParser.parse("to:bob cc:carol subject:plan body:draft"));
	}

	@Test
	void aNameThatIsNotAFieldIsKeptAsAnOperatorWithItsValueWhole() {
		assertEquals(
				List.of(new Term(Field.ANY, new Operator("has", ":"), "attachment", false, false),
						new Term(Field.ANY, new Operator("received", ">="), "2026-10-01", false, false),
						new Term(Field.ANY, new Operator("after", ":"), "2026/10/01", false, false),
						new Term(Field.ANY, new Operator("label", ":"), "Client X", true, false),
						new Term(Field.ANY, new Operator("Re", ":"), "budget", false, false)),
				MailSearchParser
						.parse("has:attachment received>=2026-10-01 after:2026/10/01 label:\"Client X\" Re: budget"));
	}

	@Test
	void anOperatorTypedLastWithNothingAfterItIsAWord() {
		assertEquals(List.of(new Term(Field.ANY, "budget", false), new Term(Field.ANY, "Re", false)),
				MailSearchParser.parse("budget Re:"));
	}

	@Test
	void aLeadingMinusExcludesAWordAPhraseAFieldOrAnOperator() {
		assertEquals(List.of(new Term(Field.ANY, null, "draft", false, true),
				new Term(Field.ANY, null, "weekly digest", true, true), new Term(Field.FROM, null, "ada", false, true),
				new Term(Field.ANY, new Operator("label", ":"), "promotions", false, true),
				new Term(Field.SUBJECT, null, "notes", false, true)),
				MailSearchParser.parse("-draft -\"weekly digest\" -from:ada -label:promotions -subject: notes"));
	}

	@Test
	void aMinusOnItsOwnIsAWordAndNoExclusion() {
		assertEquals(List.of(new Term(Field.ANY, "Q3", false), new Term(Field.ANY, "-", false),
				new Term(Field.ANY, "budget", false)), MailSearchParser.parse("Q3 - budget"));
	}

	@Test
	void textForOneFieldReadsNeitherMinusNorOperator() {
		assertEquals(List.of(new Term(Field.SUBJECT, "-draft", false), new Term(Field.SUBJECT, "label:x", false)),
				MailSearchParser.parse(Field.SUBJECT, "-draft label:x"));
	}

	@Test
	void anUnclosedQuoteRunsToTheEndAndAOneWordPhraseIsAWord() {
		assertEquals(List.of(new Term(Field.ANY, "budget", false), new Term(Field.SUBJECT, "q3 plan", true)),
				MailSearchParser.parse("\"budget\" subject:\"q3 plan"));
	}

	@Test
	void nothingToLookForGivesNoTerms() {
		assertTrue(MailSearchParser.parse(null).isEmpty());
		assertTrue(MailSearchParser.parse("   ").isEmpty());
		assertTrue(MailSearchParser.parse("from: \"\"").isEmpty());
	}

	@Test
	void textForOneFieldSplitsIntoWordsOfThatField() {
		assertEquals(
				List.of(new Term(Field.SUBJECT, "daily", false), new Term(Field.SUBJECT, "brief", false),
						new Term(Field.SUBJECT, "from:ops", false), new Term(Field.SUBJECT, "two words", true)),
				MailSearchParser.parse(Field.SUBJECT, "daily brief from:ops \"two words\""));
	}
}
