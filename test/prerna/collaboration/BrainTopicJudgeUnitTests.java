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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class BrainTopicJudgeUnitTests {

	private static final String MESSAGE = "Hi all, after the vendor call we agreed the cutover moves to 18 November. "
			+ "It\u2019s final. Thanks, Priya";

	@Test
	void aQuoteCountsOnlyWhenTheMessageHoldsIt() {
		// the message's own words, whatever their case, spacing, or quote marks
		assertNotNull(BrainTopicJudge.locate(MESSAGE, "the cutover moves to 18 November"));
		assertNotNull(BrainTopicJudge.locate(MESSAGE, "THE CUTOVER  moves to 18 November. It's final."));
		// made up, or too short to prove anything
		assertNull(BrainTopicJudge.locate(MESSAGE, "the cutover moves to 20 November"));
		assertNull(BrainTopicJudge.locate(MESSAGE, "cutover"));
		assertNull(BrainTopicJudge.locate(MESSAGE, null));
	}
}
