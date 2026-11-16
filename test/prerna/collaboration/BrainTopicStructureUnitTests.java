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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class BrainTopicStructureUnitTests {

	private static final String ME = "dan@plant.example";
	private static final String[] PEOPLE = { "peter@plant.example", "marco@plant.example", "louis@plant.example" };
	// three pieces of work the same three people write about, under subjects that say nothing
	private static final String[][] WORK = {
			{ "compressor", "freezer", "rental", "adjuster", "claim", "teardown", "blast", "deductible" },
			{ "capital", "ranking", "payback", "committee", "boiler", "envelope", "depreciation", "roof" },
			{ "forecast", "pallets", "shortage", "croissant", "volume", "weekly", "customer", "order" } };
	private static final String[] FILLER = { "please", "thanks", "today", "tomorrow", "morning", "update", "numbers",
			"call", "send", "back", "know", "think", "need", "week", "time", "look" };

	private record World(List<BrainTopicStructure.MailThread> threads, Map<String, List<BrainTopicStructure.Text>> texts,
			Map<String, Integer> work) {
	}

	// 3 x 14 threads; each uses a few of its work's words among everyday ones
	private static World world(boolean signOff) {
		List<BrainTopicStructure.MailThread> threads = new ArrayList<>();
		Map<String, List<BrainTopicStructure.Text>> texts = new HashMap<>();
		Map<String, Integer> work = new HashMap<>();
		for (int w = 0; w < WORK.length; w++) {
			for (int k = 0; k < 14; k++) {
				String id = "t" + w + "-" + k;
				String sender = PEOPLE[(w + k) % PEOPLE.length];
				StringBuilder said = new StringBuilder();
				for (int i = 0; i < 4; i++) {
					// two of the three words most of its mail uses, two of the rarer ones
					said.append(WORK[w][i < 2 ? (k + i) % 3 : 3 + (k + i * 2) % 5]).append(' ')
							.append(FILLER[(k * 5 + i * 7 + w) % FILLER.length]).append(' ');
				}
				String sign = signOff ? " regards " + sender.substring(0, sender.indexOf('@'))
						+ " maintenance supervisor madison office confidential" + sender.length() : "";
				threads.add(new BrainTopicStructure.MailThread(id,
						List.of(new BrainTopicStructure.Message("Quick question", "2026-03-" + String.format("%02d", 1 + k)
								+ "T10:00:00Z", sender, List.of(ME), List.of(), false),
								new BrainTopicStructure.Message("RE: Quick question", "2026-03-"
										+ String.format("%02d", 2 + k) + "T10:00:00Z", ME, List.of(sender), List.of(), false))));
				texts.put(id, List.of(new BrainTopicStructure.Text(sender, said + sign),
						new BrainTopicStructure.Text(ME, "ok " + WORK[w][k % WORK[w].length] + " noted")));
				work.put(id, w);
			}
		}
		return new World(threads, texts, work);
	}

	private static BrainTopicStructure.Prepared prepare(World world, BrainTopicStructure.Reader reader) {
		return BrainTopicStructure.prepare(world.threads(), ME, d -> d.equals("plant.example"), Set.of(),
				BrainTopicStructure.WIDE, Set.of(), Map.of(), reader);
	}

	// each group's threads, by the piece of work they belong to
	private static List<Map<Integer, Integer>> groups(World world, BrainTopicStructure.Prepared prepared) {
		List<Map<Integer, Integer>> out = new ArrayList<>();
		for (int g = 0; g < prepared.seeds().size(); g++) {
			out.add(new HashMap<>());
		}
		prepared.filed().forEach((thread, g) -> out.get(g).merge(world.work().get(prepared.pool().get(thread).id()), 1,
				Integer::sum));
		return out;
	}

	@Test
	void mailTheSamePeopleWriteUnderTheSameSubjectIsGroupedByWhatItSays() {
		World world = world(false);
		BrainTopicStructure.Prepared prepared = prepare(world, pool -> world.texts());
		List<Map<Integer, Integer>> groups = groups(world, prepared);
		assertEquals(3, groups.size(), groups.toString());
		Set<Integer> found = new HashSet<>();
		for (Map<Integer, Integer> group : groups) {
			// one piece of work per group, and most of its threads
			assertEquals(1, group.size(), groups.toString());
			assertTrue(group.values().iterator().next() >= 10, groups.toString());
			found.addAll(group.keySet());
		}
		assertEquals(Set.of(0, 1, 2), found);
		// whoever names a group is told what its mail keeps saying
		for (int g = 0; g < 3; g++) {
			int work = groups.get(g).keySet().iterator().next();
			List<?> words = (List<?>) prepared.cards().get(g).get("words");
			assertFalse(words.isEmpty());
			assertTrue(words.stream().allMatch(List.of(WORK[work])::contains), words.toString());
		}
		// the headers alone cannot tell these threads apart: one subject, the same people
		List<Map<Integer, Integer>> blind = groups(world, prepare(world, null));
		assertFalse(blind.size() == 3 && blind.stream().allMatch(group -> group.size() == 1), blind.toString());
	}

	@Test
	void aSignOffDoesNotGroupMailByWhoWroteIt() {
		World world = world(true);
		List<Map<Integer, Integer>> groups = groups(world, prepare(world, pool -> world.texts()));
		assertEquals(3, groups.size(), groups.toString());
		assertTrue(groups.stream().allMatch(group -> group.size() == 1), groups.toString());
	}

	@Test
	void theHeadersDecideWhenLittleOfThePoolCanBeRead() {
		World world = world(false);
		Map<String, List<BrainTopicStructure.Text>> few = new HashMap<>();
		world.texts().entrySet().stream().limit(10).forEach(e -> few.put(e.getKey(), e.getValue()));
		BrainTopicStructure.Prepared partly = prepare(world, pool -> few);
		BrainTopicStructure.Prepared blind = prepare(world, null);
		assertEquals(blind.filed(), partly.filed());
		assertTrue(partly.cards().stream().noneMatch(card -> card.containsKey("words")));
		// before a sort the mailbox is not read at all
		boolean[] asked = { false };
		BrainTopicStructure.prepare(world.threads(), ME, d -> d.equals("plant.example"), Set.of(), BrainTopicStructure.A1,
				Set.of(), Map.of(), pool -> {
					asked[0] = true;
					return world.texts();
				});
		assertFalse(asked[0]);
	}

	@Test
	void aGroupLargelyUnderTheOwnersTopicsIsNotProposedAgain() {
		Set<String> held = Set.of("a", "b", "c");
		// a quarter or more of it is already filed: it is a topic the owner has
		assertTrue(BrainTopicOnboarding.alreadyATopic(List.of("a", "b", "x", "y", "z", "w", "v", "u"), held));
		// new work: mail nothing holds yet, one stray filed thread among it
		assertFalse(BrainTopicOnboarding.alreadyATopic(List.of("a", "x", "y", "z", "w", "v", "u", "t"), held));
		assertFalse(BrainTopicOnboarding.alreadyATopic(List.of("x", "y", "z"), Set.of()));
	}

	@Test
	void theSamePoolAlwaysGivesTheSameGroups() {
		World world = world(false);
		assertEquals(prepare(world, pool -> world.texts()).filed(), prepare(world, pool -> world.texts()).filed());
		// two tight knots of threads tied by one weak link stay two groups
		double[][] sim = new double[8][8];
		for (int i = 0; i < 8; i++) {
			for (int j = 0; j < 8; j++) {
				sim[i][j] = i == j ? 1 : (i < 4) == (j < 4) ? 0.8 : 0;
			}
		}
		sim[3][4] = sim[4][3] = 0.1;
		assertEquals(List.of(List.of(0, 1, 2, 3), List.of(4, 5, 6, 7)), BrainTopicStructure.communities(sim, 3, 1.0, 0.4));
	}
}
