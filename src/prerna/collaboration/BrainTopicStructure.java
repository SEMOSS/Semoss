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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;

// Deterministic topic discovery over permitted headers; no model calls, bodies, database writes or embeddings.
public final class BrainTopicStructure {

	// wide: any real (non-bulk) thread can seed or join a topic, not only threads
	// the owner took part in; for a pool
	// the classifier has already cleared of automated mail
	public record Settings(int pool, int seeds, int topics, double mergeCut, double floor, int votes, int dropAt,
			int attempts, int spareVotes, int maxTokens, boolean wide) {
		public Settings {
			if (pool < 1 || pool > 1000 || seeds < 1 || topics < 1 || topics > 40 || votes < 1 || votes > 9
					|| dropAt < 1 || dropAt > votes || attempts < 1 || attempts > 2 || spareVotes < 0 || spareVotes > 1
					|| mergeCut < 0 || mergeCut > 1 || floor < 0 || floor > 1 || maxTokens < 1 || maxTokens > 8000) {
				throw new IllegalArgumentException("Invalid topic onboarding settings");
			}
		}
	}

	// Before a completed sort, require owner engagement; after sort, WIDE trusts
	// the non-automated pool.
	public static final Settings A1 = new Settings(300, 40, 25, 0.6, 0.1, 3, 2, 2, 1, 4000, false);
	// after a sort: every real thread, and a longer list for the owner to narrow
	// down
	public static final Settings WIDE = new Settings(1000, 60, 40, 0.6, 0.1, 3, 2, 2, 1, 6000, true);
	private static final Pattern PREFIX = Pattern.compile("^((\\[EXTERNAL\\] )?(RE|FW|Fwd): )*",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern WORD = Pattern.compile("(?U)\\b\\w\\w+\\b");

	public record Message(String subject, String at, String sender, List<String> to, List<String> cc, boolean bulk) {
	}

	public record MailThread(String id, List<Message> messages) {
	}

	public record Facts(String id, String subject, String at, List<String> people, Set<String> senders, boolean wrote,
			boolean bulk, boolean vip, boolean outside, int messages, boolean engaged) {
	}

	// Indices refer to pool entries and seed topics. filed is discovery membership,
	// never BRAIN_THREAD_TOPIC.
	public record Prepared(List<Facts> pool, List<List<Integer>> seeds, Map<Integer, Integer> filed,
			List<Map<String, Object>> cards, int gated) {
	}

	private BrainTopicStructure() {
	}

	public static Prepared prepare(List<MailThread> input, String owner, Predicate<String> ownDomain, Set<String> vips,
			Settings settings) {
		return prepare(input, owner, ownDomain, vips, settings, Set.of());
	}

	static Prepared prepare(List<MailThread> input, String owner, Predicate<String> ownDomain, Set<String> vips,
			Settings settings, Set<String> sentHistory) {
		// Engagement uses all permitted Sent contacts, not just the threads that
		// survive the pool limit.
		Set<String> sentTo = new HashSet<>(sentHistory);
		for (MailThread thread : input) {
			for (Message m : thread.messages()) {
				if (owner.equals(m.sender())) {
					sentTo.addAll(m.to());
					sentTo.addAll(m.cc());
				}
			}
		}
		List<Facts> pool = new ArrayList<>();
		for (MailThread thread : input) {
			List<Message> messages = new ArrayList<>(thread.messages());
			messages.sort(Comparator.comparing(Message::at));
			if (messages.isEmpty()) {
				continue;
			}
			Set<String> people = new LinkedHashSet<>();
			Set<String> senders = new LinkedHashSet<>();
			for (Message m : messages) {
				people.add(m.sender());
				people.addAll(m.to());
				people.addAll(m.cc());
				senders.add(m.sender());
			}
			boolean wrote = senders.contains(owner);
			boolean bulk = messages.stream().anyMatch(Message::bulk);
			boolean engaged = !bulk && (wrote || senders.stream().anyMatch(sentTo::contains));
			boolean outside = people.stream().anyMatch(p -> !ownDomain.test(domain(p)));
			boolean vip = people.stream().anyMatch(vips::contains);
			people.remove(owner);
			pool.add(new Facts(thread.id(), subject(messages.getFirst().subject()), messages.getLast().at(),
					new ArrayList<>(people), senders, wrote, bulk, vip, outside, messages.size(), engaged));
		}
		pool.sort(Comparator.comparing(Facts::bulk)
				.thenComparing(Comparator.comparingInt(BrainTopicStructure::rank).reversed())
				.thenComparing(Comparator.comparingInt(Facts::messages).reversed())
				.thenComparing(Comparator.comparing(Facts::at).reversed()));
		pool = new ArrayList<>(pool.subList(0, Math.min(settings.pool(), pool.size())));
		final List<Facts> ranked = pool;
		int gated = (int) pool.stream().filter(t -> !t.engaged()).count();
		if (pool.size() < 3) {
			return new Prepared(pool, List.of(), Map.of(), List.of(), gated);
		}
		// Similarity balances subject wording and participants, with less weight across
		// distant dates.
		List<List<String>> words = pool.stream().map(t -> words(t.subject())).toList();
		double[][] text = tfidf(words, 0.15);
		double[][] people = tfidf(pool.stream().map(Facts::people).toList(), 0.1);
		double[][] sim = new double[pool.size()][pool.size()];
		for (int i = 0; i < pool.size(); i++) {
			for (int j = 0; j <= i; j++) {
				double days = Math.abs(day(pool.get(i).at()) - day(pool.get(j).at()));
				sim[i][j] = sim[j][i] = (0.5 * dot(text[i], text[j]) + 0.5 * dot(people[i], people[j]))
						* (0.5 + 0.5 * Math.exp(-days / 60));
			}
		}
		// Seed coherent groups first; only then apply engagement/non-bulk eligibility
		// and the seed cap.
		List<List<Integer>> groups = linkage(sim, 0.9);
		groups.removeIf(g -> g.size() < 3);
		groups.sort(Comparator.<List<Integer>>comparingInt(List::size).reversed());
		List<List<Integer>> seeds = new ArrayList<>();
		for (List<Integer> group : groups) {
			List<Integer> kept = group.stream()
					.filter(j -> settings.wide() ? !ranked.get(j).bulk() : ranked.get(j).engaged()).toList();
			if (kept.size() >= 3 && seeds.size() < settings.seeds()) {
				seeds.add(kept);
			}
		}
		if (seeds.isEmpty()) {
			return new Prepared(pool, List.of(), Map.of(), List.of(), gated);
		}
		// Merge seeds by their normalized participant centroid. mergeCut is distance (1
		// - cosine), not similarity.
		double[][] centroids = new double[seeds.size()][people[0].length];
		for (int i = 0; i < seeds.size(); i++) {
			for (int j : seeds.get(i)) {
				for (int k = 0; k < centroids[i].length; k++) {
					centroids[i][k] += people[j][k];
				}
			}
			normalize(centroids[i]);
		}
		double[][] mergeSim = new double[seeds.size()][seeds.size()];
		for (int i = 0; i < seeds.size(); i++) {
			for (int j = 0; j <= i; j++) {
				mergeSim[i][j] = mergeSim[j][i] = dot(centroids[i], centroids[j]);
			}
		}
		List<List<Integer>> merged = new ArrayList<>();
		for (List<Integer> group : linkage(mergeSim, settings.mergeCut())) {
			List<Integer> members = new ArrayList<>();
			for (int i : group) {
				members.addAll(seeds.get(i));
			}
			merged.add(members);
		}
		merged.sort(Comparator.<List<Integer>>comparingInt(List::size).reversed());
		merged = new ArrayList<>(merged.subList(0, Math.min(settings.topics(), merged.size())));
		// Extend discovery groups using each remaining thread's three closest seed
		// members; cards stay seed-only.
		Map<Integer, Integer> filed = new LinkedHashMap<>();
		for (int i = 0; i < merged.size(); i++) {
			for (int j : merged.get(i)) {
				filed.put(j, i);
			}
		}
		for (int j = 0; j < pool.size(); j++) {
			if (filed.containsKey(j) || (settings.wide() ? pool.get(j).bulk() : !pool.get(j).engaged())) {
				continue;
			}
			int best = -1;
			double score = -1;
			for (int i = 0; i < merged.size(); i++) {
				final int at = j;
				double[] scores = merged.get(i).stream().mapToDouble(k -> sim[at][k]).sorted().toArray();
				double mean = 0;
				for (int k = 0; k < Math.min(3, scores.length); k++) {
					mean += scores[scores.length - 1 - k];
				}
				mean /= Math.min(3, scores.length);
				if (mean >= score) {
					score = mean;
					best = i;
				}
			}
			if (score >= settings.floor()) {
				filed.put(j, best);
			}
		}
		return new Prepared(pool, merged, filed, cards(pool, merged, sim), gated);
	}

	private static int rank(Facts t) {
		return (t.wrote() ? 1 : 0) + (t.senders().size() >= 2 ? 1 : 0) + (t.vip() ? 1 : 0) + (t.outside() ? 1 : 0);
	}

	static String domain(String address) {
		int at = address.lastIndexOf('@');
		return at < 0 ? "" : address.substring(at + 1);
	}

	private static String subject(String subject) {
		return PREFIX.matcher(subject == null ? "" : subject).replaceFirst("").trim();
	}

	private static double day(String at) {
		return Instant.parse(at).toEpochMilli() / 86400000.0;
	}

	private static List<String> words(String subject) {
		List<String> tokens = new ArrayList<>();
		var matcher = WORD.matcher(subject.toLowerCase(Locale.ROOT));
		while (matcher.find()) {
			tokens.add(matcher.group());
		}
		List<String> out = new ArrayList<>(tokens);
		for (int i = 1; i < tokens.size(); i++) {
			out.add(tokens.get(i - 1) + " " + tokens.get(i));
		}
		return out;
	}

	private static double[][] tfidf(List<List<String>> docs, double maxDf) {
		Map<String, Integer> df = new TreeMap<>();
		for (List<String> doc : docs) {
			for (String token : new HashSet<>(doc)) {
				df.merge(token, 1, Integer::sum);
			}
		}
		df.entrySet().removeIf(e -> e.getValue() < 2 || e.getValue() > maxDf * docs.size());
		List<String> vocabulary = new ArrayList<>(df.keySet());
		double[][] matrix = new double[docs.size()][vocabulary.size()];
		for (int i = 0; i < docs.size(); i++) {
			Map<String, Integer> counts = new HashMap<>();
			for (String token : docs.get(i)) {
				counts.merge(token, 1, Integer::sum);
			}
			for (int j = 0; j < vocabulary.size(); j++) {
				String token = vocabulary.get(j);
				int count = counts.getOrDefault(token, 0);
				if (count > 0) {
					matrix[i][j] = (1 + Math.log(count)) * (1 + Math.log((1.0 + docs.size()) / (1 + df.get(token))));
				}
			}
			normalize(matrix[i]);
		}
		return matrix;
	}

	private static double dot(double[] a, double[] b) {
		double sum = 0;
		for (int i = 0; i < a.length; i++) {
			sum += a[i] * b[i];
		}
		return sum;
	}

	private static void normalize(double[] row) {
		double norm = Math.sqrt(dot(row, row));
		if (norm > 0) {
			for (int i = 0; i < row.length; i++) {
				row[i] /= norm;
			}
		}
	}

	// Average linkage by nearest-neighbour chains, with input order resolving equal
	// distances.
	private static List<List<Integer>> linkage(double[][] sim, double cut) {
		int n = sim.length;
		double[][] distance = new double[n][n];
		int[] size = new int[n];
		List<List<Integer>> members = new ArrayList<>();
		List<List<Integer>> result = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			size[i] = 1;
			members.add(new ArrayList<>(List.of(i)));
			for (int j = 0; j < n; j++) {
				distance[i][j] = Math.max(0, 1 - sim[i][j]);
			}
		}
		List<Integer> chain = new ArrayList<>();
		for (int step = 0; step < n - 1; step++) {
			if (chain.isEmpty()) {
				for (int i = 0; i < n; i++) {
					if (size[i] > 0) {
						chain.add(i);
						break;
					}
				}
			}
			int x;
			int y;
			double minimum;
			while (true) {
				x = chain.getLast();
				y = chain.size() > 1 ? chain.get(chain.size() - 2) : -1;
				minimum = y < 0 ? Double.POSITIVE_INFINITY : distance[x][y];
				for (int i = 0; i < n; i++) {
					if (size[i] > 0 && i != x && distance[x][i] < minimum) {
						minimum = distance[x][i];
						y = i;
					}
				}
				if (chain.size() > 1 && y == chain.get(chain.size() - 2)) {
					break;
				}
				chain.add(y);
			}
			chain.removeLast();
			chain.removeLast();
			if (x > y) {
				int swap = x;
				x = y;
				y = swap;
			}
			int nx = size[x];
			int ny = size[y];
			if (minimum > cut) {
				if (!members.get(x).isEmpty()) {
					result.add(members.get(x));
				}
				if (!members.get(y).isEmpty()) {
					result.add(members.get(y));
				}
				members.set(y, new ArrayList<>());
			} else {
				members.get(y).addAll(members.get(x));
			}
			members.set(x, new ArrayList<>());
			for (int i = 0; i < n; i++) {
				if (size[i] > 0 && i != x && i != y) {
					distance[i][y] = distance[y][i] = (nx * distance[i][x] + ny * distance[i][y]) / (nx + ny);
				}
			}
			size[x] = 0;
			size[y] = nx + ny;
		}
		for (List<Integer> group : members) {
			if (!group.isEmpty()) {
				result.add(group);
			}
		}
		for (List<Integer> group : result) {
			group.sort(Integer::compareTo);
		}
		result.sort(Comparator.comparingInt(g -> g.getFirst()));
		return result;
	}

	private static List<Map<String, Object>> cards(List<Facts> pool, List<List<Integer>> seeds, double[][] sim) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (List<Integer> group : seeds) {
			Map<String, Integer> counts = new LinkedHashMap<>();
			List<String> months = new ArrayList<>();
			Map<Integer, Double> central = new HashMap<>();
			for (int j : group) {
				Facts t = pool.get(j);
				months.add(t.at().substring(0, 7));
				for (String person : t.people()) {
					counts.merge(person, 1, Integer::sum);
				}
				central.put(j, group.stream().mapToDouble(k -> sim[j][k]).average().orElse(0));
			}
			months.sort(String::compareTo);
			List<Integer> order = new ArrayList<>(group);
			order.sort(Comparator.<Integer>comparingDouble(central::get).reversed());
			Set<String> subjects = new LinkedHashSet<>();
			for (int j : order.subList(0, Math.min(12, order.size()))) {
				String subject = pool.get(j).subject();
				subjects.add(subject.substring(0, Math.min(70, subject.length())));
			}
			Map<String, Object> card = new LinkedHashMap<>();
			card.put("id", "k" + (out.size() + 1));
			card.put("threads", group.size());
			card.put("from", months.getFirst());
			card.put("to", months.getLast());
			card.put("people",
					counts.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(3)
							.map(Map.Entry::getKey).toList());
			card.put("subjects", subjects.stream().limit(6).toList());
			out.add(card);
		}
		return out;
	}
}
