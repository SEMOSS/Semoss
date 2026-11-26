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

// Deterministic topic discovery; no model calls, database writes or embeddings. Before a sort it reads headers
// only. After one, when a Reader is given, it also reads what the pool's messages say, at call time and without
// keeping any of it: threads are then grouped by their wording, which is what tells apart work the same people do.
public final class BrainTopicStructure {

	// the words of one message as the thread read cleaned it (no quoted history, no footer), and who wrote it
	public record Text(String sender, String text) {
	}

	/** The pool's messages by thread id, oldest first; a thread it leaves out or cannot read has no text. */
	public interface Reader {
		Map<String, List<Text>> read(List<Facts> pool);
	}

	// reading: as much of each message as says what it is about
	private static final int TEXT_CHARS = 600;
	// reading: a word in more than this share of the pool's threads says nothing about any one topic
	// (and in more than thirty threads: in a small pool one topic can be a third of the mail)
	private static final double COMMON = 0.1;
	private static final int COMMON_LEAST = 30;
	// reading: a word a sender puts in at least this share of their messages (three or more) is their sign-off
	private static final double BOILERPLATE = 0.5;
	// reading: each thread is tied to this many threads most like it
	private static final int NEIGHBOURS = 10;
	// reading: above 1 prefers more and smaller groups; 2 keeps one topic's threads together on the fixture worlds
	private static final double RESOLUTION = 2.0;
	// reading: a group with more than this share of its ties going into one other group is joined to it
	private static final double JOINS = 1.0 / 3;
	// reading: a thread stays in its group when this share of its neighbours are in it too
	private static final double BELONGS = 0.4;
	// reading: the smallest group worth proposing, in a pool of 320 threads or more
	private static final int SMALLEST = 8;

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

	// folder: the Outlook folder the owner (or their rules) filed it in, only one they made; categories: theirs
	public record Message(String subject, String at, String sender, List<String> to, List<String> cc, boolean bulk,
			String folder, List<String> categories) {

		public Message(String subject, String at, String sender, List<String> to, List<String> cc, boolean bulk) {
			this(subject, at, sender, to, cc, bulk, null, List.of());
		}
	}

	public record MailThread(String id, List<Message> messages) {
	}

	public record Facts(String id, String subject, String at, List<String> people, Set<String> senders, boolean wrote,
			boolean bulk, boolean vip, boolean outside, int messages, boolean engaged, String folder,
			Set<String> categories) {
	}

	// Indices refer to pool entries and seed topics. filed is discovery membership,
	// never BRAIN_THREAD_TOPIC. folders: the owner's folder each topic is, null for a discovered one.
	public record Prepared(List<Facts> pool, List<List<Integer>> seeds, Map<Integer, Integer> filed,
			List<Map<String, Object>> cards, int gated, List<String> folders) {

		public Prepared(List<Facts> pool, List<List<Integer>> seeds, Map<Integer, Integer> filed,
				List<Map<String, Object>> cards, int gated) {
			this(pool, seeds, filed, cards, gated, java.util.Collections.nCopies(seeds.size(), null));
		}
	}

	private BrainTopicStructure() {
	}

	public static Prepared prepare(List<MailThread> input, String owner, Predicate<String> ownDomain, Set<String> vips,
			Settings settings) {
		return prepare(input, owner, ownDomain, vips, settings, Set.of());
	}

	static Prepared prepare(List<MailThread> input, String owner, Predicate<String> ownDomain, Set<String> vips,
			Settings settings, Set<String> sentHistory) {
		return prepare(input, owner, ownDomain, vips, settings, sentHistory, Map.of());
	}

	/**
	 * folderPaths: the owner's folders that may steer grouping, by id. A folder is evidence, not a topic: two threads
	 * in the same folder (or under the same categories) count as more alike only as far as their people and wording
	 * already are, so a catch-all folder still splits and a project folder stays together. A group most of whose
	 * threads sit in one folder carries that folder as a naming hint. Without folders this is the plain discovery.
	 */
	static Prepared prepare(List<MailThread> input, String owner, Predicate<String> ownDomain, Set<String> vips,
			Settings settings, Set<String> sentHistory, Map<String, String> folderPaths) {
		return prepare(input, owner, ownDomain, vips, settings, sentHistory, folderPaths, null);
	}

	/**
	 * reader: when given with wide settings, the pool is grouped by what its messages say ({@link #communities})
	 * instead of by subject wording and people. It is asked once, for the pool only, and its answer is not kept.
	 * When it reads less than half of the pool the headers decide, as without it.
	 */
	static Prepared prepare(List<MailThread> input, String owner, Predicate<String> ownDomain, Set<String> vips,
			Settings settings, Set<String> sentHistory, Map<String, String> folderPaths, Reader reader) {
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
			// the folder most of its mail is in, the newest on a tie
			Map<String, Integer> inFolder = new LinkedHashMap<>();
			Set<String> categories = new LinkedHashSet<>();
			for (Message m : messages.reversed()) {
				if (m.folder() != null) {
					inFolder.merge(m.folder(), 1, Integer::sum);
				}
				categories.addAll(m.categories() == null ? List.of() : m.categories());
			}
			String folder = inFolder.entrySet().stream().reduce((a, b) -> b.getValue() > a.getValue() ? b : a)
					.map(Map.Entry::getKey).orElse(null);
			pool.add(new Facts(thread.id(), subject(messages.getFirst().subject()), messages.getLast().at(),
					new ArrayList<>(people), senders, wrote, bulk, vip, outside, messages.size(), engaged, folder,
					categories));
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
		Words read = settings.wide() && reader != null ? words(pool, reader.read(pool)) : null;
		// Without a reading, similarity balances subject wording and participants, with less weight across
		// distant dates.
		double[][] text = read != null ? null : tfidf(pool.stream().map(t -> words(t.subject())).toList(), 0.15);
		double[][] people = read != null ? null : tfidf(pool.stream().map(Facts::people).toList(), 0.1);
		double[][] sim = new double[pool.size()][pool.size()];
		// a folder holding much of the pool says little (a catch-all); one holding a few threads says a lot
		Map<String, Integer> inFolder = new HashMap<>();
		pool.forEach(t -> {
			if (t.folder() != null) {
				inFolder.merge(t.folder(), 1, Integer::sum);
			}
		});
		for (int i = 0; i < pool.size(); i++) {
			for (int j = 0; j <= i; j++) {
				double base;
				if (read != null) {
					base = read.sim()[i][j];
				} else {
					double days = Math.abs(day(pool.get(i).at()) - day(pool.get(j).at()));
					base = (0.5 * dot(text[i], text[j]) + 0.5 * dot(people[i], people[j]))
							* (0.5 + 0.5 * Math.exp(-days / 60));
				}
				// the owner's own grouping (same folder, shared categories) strengthens what is already there
				String folder = pool.get(i).folder();
				double guide = (folder != null && folder.equals(pool.get(j).folder())
						? Math.max(0, 1 - 2.0 * inFolder.get(folder) / pool.size()) : 0)
						+ 0.5 * shared(pool.get(i).categories(), pool.get(j).categories());
				sim[i][j] = sim[j][i] = Math.min(1, base * (1 + guide));
			}
		}
		List<List<Integer>> merged = new ArrayList<>();
		Map<Integer, Integer> filed = new LinkedHashMap<>();
		if (read != null) {
			// a group is a set of threads that are each other's nearest; nothing is added to it afterwards, so a
			// thread that only resembles a group is left for the classifier to file
			int smallest = Math.max(3, Math.min(SMALLEST, pool.size() / 40));
			for (List<Integer> group : communities(sim, NEIGHBOURS, RESOLUTION, BELONGS)) {
				List<Integer> kept = group.stream().filter(j -> !ranked.get(j).bulk()).toList();
				if (kept.size() >= smallest) {
					merged.add(kept);
				}
			}
			merged.sort(Comparator.<List<Integer>>comparingInt(List::size).reversed());
			merged = new ArrayList<>(merged.subList(0, Math.min(settings.topics(), merged.size())));
			if (merged.isEmpty()) {
				return new Prepared(pool, List.of(), Map.of(), List.of(), gated);
			}
			for (int i = 0; i < merged.size(); i++) {
				for (int j : merged.get(i)) {
					filed.put(j, i);
				}
			}
		} else {
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
			double[][] centroids = new double[seeds.size()][];
			for (int i = 0; i < seeds.size(); i++) {
				centroids[i] = centroid(seeds.get(i), people);
			}
			double[][] mergeSim = new double[seeds.size()][seeds.size()];
			for (int i = 0; i < seeds.size(); i++) {
				for (int j = 0; j <= i; j++) {
					mergeSim[i][j] = mergeSim[j][i] = dot(centroids[i], centroids[j]);
				}
			}
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
		}
		List<Map<String, Object>> cards = cards(pool, merged, sim);
		List<String> folders = new ArrayList<>();
		for (int i = 0; i < cards.size(); i++) {
			if (read != null) {
				// what the group's messages keep saying that the rest of the pool does not, for whoever names it
				List<String> said = read.telling(merged.get(i), 8);
				if (!said.isEmpty()) {
					cards.get(i).put("words", said);
				}
			}
			// the folder whose mail is mostly this group's, as the owner named it
			String folder = groupFolder(pool, merged.get(i), inFolder);
			folders.add(folder);
			if (folder != null) {
				cards.get(i).put("outlookFolder", folderPaths.get(folder));
			}
			Map<String, Integer> categories = new HashMap<>();
			for (int j : merged.get(i)) {
				pool.get(j).categories().forEach(c -> categories.merge(c, 1, Integer::sum));
			}
			List<String> common = categories.entrySet().stream().filter(e -> e.getValue() >= 2)
					.sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(3).map(Map.Entry::getKey)
					.toList();
			if (!common.isEmpty()) {
				cards.get(i).put("outlookCategories", common);
			}
		}
		return new Prepared(pool, merged, filed, cards, gated, folders);
	}

	// the folder with at least 60% of its pool threads (two or more) in the group, making up a third of it or more;
	// a folder spread over several groups (a catch-all, a broad one) is no group's own
	private static String groupFolder(List<Facts> pool, List<Integer> group, Map<String, Integer> inFolder) {
		Map<String, Integer> counts = new HashMap<>();
		for (int j : group) {
			if (pool.get(j).folder() != null) {
				counts.merge(pool.get(j).folder(), 1, Integer::sum);
			}
		}
		return counts.entrySet().stream()
				.filter(e -> e.getValue() >= 2 && e.getValue() >= 0.6 * inFolder.get(e.getKey())
						&& 3 * e.getValue() >= group.size())
				.max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse(null);
	}

	// What each pool thread says, weighed so that words few threads share count most, and how alike two threads are
	// by it (cosine). docs: each thread's kept words with their weights, for naming a group afterwards.
	private record Words(double[][] sim, List<Map<String, Double>> docs, Map<String, Integer> df) {

		// the words most of a group uses and little of the pool does
		List<String> telling(List<Integer> group, int limit) {
			Map<String, Integer> in = new HashMap<>();
			for (int j : group) {
				docs.get(j).keySet().forEach(w -> in.merge(w, 1, Integer::sum));
			}
			int least = Math.max(2, (int) Math.ceil(group.size() * 0.2));
			return in.entrySet().stream().filter(e -> e.getValue() >= least && e.getKey().length() >= 3)
					.sorted(Comparator.<Map.Entry<String, Integer>>comparingDouble(e -> -e.getValue()
							* Math.log((1.0 + docs.size()) / (1 + df.get(e.getKey())))).thenComparing(Map.Entry::getKey))
					.limit(limit).map(Map.Entry::getKey).toList();
		}
	}

	// null when less than half of the pool could be read: the headers then decide
	private static Words words(List<Facts> pool, Map<String, List<Text>> texts) {
		if (texts == null || 2 * pool.stream().filter(t -> !texts.getOrDefault(t.id(), List.of()).isEmpty()).count() < pool
				.size()) {
			return null;
		}
		// each message's words, and the ones its sender puts in most of what they write: a sign-off, a title, a
		// disclaimer. Those say who wrote, not what about, and would group mail by sender
		Map<String, Integer> wrote = new HashMap<>();
		Map<String, Map<String, Integer>> uses = new HashMap<>();
		List<List<List<String>>> said = new ArrayList<>();
		for (Facts t : pool) {
			List<List<String>> messages = new ArrayList<>();
			for (Text m : texts.getOrDefault(t.id(), List.of())) {
				String text = m.text() == null ? "" : m.text();
				List<String> tokens = plain(text.substring(0, Math.min(TEXT_CHARS, text.length())));
				messages.add(tokens);
				String sender = m.sender() == null ? "" : m.sender();
				wrote.merge(sender, 1, Integer::sum);
				Map<String, Integer> theirs = uses.computeIfAbsent(sender, k -> new HashMap<>());
				new HashSet<>(tokens).forEach(w -> theirs.merge(w, 1, Integer::sum));
			}
			said.add(messages);
		}
		List<Map<String, Integer>> counts = new ArrayList<>();
		Map<String, Integer> df = new TreeMap<>();
		for (int i = 0; i < pool.size(); i++) {
			Map<String, Integer> count = new HashMap<>();
			plain(pool.get(i).subject()).forEach(w -> count.merge(w, 1, Integer::sum));
			List<Text> messages = texts.getOrDefault(pool.get(i).id(), List.of());
			for (int k = 0; k < messages.size(); k++) {
				String sender = messages.get(k).sender() == null ? "" : messages.get(k).sender();
				int theirs = wrote.get(sender);
				Map<String, Integer> habit = uses.get(sender);
				for (String w : said.get(i).get(k)) {
					if (theirs < 3 || habit.get(w) < BOILERPLATE * theirs) {
						count.merge(w, 1, Integer::sum);
					}
				}
			}
			counts.add(count);
		}
		int size = pool.size();
		for (Map<String, Integer> count : counts) {
			count.keySet().forEach(w -> df.merge(w, 1, Integer::sum));
		}
		// mail in another language than most of the pool still groups by that language: its common words are too
		// few in the pool to be dropped here
		df.entrySet().removeIf(e -> e.getValue() < 2 || e.getValue() > Math.max(COMMON_LEAST, COMMON * size));
		List<Map<String, Double>> docs = new ArrayList<>();
		Map<String, List<Integer>> holders = new TreeMap<>();
		for (int i = 0; i < size; i++) {
			Map<String, Double> doc = new TreeMap<>();
			double norm = 0;
			for (Map.Entry<String, Integer> e : counts.get(i).entrySet()) {
				Integer in = df.get(e.getKey());
				if (in != null) {
					double weight = (1 + Math.log(e.getValue())) * (1 + Math.log((1.0 + size) / (1 + in)));
					doc.put(e.getKey(), weight);
					norm += weight * weight;
				}
			}
			final double length = Math.sqrt(norm);
			final int at = i;
			doc.replaceAll((w, weight) -> weight / length);
			doc.keySet().forEach(w -> holders.computeIfAbsent(w, k -> new ArrayList<>()).add(at));
			docs.add(doc);
		}
		// only threads that share a word are alike at all, so go word by word instead of pair by pair
		double[][] sim = new double[size][size];
		holders.forEach((w, at) -> {
			for (int a = 0; a < at.size(); a++) {
				double left = docs.get(at.get(a)).get(w);
				for (int b = 0; b <= a; b++) {
					double add = left * docs.get(at.get(b)).get(w);
					sim[at.get(a)][at.get(b)] += add;
					if (a != b) {
						sim[at.get(b)][at.get(a)] += add;
					}
				}
			}
		});
		return new Words(sim, docs, df);
	}

	// single words, lowercased; numbers alone tell nothing
	private static List<String> plain(String text) {
		List<String> out = new ArrayList<>();
		var matcher = WORD.matcher(text.toLowerCase(Locale.ROOT));
		while (matcher.find()) {
			String token = matcher.group();
			if (!token.chars().allMatch(Character::isDigit)) {
				out.add(token);
			}
		}
		return out;
	}

	/**
	 * Groups by who is nearest to whom. Every thread is tied to the k threads most like it; the groups are the
	 * communities of that graph, found the Louvain way: move each thread to the neighbouring group that gains most,
	 * then treat every group as one node and repeat. Threads are taken in pool order, so the same pool always gives
	 * the same groups. Two groups that are mostly each other's neighbours are joined, and a thread stays in its
	 * group only when at least `belongs` of its own neighbours are there.
	 */
	static List<List<Integer>> communities(double[][] sim, int k, double resolution, double belongs) {
		int n = sim.length;
		List<List<Integer>> nearest = new ArrayList<>();
		List<Map<Integer, Double>> graph = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			graph.add(new LinkedHashMap<>());
		}
		for (int i = 0; i < n; i++) {
			final int at = i;
			List<Integer> order = new ArrayList<>();
			for (int j = 0; j < n; j++) {
				if (j != i && sim[i][j] > 0) {
					order.add(j);
				}
			}
			order.sort(Comparator.<Integer>comparingDouble(j -> -sim[at][j]).thenComparing(Comparator.naturalOrder()));
			List<Integer> mine = new ArrayList<>(order.subList(0, Math.min(k, order.size())));
			nearest.add(mine);
			for (int j : mine) {
				graph.get(i).put(j, sim[i][j]);
				graph.get(j).put(i, sim[i][j]);
			}
		}
		int[] label = new int[n];
		for (int i = 0; i < n; i++) {
			label[i] = i;
		}
		while (true) {
			int size = graph.size();
			double[] degree = new double[size];
			double total = 0;
			for (int i = 0; i < size; i++) {
				for (Map.Entry<Integer, Double> e : graph.get(i).entrySet()) {
					// a node's tie to itself holds the ties inside the group it stands for, and counts from both ends
					degree[i] += e.getKey() == i ? 2 * e.getValue() : e.getValue();
				}
				total += degree[i];
			}
			double m = total / 2;
			if (m <= 0) {
				break;
			}
			int[] group = new int[size];
			double[] weight = degree.clone();
			for (int i = 0; i < size; i++) {
				group[i] = i;
			}
			boolean moved = false;
			boolean again = true;
			while (again) {
				again = false;
				for (int i = 0; i < size; i++) {
					Map<Integer, Double> ties = new LinkedHashMap<>();
					for (Map.Entry<Integer, Double> e : graph.get(i).entrySet()) {
						if (e.getKey() != i) {
							ties.merge(group[e.getKey()], e.getValue(), Double::sum);
						}
					}
					int from = group[i];
					weight[from] -= degree[i];
					int to = from;
					double best = ties.getOrDefault(from, 0.0) / m - resolution * weight[from] * degree[i] / (2 * m * m);
					for (Map.Entry<Integer, Double> e : ties.entrySet()) {
						double gain = e.getValue() / m - resolution * weight[e.getKey()] * degree[i] / (2 * m * m);
						if (gain > best + 1e-12) {
							best = gain;
							to = e.getKey();
						}
					}
					weight[to] += degree[i];
					if (to != from) {
						group[i] = to;
						again = true;
						moved = true;
					}
				}
			}
			if (!moved) {
				break;
			}
			Map<Integer, Integer> number = new LinkedHashMap<>();
			for (int i = 0; i < size; i++) {
				number.putIfAbsent(group[i], number.size());
			}
			for (int i = 0; i < n; i++) {
				label[i] = number.get(group[label[i]]);
			}
			List<Map<Integer, Double>> next = new ArrayList<>();
			for (int i = 0; i < number.size(); i++) {
				next.add(new LinkedHashMap<>());
			}
			for (int i = 0; i < size; i++) {
				int a = number.get(group[i]);
				for (Map.Entry<Integer, Double> e : graph.get(i).entrySet()) {
					int b = number.get(group[e.getKey()]);
					// a tie between two nodes is listed at both, so half of it from each end when it falls inside
					next.get(a).merge(b, a == b && e.getKey() != i ? e.getValue() / 2 : e.getValue(), Double::sum);
				}
			}
			graph = next;
		}
		// In a small pool one piece of work can be a large share of the mail, and the step above then cuts it in
		// two. A group that sends a third of its ties into one other group is part of it: join them, the most
		// tied pair first
		while (true) {
			Map<Integer, Integer> ties = new HashMap<>();
			Map<Long, Integer> across = new TreeMap<>();
			for (int i = 0; i < n; i++) {
				for (int j : nearest.get(i)) {
					ties.merge(label[i], 1, Integer::sum);
					if (label[j] != label[i]) {
						across.merge((long) label[i] * n + label[j], 1, Integer::sum);
					}
				}
			}
			long pair = -1;
			double most = JOINS;
			for (Map.Entry<Long, Integer> e : across.entrySet()) {
				double share = (double) e.getValue() / ties.get((int) (e.getKey() / n));
				if (share > most) {
					most = share;
					pair = e.getKey();
				}
			}
			if (pair < 0) {
				break;
			}
			int from = (int) (pair / n);
			int into = (int) (pair % n);
			for (int i = 0; i < n; i++) {
				if (label[i] == from) {
					label[i] = into;
				}
			}
		}
		Map<Integer, List<Integer>> groups = new TreeMap<>();
		for (int i = 0; i < n; i++) {
			final int mine = label[i];
			long there = nearest.get(i).stream().filter(j -> label[j] == mine).count();
			if (!nearest.get(i).isEmpty() && there >= belongs * nearest.get(i).size()) {
				groups.computeIfAbsent(mine, g -> new ArrayList<>()).add(i);
			}
		}
		return new ArrayList<>(groups.values());
	}

	private static double[] centroid(List<Integer> members, double[][] vectors) {
		double[] out = new double[vectors.length == 0 ? 0 : vectors[0].length];
		for (int j : members) {
			for (int k = 0; k < out.length; k++) {
				out[k] += vectors[j][k];
			}
		}
		normalize(out);
		return out;
	}

	// the share of categories two threads have in common (Jaccard); 0 when either has none
	private static double shared(Set<String> a, Set<String> b) {
		if (a.isEmpty() || b.isEmpty()) {
			return 0;
		}
		int both = 0;
		for (String c : a) {
			both += b.contains(c) ? 1 : 0;
		}
		return (double) both / (a.size() + b.size() - both);
	}

	/** Distinctive subject words and phrases of one group, offered to the owner as optional clues. */
	public static List<String> terms(Prepared prepared, int topic, int limit) {
		List<Integer> members = prepared.filed().entrySet().stream().filter(e -> e.getValue() == topic)
				.map(Map.Entry::getKey).toList();
		if (members.size() < 2) {
			return List.of();
		}
		Map<String, Integer> poolDf = new HashMap<>();
		for (Facts t : prepared.pool()) {
			new HashSet<>(words(subject(t.subject()))).forEach(token -> poolDf.merge(token, 1, Integer::sum));
		}
		Map<String, Integer> groupDf = new HashMap<>();
		for (int j : members) {
			new HashSet<>(words(subject(prepared.pool().get(j).subject()))).forEach(token -> groupDf.merge(token, 1, Integer::sum));
		}
		int least = Math.max(2, (int) Math.ceil(members.size() * 0.2));
		int size = prepared.pool().size();
		List<Map.Entry<String, Double>> scored = new ArrayList<>();
		groupDf.forEach((token, df) -> {
			// digits and very short words are rarely a useful clue, and neither is a word much of the pool's
			// subjects use ("the", "for", "update")
			if (df < least || token.length() < 3 || token.chars().allMatch(c -> Character.isDigit(c) || c == ' ')
					|| poolDf.getOrDefault(token, 0) > Math.max(COMMON_LEAST, 0.15 * size)) {
				return;
			}
			double coverage = (double) df / members.size();
			double idf = Math.log((1.0 + size) / (1 + poolDf.getOrDefault(token, 0)));
			scored.add(Map.entry(token, coverage * idf * (token.contains(" ") ? 1.3 : 1.0)));
		});
		scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());
		List<String> out = new ArrayList<>();
		for (Map.Entry<String, Double> entry : scored) {
			String token = entry.getKey();
			// a phrase and its own words are one clue
			if (out.stream().anyMatch(kept -> kept.contains(token) || token.contains(kept))) {
				continue;
			}
			out.add(token);
			if (out.size() >= limit) {
				break;
			}
		}
		return out;
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
