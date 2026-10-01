package prerna.collaboration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

// A.1's model contract is separate from model transport and persistence.
public final class BrainTopicVotes {

	public static final String INSTRUCTIONS = """
			You help one person set up their work topics from their email. Below are clusters of their email threads,
			found from who is on each thread, subject wording and dates. Each cluster has an id, its thread count, first and last
			month, the people who appear most, and example subjects.

			For every cluster, say whether it is the owner's own work: "work" when it is a project, client, deal, team or piece of
			work they take part in; "not_work" when it is personal (family, friends, shopping, social plans, personal finances),
			automated or bulk (notifications, receipts, newsletters, marketing, alerts), or company-wide news and announcements
			that need nothing from them; "unclear" when you cannot tell. Then name it the way a colleague would, in 2 to 5 words,
			after the project, client or piece of work (for not_work, say what kind of mail it is). Do not name a topic after a
			person or a date. Also write "about": one plain sentence, under 25 words, saying what mail belongs in this topic
			(the project, client or work it covers and what it involves), so someone could sort a new email into it.

			Answer for every cluster id given, once each.
			""";

	@FunctionalInterface
	public interface Caller {
		String ask(String prompt, String instructions, Map<String, Object> parameters);
	}

	public record Vote(String kind, String name, String about) {
	}

	public record Result(String status, List<Map<String, Vote>> votes, Set<Integer> dropped,
			Map<Integer, String> names, Map<Integer, String> abouts, int calls, int failedVotes) {
	}

	private BrainTopicVotes() {
	}

	public static Map<String, Object> schema(List<String> ids) {
		Map<String, Object> item = Map.of("type", "object", "additionalProperties", false,
				"required", List.of("id", "kind", "name", "about"), "properties", Map.of(
						"id", Map.of("type", "string", "enum", ids),
						"kind", Map.of("type", "string", "enum", List.of("work", "not_work", "unclear")),
						"name", Map.of("type", "string"),
						"about", Map.of("type", "string")));
		return Map.of("type", "object", "additionalProperties", false, "required", List.of("clusters"),
				"properties", Map.of("clusters", Map.of("type", "array", "minItems", ids.size(),
						"maxItems", ids.size(), "items", item)));
	}

	public static Result run(BrainTopicStructure.Prepared prepared, BrainTopicStructure.Settings settings, Caller caller) {
		if (prepared.cards().isEmpty()) {
			return new Result("no_topics", List.of(), Set.of(), Map.of(), Map.of(), 0, 0);
		}
		List<String> ids = prepared.cards().stream().map(c -> (String) c.get("id")).toList();
		String prompt = new com.google.gson.Gson().toJson(Map.of("clusters", prepared.cards()));
		Map<String, Object> parameters = Map.of("temperature", 0, "schema", schema(ids), "max_tokens", settings.maxTokens());
		List<Map<String, Vote>> votes = new ArrayList<>();
		int calls = 0;
		int failed = 0;
		for (int slot = 0; slot < settings.votes() + settings.spareVotes() && votes.size() < settings.votes(); slot++) {
			Map<String, Vote> vote = null;
			for (int attempt = 0; attempt < settings.attempts() && vote == null; attempt++) {
				calls++;
				try {
					vote = validate(caller.ask(prompt, INSTRUCTIONS, parameters), ids);
				} catch (RuntimeException e) {
					// A failed call consumes its budget; private prompts and replies are never logged here.
				}
			}
			if (vote == null) {
				failed++;
			} else {
				votes.add(vote);
			}
		}
		if (votes.size() != settings.votes()) {
			return new Result("vote_failed", votes, Set.of(), Map.of(), Map.of(), calls, failed);
		}
		Set<Integer> dropped = new HashSet<>();
		Map<Integer, String> names = new LinkedHashMap<>();
		Map<Integer, String> abouts = new LinkedHashMap<>();
		for (int i = 0; i < ids.size(); i++) {
			String id = ids.get(i);
			long notWork = votes.stream().filter(v -> "not_work".equals(v.get(id).kind())).count();
			if (notWork >= settings.dropAt()) {
				dropped.add(i);
			}
			Map<String, Integer> counts = new LinkedHashMap<>();
			for (Map<String, Vote> vote : votes) {
				counts.merge(vote.get(id).name(), 1, Integer::sum);
			}
			final int index = i;
			String name = counts.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
			names.put(i, name);
			// the description from the first vote that gave the winning name, so name and description agree
			votes.stream().map(v -> v.get(id)).filter(v -> v.name().equals(name)).findFirst()
					.ifPresent(v -> abouts.put(index, v.about()));
		}
		return new Result("ok", votes, dropped, names, abouts, calls, failed);
	}

	public static Map<String, Vote> validate(String reply, List<String> ids) {
		if (reply == null) {
			return null;
		}
		String text = reply.replaceAll("(?s)<think>.*?</think>", "").trim();
		if (text.startsWith("```")) {
			int newline = text.indexOf('\n');
			int end = text.lastIndexOf("```");
			text = newline >= 0 && end > newline ? text.substring(newline + 1, end).trim() : text;
		}
		try {
			JsonElement root = JsonParser.parseString(text);
			if (!root.isJsonObject() || !root.getAsJsonObject().keySet().equals(Set.of("clusters"))) {
				return null;
			}
			JsonElement clusters = root.getAsJsonObject().get("clusters");
			if (!clusters.isJsonArray() || clusters.getAsJsonArray().size() != ids.size()) {
				return null;
			}
			Set<String> expected = new HashSet<>(ids);
			Map<String, Vote> out = new LinkedHashMap<>();
			for (JsonElement element : clusters.getAsJsonArray()) {
				if (!element.isJsonObject()) {
					return null;
				}
				JsonObject row = element.getAsJsonObject();
				if (!row.keySet().equals(Set.of("id", "kind", "name", "about"))) {
					return null;
				}
				for (String key : List.of("id", "kind", "name", "about")) {
					if (!row.get(key).isJsonPrimitive() || !row.get(key).getAsJsonPrimitive().isString()) {
						return null;
					}
				}
				String id = row.get("id").getAsString();
				String kind = row.get("kind").getAsString();
				String name = row.get("name").getAsString().trim();
				if (!expected.contains(id) || out.containsKey(id) || !Set.of("work", "not_work", "unclear").contains(kind)
						|| name.isBlank() || name.length() > 60 || name.split("\\s+").length > 5) {
					return null;
				}
				String about = row.get("about").getAsString().trim();
				out.put(id, new Vote(kind, name, about.isEmpty() || about.length() > 300 ? null : about));
			}
			return out;
		} catch (RuntimeException e) {
			return null;
		}
	}
}
