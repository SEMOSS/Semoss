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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

final class BrainFilingSignals {

	// One contribution per topic, not per keyword or occurrence.
	static final double KEYWORD_WEIGHT = 0.10;
	private static final String EVIDENCE_KEY = "filingEvidence";

	private BrainFilingSignals() {
	}

	static BrainClassifier.Scores apply(BrainClassifier.Scores scores, List<BrainClassifier.TopicOption> topics,
			String subject, List<BrainClassifier.Message> messages, Set<String> senderIds, Set<String> rejected) {
		Map<String, Double> adjusted = new LinkedHashMap<>(scores.topics());
		Map<String, Object> evidence = new LinkedHashMap<>();
		boolean boosted = false;
		for (BrainClassifier.TopicOption topic : topics) {
			if (BrainThreadClassifier.OTHER_TOPIC.equals(topic.id()) || rejected.contains(topic.id())
					|| !adjusted.containsKey(topic.id())) {
				continue;
			}
			List<Map<String, Object>> matches = keywordMatches(topic.keywords(), subject, messages);
			List<String> people = topic.personIds().stream().filter(senderIds::contains).distinct().toList();
			Map<String, Object> topicEvidence = new LinkedHashMap<>();
			topicEvidence.put("keywordMatches", matches);
			topicEvidence.put("matchedSenderIds", people);
			topicEvidence.put("keywordWeight", matches.isEmpty() ? 0.0 : KEYWORD_WEIGHT);
			evidence.put(topic.id(), topicEvidence);
			if (!matches.isEmpty()) {
				adjusted.put(topic.id(), adjusted.get(topic.id()) + KEYWORD_WEIGHT);
				boosted = true;
			}
		}
		if (boosted) {
			double total = adjusted.values().stream().mapToDouble(Double::doubleValue).sum();
			if (!Double.isFinite(total) || total <= 0) {
				throw new IllegalStateException("Invalid adjusted topic scores");
			}
			adjusted.replaceAll((id, score) -> score / total);
		}
		Map<String, Object> filing = new LinkedHashMap<>();
		filing.put("modelScores", scores.topics());
		filing.put("topics", evidence);
		Map<String, Object> raw = new LinkedHashMap<>();
		if (scores.raw() != null) {
			raw.putAll(scores.raw());
		}
		raw.put(EVIDENCE_KEY, filing);
		return new BrainClassifier.Scores(adjusted, scores.fyi(), scores.automated(), scores.urgency(), raw);
	}

	static Map<String, Object> forLink(BrainClassifier.Scores scores, String topicId, int baseConfidence,
			int confidence, boolean topicVipApplied) {
		Map<String, Object> signals = new LinkedHashMap<>();
		signals.put("version", 1);
		signals.put("topicScores", scores.topics());
		signals.put("baseConfidence", baseConfidence);
		signals.put("finalConfidence", confidence);
		signals.put("topicVipApplied", topicVipApplied);
		signals.put("topicVipPoints", topicVipApplied ? confidence - baseConfidence : 0);
		signals.put("keywordMatches", List.of());
		signals.put("matchedSenderIds", List.of());
		if (scores.raw() != null && scores.raw().get(EVIDENCE_KEY) instanceof Map<?, ?> filing) {
			signals.put("modelScores", filing.get("modelScores"));
			if (filing.get("topics") instanceof Map<?, ?> topics
					&& topics.get(topicId) instanceof Map<?, ?> evidence) {
				signals.put("keywordMatches", evidence.get("keywordMatches"));
				signals.put("matchedSenderIds", evidence.get("matchedSenderIds"));
				signals.put("keywordWeight", evidence.get("keywordWeight"));
			}
		}
		return signals;
	}

	static List<Map<String, Object>> keywordMatches(List<String> keywords, String subject,
			List<BrainClassifier.Message> messages) {
		List<Map<String, Object>> matches = new ArrayList<>();
		Set<String> seen = new LinkedHashSet<>();
		for (String keyword : keywords) {
			if (keyword == null || keyword.isBlank()) {
				continue;
			}
			String value = keyword.strip();
			if (!seen.add(value.toLowerCase(Locale.ROOT))) {
				continue;
			}
			Pattern pattern = Pattern.compile(
					"(?<![\\p{L}\\p{N}_])" + Pattern.quote(value) + "(?![\\p{L}\\p{N}_])",
					Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
			if (matches(pattern, subject)) {
				matches.add(Map.of("keyword", value, "field", "subject"));
				continue;
			}
			for (int i = 0; i < messages.size(); i++) {
				if (matches(pattern, messages.get(i).text())) {
					matches.add(Map.of("keyword", value, "field", "body", "messageIndex", i));
					break;
				}
			}
		}
		return matches;
	}

	private static boolean matches(Pattern pattern, String text) {
		return text != null && pattern.matcher(text).find();
	}
}
