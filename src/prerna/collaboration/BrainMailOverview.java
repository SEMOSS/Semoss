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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;

// onboarding step 1: a look at the mailbox from headers (counts, top senders, keep-out suggestions); stores nothing
public final class BrainMailOverview {

	private static final int MAX_PER_FOLDER = 5000;
	// 90 days read slowly on a real mailbox; 30 is the default look
	public static final int DEFAULT_DAYS = 30;
	private static final int[] WINDOWS = { 7, 30 };
	private static final int TOP = 15;
	private static final int SUGGESTIONS = 25;
	private static final int OFTEN = 5;
	private static final int OTHER_MIN = 2;
	private static final int DOMAIN_SENDERS = 2;
	private static final int DIRECTORY_LOOKUPS = 200;

	private BrainMailOverview() {
	}

	public static Map<String, Object> overview(User user, int days) throws Exception {
		if (days < 1 || days > BrainMailImport.MAX_DAYS) {
			throw new IllegalArgumentException("days must be 1 to " + BrainMailImport.MAX_DAYS);
		}
		var owner = CollaborationDbUtils.ownerOf(user);
		BrainMailHeaderSource source = BrainMailHeaderSource.current();
		Map<String, Object> me = source.me(user);
		String myAddress = BrainRulesGate
				.norm(me.get("mail") instanceof String m ? m : (String) me.get("userPrincipalName"));
		String myDomain = BrainMailImport.domain(myAddress);
		Map<String, Object> organization = source.organization(user);
		BrainOrgDomains.save(owner.getValue0(), owner.getValue1(), organization);
		BrainOrgDomains.Org ownOrg = BrainOrgDomains.of(
				organization.get("domains") instanceof List<?> d ? CollaborationDbUtils.toStringList(new ArrayList<>(d))
						: List.of(),
				myDomain);
		Instant now = Instant.now();
		Instant since = now.minus(Duration.ofDays(days));

		Map<String, Object> counts = new LinkedHashMap<>();
		Map<String, List<Map<String, Object>>> byFolder = new HashMap<>();
		boolean truncated = false;
		for (String folder : BrainMailHeaderSource.FOLDERS) {
			List<Map<String, Object>> headers = source.list(user, folder, since, MAX_PER_FOLDER);
			truncated |= headers.size() >= MAX_PER_FOLDER;
			byFolder.put(folder, headers);
			Map<String, Integer> perWindow = new LinkedHashMap<>();
			for (int window : WINDOWS) {
				if (window <= days) {
					Instant from = now.minus(Duration.ofDays(window));
					perWindow.put(String.valueOf(window),
							(int) headers.stream()
									.filter(h -> Instant.parse((String) h.get("receivedDateTime")).compareTo(from) >= 0)
									.count());
				}
			}
			perWindow.put(String.valueOf(days), headers.size());
			counts.put(folder, perWindow);
		}

		// everyone you wrote to, from Sent
		Set<String> wroteTo = new HashSet<>();
		for (Map<String, Object> h : byFolder.get(BrainMailHeaderSource.SENT)) {
			for (String field : List.of("toRecipients", "ccRecipients")) {
				if (h.get(field) instanceof List<?> list) {
					for (Object r : list) {
						String a = BrainMailImport.address(r);
						if (a != null) {
							wroteTo.add(a);
						}
					}
				}
			}
		}
		Set<String> mine = new HashSet<>(source.aliases(user));
		mine.add(myAddress);
		Map<String, Map<String, Object>> senders = new LinkedHashMap<>();
		for (Map<String, Object> h : byFolder.get(BrainMailHeaderSource.INBOX)) {
			String a = BrainMailImport.address(h.get("from"));
			if (a == null || mine.contains(a)) {
				continue;
			}
			Map<String, Object> s = senders.computeIfAbsent(a, k -> {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("address", k);
				row.put("name", name(h.get("from")));
				row.put("count", 0);
				row.put("youWrote", wroteTo.contains(k));
				return row;
			});
			s.put("count", (Integer) s.get("count") + 1);
			if (!addressedTo(h, mine)) {
				s.put("notToYou", (Integer) s.getOrDefault("notToYou", 0) + 1);
			}
			// Focused Inbox put it in Other
			if ("other".equals(h.get("inferenceClassification"))) {
				s.put("other", (Integer) s.getOrDefault("other", 0) + 1);
			}
		}
		List<Map<String, Object>> ranked = new ArrayList<>(senders.values());
		ranked.sort((x, y) -> (Integer) y.get("count") - (Integer) x.get("count"));
		// the directory settles the busiest senders: a colleague is never suggested, a
		// mailbox or list always is
		BrainMailHeaderSource.Directory directory = source.lookup(user,
				ranked.stream().limit(DIRECTORY_LOOKUPS).map(r -> (String) r.get("address")).toList());
		Map<String, Map<String, Object>> known = directory == null ? Map.of() : directory.entries();

		// keep-out suggestions: shared mailboxes and lists, mail Focused Inbox files as
		// Other, and frequent outside
		// senders you never wrote to
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(owner.getValue0(), owner.getValue1());
		List<Map<String, Object>> keepOut = new ArrayList<>();
		Map<String, Integer> automatedByDomain = new HashMap<>();
		for (Map<String, Object> s : ranked) {
			String a = (String) s.get("address");
			String kind = known.get(a) == null ? null : String.valueOf(known.get(a).get("kind"));
			if ("person".equals(kind)) {
				continue;
			}
			int count = (Integer) s.get("count");
			int other = (Integer) s.getOrDefault("other", 0);
			boolean automated = "mailbox".equals(kind) || "list".equals(kind) || (kind == null
					&& !Boolean.TRUE.equals(s.get("youWrote")) && other >= OTHER_MIN && 2 * other >= count);
			// never someone who writes to you by name: a client who writes often is still a
			// client
			boolean ignored = !automated && count >= OFTEN && !Boolean.TRUE.equals(s.get("youWrote"))
					&& 2 * (Integer) s.getOrDefault("notToYou", 0) >= count
					&& !ownOrg.isMine(BrainMailImport.domain(a));
			s.remove("notToYou");
			s.remove("other");
			if (!automated && !ignored) {
				continue;
			}
			String domain = BrainMailImport.domain(a);
			if (automated && domain != null && !ownOrg.isMine(domain)) {
				automatedByDomain.merge(domain, 1, Integer::sum);
			}
			Map<String, Object> suggestion = new LinkedHashMap<>();
			suggestion.put("kind", BrainRuleUtils.NEVER_SENDER);
			suggestion.put("value", a);
			suggestion.put("name", s.get("name"));
			suggestion.put("count", s.get("count"));
			suggestion.put("reason", "mailbox".equals(kind) ? "shared mailbox"
					: "list".equals(kind) ? "distribution list"
							: automated ? "Focused Inbox files it as Other" : "writes often, you never wrote back");
			suggestion.put("alreadyKeptOut", BrainRulesGate.neverRule(rules, a, null, null) != null);
			keepOut.add(suggestion);
		}
		for (Map.Entry<String, Integer> d : automatedByDomain.entrySet()) {
			if (d.getValue() >= DOMAIN_SENDERS) {
				Map<String, Object> suggestion = new LinkedHashMap<>();
				suggestion.put("kind", BrainRuleUtils.NEVER_DOMAIN);
				suggestion.put("value", d.getKey());
				suggestion.put("count", d.getValue());
				suggestion.put("reason", d.getValue() + " automated senders from this domain");
				suggestion.put("alreadyKeptOut",
						BrainRulesGate.neverRule(rules, "x@" + d.getKey(), null, null) != null);
				keepOut.add(0, suggestion);
			}
		}

		Map<String, Object> mailbox = new LinkedHashMap<>();
		mailbox.put("address", myAddress);
		mailbox.put("name", me.get("displayName"));
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("mailbox", mailbox);
		out.put("days", days);
		out.put("counts", counts);
		out.put("truncated", truncated);
		// people only: a directory colleague or someone you wrote to; notifications and receipts stay out
		out.put("topSenders", ranked.stream()
				.filter(r -> Boolean.TRUE.equals(r.get("youWrote"))
						|| (known.get(r.get("address")) != null && "person".equals(String.valueOf(known.get(r.get("address")).get("kind")))))
				.limit(TOP).toList());
		wroteTo.removeAll(mine);
		out.put("wroteTo", wroteTo.size());
		out.put("keepOut", keepOut.subList(0, Math.min(SUGGESTIONS, keepOut.size())));
		out.put("lastImport", CollaborationJobUtils.latest(owner.getValue0(), owner.getValue1(), BrainMailImport.KIND));
		return out;
	}

	// the owner is on To or Cc
	private static boolean addressedTo(Map<String, Object> header, Set<String> mine) {
		for (String field : List.of("toRecipients", "ccRecipients")) {
			if (header.get(field) instanceof List<?> list) {
				for (Object r : list) {
					if (mine.contains(BrainMailImport.address(r))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private static String name(Object recipient) {
		if (recipient instanceof Map<?, ?> r && r.get("emailAddress") instanceof Map<?, ?> e) {
			return (String) ((Map<String, Object>) e).get("name");
		}
		return null;
	}
}
