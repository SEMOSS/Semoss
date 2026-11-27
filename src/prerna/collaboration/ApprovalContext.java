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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.User;

/**
 * What the owner should weigh before approving a pending action: whether the run read content from
 * outside the owner, and who an email would go to, each recipient known or new. It only informs the
 * approval; it never blocks it.
 */
public final class ApprovalContext {

	public static final String KEY = "approvalContext";

	// the recipients are looked up in Outlook and the Brain, and a run snapshot is polled
	private static final Cache<String, Map<String, Object>> RECIPIENTS = Caffeine.newBuilder()
			.expireAfterWrite(Duration.ofSeconds(15)).maximumSize(1000).build();

	private ApprovalContext() {
	}

	/**
	 * Adds {@value #KEY} to each pending action that has something to flag, as {untrusted, sources,
	 * recipients, hasNewRecipient, resolved}. {@code user} is the owner the recipients are checked
	 * for; null leaves the recipients out.
	 */
	public static void attach(User user, List<Map<String, Object>> actions) {
		if (actions == null) {
			return;
		}
		for (Map<String, Object> action : actions) {
			Map<String, Object> context = build(user, action);
			if (context != null) {
				action.put(KEY, context);
			}
		}
	}

	/** The context for one pending action whose toolArgs and toolMeta are already parsed; null when it has none. */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> build(User user, Map<String, Object> action) {
		if (action == null) {
			return null;
		}
		Map<String, Object> meta = action.get("toolMeta") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
		Map<String, Object> args = action.get("toolArgs") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
		boolean untrusted = Boolean.TRUE.equals(meta.get(CollaborationTaint.META_UNTRUSTED));
		Map<String, Object> recipients = user == null ? null : recipients(user, action, args);
		if (!untrusted && recipients == null) {
			return null;
		}
		Map<String, Object> context = new LinkedHashMap<>();
		context.put("untrusted", untrusted);
		context.put("sources", untrusted && meta.get(CollaborationTaint.META_SOURCES) instanceof List<?> s ? s
				: new ArrayList<>());
		if (recipients != null) {
			context.putAll(recipients);
		}
		return context;
	}

	private static Map<String, Object> recipients(User user, Map<String, Object> action, Map<String, Object> args) {
		String toolName = action.get("toolName") == null ? null : String.valueOf(action.get("toolName"));
		Object actionId = action.get("actionId");
		if (actionId == null) {
			return BrainRecipientCheck.forToolCall(user, toolName, args);
		}
		String key = actionId + "|" + args.hashCode();
		Map<String, Object> cached = RECIPIENTS.getIfPresent(key);
		if (cached == null) {
			cached = BrainRecipientCheck.forToolCall(user, toolName, args);
			if (cached != null && Boolean.TRUE.equals(cached.get("resolved"))) {
				RECIPIENTS.put(key, cached);
			}
		}
		return cached;
	}
}
