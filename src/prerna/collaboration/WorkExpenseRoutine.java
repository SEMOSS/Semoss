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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.io.connector.mail.MailRecipientRules;
import prerna.om.Insight;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * The opted-in expense review: when a Meeting Approval or Business Approval request reaches the owner's inbox, their
 * assistant reviews it against the guideline documents and example threads they uploaded and emails them a suggested
 * decision, copied to the recipients they chose; unless it is a plain approval, a reply to the approver waits in their
 * drafts. Run by {@link WorkRoutine} off the mail import ({@link #onNewMail}) instead of a schedule.
 */
public final class WorkExpenseRoutine {
	private static final Logger classLogger = LogManager.getLogger(WorkExpenseRoutine.class);

	public static final String ID = "expense-review";
	/** AUTOMATION_ID of the reviews' runs in the scheduler database. */
	public static final String AUTOMATION_ID = "collaboration:" + ID;
	/** The fixed routine id of the owner's guideline documents folder in {@link WorkRoutineDocuments}. */
	public static final String DOCUMENTS_ID = "e8f3a1c2-5b6d-4a7e-9c0f-1d2e3b4a5f60";
	/** A request is mail whose subject starts with one of these, in any case. */
	static final List<String> SUBJECTS = List.of("Meeting Approval", "Business Approval");
	// mail gateways tag the subject, such as "[EXT] Meeting Approval"; a reply prefix is not a tag and never matches
	private static final Pattern GATEWAY_TAGS = Pattern.compile("^(?:\\[[^\\]]{1,30}\\]\\s*)+");
	// a review already running skips the claim; the new requests wait this long for it
	private static final Duration RETRY = Duration.ofMinutes(2);
	/** The most addresses the owner can copy each review to. */
	static final int MAX_RECIPIENTS = 10;
	static final WorkRoutine.Kind KIND = new WorkRoutine.Kind(ID, "WORK_EXPENSE_ROUTINE", "WorkRunExpenseRoutine();",
			"Expense review", "expense-review.automation.json", "review", "expense review", "review",
			Map.of("timezone", "UTC", "recipients", List.of()), WorkExpenseRoutine::settings,
			(row, config) -> Map.of("cc", Objects.requireNonNullElse(config.get("recipients"), List.of())),
			WorkExpenseRoutine::agent);

	private WorkExpenseRoutine() { }

	public static Map<String, Object> get(Insight insight) {
		Map<String, Object> result = new LinkedHashMap<>(WorkRoutine.get(KIND, insight));
		result.put("documentsId", DOCUMENTS_ID);
		result.put("subjects", SUBJECTS);
		return result;
	}

	public static Map<String, Object> save(User user, Map<String, Object> changes) {
		return WorkRoutine.save(KIND, user, changes);
	}

	// who the owner copies each review to: bare addresses, each once; settings saved before this have none
	private static void settings(Map<String, Object> input, Map<String, Object> config) {
		Object value = input.get("recipients") == null ? List.of() : input.get("recipients");
		if (!(value instanceof List<?> list) || list.size() > MAX_RECIPIENTS
				|| !list.stream().allMatch(String.class::isInstance)) {
			throw new IllegalArgumentException("recipients must be at most " + MAX_RECIPIENTS + " email addresses");
		}
		Map<String, String> unique = new LinkedHashMap<>();
		for (String address : MailRecipientRules.validate(list.stream().map(String.class::cast).toList())) {
			unique.putIfAbsent(address.toLowerCase(Locale.ROOT), address);
		}
		config.put("recipients", List.copyOf(unique.values()));
	}

	/** Whether a mail subject names an approval request. */
	static boolean matches(String subject) {
		String trimmed = GATEWAY_TAGS.matcher(subject == null ? "" : subject.strip()).replaceFirst("");
		return SUBJECTS.stream().anyMatch(prefix -> trimmed.regionMatches(true, 0, prefix, 0, prefix.length()));
	}

	/**
	 * Newly imported mail: one review run for the requests among it, when the owner's review is on. Never fails the
	 * import that calls it.
	 *
	 * @param inbound [messageKey, threadId, subject] of each new message someone else sent
	 */
	static void onNewMail(String ownerId, String ownerType, List<String[]> inbound) {
		List<String> threadIds = new ArrayList<>();
		List<String> messageKeys = new ArrayList<>();
		for (String[] message : inbound) {
			if (matches(message[2])) {
				messageKeys.add(message[0]);
				threadIds.add(message[1]);
			}
		}
		if (messageKeys.isEmpty()) {
			return;
		}
		try {
			WorkRoutine.trigger(KIND, Pair.with(ownerId, ownerType), pixel(threadIds, messageKeys), null);
			classLogger.info("Expense review asked for {} request(s) of {} {}", messageKeys.size(), ownerType, ownerId);
		} catch (RuntimeException e) {
			classLogger.warn("Could not start the expense review of {} request(s)", messageKeys.size(), e);
		}
	}

	/** The scheduler recipe that reviews the given requests. */
	static String pixel(List<String> threadIds, List<String> messageKeys) {
		return "WorkRunExpenseRoutine(threadId=" + AutomationRuntimeUtils.GSON.toJson(threadIds) + ", messageKey="
				+ AutomationRuntimeUtils.GSON.toJson(messageKeys) + ");";
	}

	/** Reviews the given requests; while an earlier review still runs, they wait and this is tried again. */
	public static Map<String, Object> run(Insight insight, List<String> threadIds, List<String> messageKeys) {
		if (threadIds == null || messageKeys == null || threadIds.isEmpty() || threadIds.size() != messageKeys.size()) {
			throw new IllegalArgumentException("Pass the threadId and messageKey of each request");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		WorkRoutine.Slot slot = new WorkRoutine.Slot(KIND, owner, null);
		// a run this server lost would otherwise hold the row until Routines is opened
		WorkRoutine.settle(slot);
		Map<String, Object> result = WorkRoutine.run(KIND, insight, null, true,
				Map.of("thread_ids", threadIds, "message_keys", messageKeys));
		if ("skipped".equals(result.get("status"))) {
			Map<String, Object> row = WorkRoutine.stored(slot);
			if (row != null && Boolean.TRUE.equals(row.get("enabled")) && "running".equals(row.get("status"))) {
				WorkRoutine.trigger(KIND, owner, pixel(threadIds, messageKeys), RETRY);
				return Map.of("status", "waiting");
			}
		}
		return result;
	}

	/** The agent step: the Collaboration agent with the owner's guideline documents and the review instructions. */
	private static Map<String, Object> agent(User user, Map<String, Object> config) {
		Map<String, Object> agent = new LinkedHashMap<>();
		List<String> documents = WorkRoutineDocuments.names(user, DOCUMENTS_ID);
		if (!documents.isEmpty()) {
			// the automation copies them into the run room before the agent starts
			agent.put("routineId", DOCUMENTS_ID);
		}
		agent.put("command", command(config.get("timezone"), documents));
		return agent;
	}

	// the run's message: the routine, the time, the documents and the task; the rules and the delivery are in
	// CollaborationPrompts.routinePrompt, as for every routine room
	static String command(Object timezone, List<String> documents) {
		StringBuilder command = new StringBuilder();
		command.append("Routine: \"").append(KIND.jobName()).append("\"\n");
		command.append("It is now ").append(WorkCustomRoutine.now(timezone)).append(" in ").append(timezone)
				.append(".\n");
		command.append("You review approval requests for the owner, a designated business approver: Meeting & Event "
				+ "Services (MES) emails asking them to approve a meeting, an event or a business expense. Recommend a "
				+ "decision on each request below; the owner decides and responds in MES themselves.\n");
		if (documents.isEmpty()) {
			command.append("The owner has uploaded no guideline documents yet, so review against the policy exceptions "
					+ "the request itself states and ordinary expense judgment, and say in the result that no "
					+ "guidelines were available.\n");
		} else {
			command.append("First read the owner's documents in the \"").append(WorkRoutineDocuments.FOLDER)
					.append("\" folder of your working folder with the file tools, or the document reader for Office "
							+ "files and PDFs: ")
					.append(String.join(", ", documents)).append(". The guideline documents are the policy you review "
							+ "against. Email threads among them (an .eml or .msg file comes as a .txt copy) are examples "
							+ "of how approvers answered earlier requests: pushing back, setting conditions, declining. "
							+ "Use them as precedent for the decision, and model the draft reply below on them: who it "
							+ "goes to, its length, tone, greeting and sign-off.\n");
		}
		command.append("\nTask: write the review, and nothing else, in this shape:\n"
				+ "- For each request, in order, a level-2 heading with the meeting or expense name and its number, "
				+ "such as \"OSLT In Person (102783)\".\n"
				+ "- Under it, first one paragraph that starts with \"Suggested decision:\" and one of Approve, Approve "
				+ "with conditions, Return for more information, or Decline, followed by only what is out of "
				+ "compliance or unclear, in at most three short sentences, each naming the guideline document and "
				+ "section it breaks. Leave out everything that complies; when nothing is out of compliance, say so "
				+ "in one sentence.\n"
				+ "- Then, when there are any, a level-3 heading Before you respond with bullets: the conditions to set "
				+ "or the questions to ask, each naming the guideline it rests on.\n"
				+ "- Then, unless the decision is Approve, a level-3 heading Draft reply, a line \"To: <name>, "
				+ "<email address>\", and the email itself as a blockquote. Address it to whoever the example threads "
				+ "write to; without examples, the person the request's Financial Approval History shows approving "
				+ "it last, else the sponsoring PPMD or CLO it names. Find the address with FindPerson; when no one "
				+ "matches, give the name alone. In a few short paragraphs, no lists, it asks the questions or sets "
				+ "the conditions from Before you respond, or for Decline gives the reason and the guideline behind "
				+ "it; it never says the request is approved. Write it as the owner, signed with their first name "
				+ "from the Owner section. Do not call SaveDraft for it: the routine saves it in the owner's drafts "
				+ "as a reply to the request.\n"
				+ "- Last, a level-3 heading Request and a two-column table with what was asked: who requested it and "
				+ "the sponsoring PPMD, dates, location, meeting type, forecast (all expenses), attendees and attendee "
				+ "level, KPI per person, DU chargeback, funding code, sleeping rooms, the stated policy exceptions, "
				+ "and the objective in one line. Take every figure from the request; never invent or round one, and "
				+ "leave out what it does not say.\n"
				+ "- Start with the first heading: no greeting, no \"Here is\", no description of your steps or tools.\n"
				+ "- Everything but the draft reply is a memo to the owner: it addresses no one else and is never "
				+ "written as the owner approving or declining; the owner decides.\n\n"
				+ "Requests: ${context.reviewInput}");
		return command.toString();
	}
}
