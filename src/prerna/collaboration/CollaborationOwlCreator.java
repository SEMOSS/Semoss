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
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

// Brain and Work tables; every row is owner-scoped; message bodies are read from the source, never stored
public class CollaborationOwlCreator extends AbstractOwlCreator {

	public CollaborationOwlCreator(AbstractSqlQueryUtil queryUtil) {
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String VARCHAR_20 = "VARCHAR(20)";
		final String VARCHAR_50 = "VARCHAR(50)";
		final String VARCHAR_255 = "VARCHAR(255)";
		// owner boundary columns on every table
		final Pair<String, String> OWNER_ID = Pair.with("OWNER_ID", VARCHAR_255);
		final Pair<String, String> OWNER_TYPE = Pair.with("OWNER_TYPE", VARCHAR_50);

		this.allSchemas = new ArrayList<>();

		// @formatter:off

		// --- Owner and sources ---
		// The owner's link to Microsoft 365. A data reset (BrainResetUtils) keeps these two tables,
		// BRAIN_OWN_MAIL and BRAIN_PERSONA, and deletes the owner's rows in every other table.
		// one row per owner: the Microsoft 365 mailbox their Brain reads (MS_USER_ID, MS_UPN),
		// saved by each mail import. A Graph new-mail notification and the scheduled sync find the
		// owner by MS_USER_ID, and the routines check it before they run
		addTable("COLLAB_OWNER", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MS_USER_ID", VARCHAR_255),
				Pair.with("MS_UPN", VARCHAR_255),
				Pair.with("STATUS", VARCHAR_20),
				// the Microsoft tenant's name and verified domains: what counts as the owner's own organisation
				Pair.with("ORG_NAME", VARCHAR_255),
				Pair.with("ORG_DOMAINS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// one row per owner and source (email, calendar, teams). ENABLED is the owner's consent to
		// read it: while it is off, the rules gate drops that source's messages without recording
		// them. LAST_EVENT_AT is the last good read, and LAST_ERROR and REAUTH_NEEDED the last
		// failure, so the owner sees each source as connected, needs_permission or off
		addTable("SOURCE_CONNECTION", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("SOURCE", VARCHAR_50),
				Pair.with("ENABLED", BOOLEAN_DATATYPE_NAME),
				Pair.with("LAST_EVENT_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAST_ERROR", CLOB_DATATYPE_NAME),
				Pair.with("LAST_ERROR_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("REAUTH_NEEDED", BOOLEAN_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// one row per background job of an owner's (KIND import, sync, classify, style, topic_map,
		// topic_match, topic_resort or topic_watch): its step, progress, counts and outcome, so the
		// page can poll it (CollaborationJobUtils). One job of a kind runs per owner at a time, and
		// a row a server restart left running reads as failed
		addTable("COLLAB_JOB", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("JOB_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("STEP", VARCHAR_50),
				Pair.with("PROGRESS", INTEGER_DATATYPE_NAME),
				Pair.with("PARAMS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("COUNTS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("ERROR", CLOB_DATATYPE_NAME),
				Pair.with("STARTED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("FINISHED_AT", TIMESTAMP_DATATYPE_NAME)));

		// Routines: one table per routine kind (WorkRoutine.Kind), all with the same columns. A row
		// is one owner's routine: their settings (SETTINGS_JSON), whether they opted in (ENABLED),
		// and its latest run (RUN_STATUS, STARTED_AT, FINISHED_AT, SUMMARY, ERROR, ERROR_CAUSE),
		// with RUN_ID naming the Automation run in the scheduler database and ROOM_ID its room.
		// LAST_DATE is the owner's local date of the latest run. Each row has its own scheduler
		// job. The first four tables hold one row per owner (a unique index); WORK_CUSTOM_ROUTINE
		// holds many per owner, told apart by ROUTINE_ID (null in the others)
		for (String routine : List.of("WORK_INBOX_ROUTINE", "WORK_AGENDA_ROUTINE", "WORK_EXPENSE_ROUTINE",
				"WORK_EVENT_PLANNING_ROUTINE", "WORK_CUSTOM_ROUTINE")) {
			addTable(routine, Arrays.asList(
					OWNER_ID, OWNER_TYPE,
					Pair.with("ROUTINE_ID", VARCHAR_50),
					Pair.with("SETTINGS_JSON", CLOB_DATATYPE_NAME),
					Pair.with("ENABLED", BOOLEAN_DATATYPE_NAME),
					Pair.with("LAST_DATE", VARCHAR_20),
					Pair.with("RUN_STATUS", VARCHAR_20),
					Pair.with("STARTED_AT", TIMESTAMP_DATATYPE_NAME),
					Pair.with("FINISHED_AT", TIMESTAMP_DATATYPE_NAME),
					Pair.with("LAST_SUCCESS_AT", TIMESTAMP_DATATYPE_NAME),
					Pair.with("ROOM_ID", VARCHAR_50),
					Pair.with("RUN_ID", VARCHAR_50),
					Pair.with("SUMMARY", CLOB_DATATYPE_NAME),
					Pair.with("ERROR", CLOB_DATATYPE_NAME),
					Pair.with("ERROR_CAUSE", VARCHAR_20),
					Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		}

		// --- Brain: you ---
		// What Brain knows about the owner and how they set it up, one row per owner in each table.
		// who the owner is and how they write, made on first read from their login's name, email
		// and time zone. The assistant prompt introduces the owner from it, and drafts use the
		// confirmed writing style. ROLE_STATE and STYLE_STATE say where a value came from: learned
		// (Brain's proposal), confirmed (learned, then accepted) or you (typed by the owner)
		addTable("BRAIN_PROFILE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("DISPLAY_NAME", VARCHAR_255),
				Pair.with("EMAIL", VARCHAR_255),
				Pair.with("ROLE", VARCHAR_255),
				Pair.with("ROLE_STATE", VARCHAR_20),
				Pair.with("ROLE_NOTE", VARCHAR_255),
				Pair.with("ORG", VARCHAR_255),
				Pair.with("TIMEZONE", VARCHAR_50),
				Pair.with("WORKING_HOURS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STYLE_SUMMARY", CLOB_DATATYPE_NAME),
				Pair.with("STYLE_STATE", VARCHAR_20),
				Pair.with("STYLE_EXAMPLES_JSON", CLOB_DATATYPE_NAME),
				// audience variants the owner agreed, {internal, external} each {summary, examples}
				Pair.with("STYLE_VARIANTS_JSON", CLOB_DATATYPE_NAME),
				// the learned proposal waiting for the owner's review; never used when drafting
				Pair.with("STYLE_PENDING_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STYLE_LEARNED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// the owner's Brain settings, made on first read. Brain files a thread under its best topic
		// at FILE_AT confidence or more and asks the owner at ASK_AT or more (0 to 100, defaults 85
		// and 40 from BrainProfileUtils). VERSION goes up on each save, and a save made against an
		// older VERSION is refused. The other columns hold the owner's choices for memory, drafts,
		// skills, sync, topic watch and setup
		addTable("BRAIN_SETTINGS", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("FILE_AT", INTEGER_DATATYPE_NAME),
				Pair.with("ASK_AT", INTEGER_DATATYPE_NAME),
				Pair.with("VERSION", INTEGER_DATATYPE_NAME),
				// memory recall and tools, and the review of finished chats; null reads as on
				Pair.with("MEMORY_USE", BOOLEAN_DATATYPE_NAME),
				Pair.with("MEMORY_LEARN", BOOLEAN_DATATYPE_NAME),
				// the Outlook folder drafts are left in; null means the mailbox's own Drafts
				Pair.with("DRAFT_FOLDER_ID", VARCHAR_255),
				Pair.with("DRAFT_FOLDER_NAME", VARCHAR_255),
				// how drafts look: font (null is Aptos 12pt) and the signature copied from sent mail; null on is on
				Pair.with("DRAFT_FONT", VARCHAR_255),
				Pair.with("DRAFT_FONT_SIZE", INTEGER_DATATYPE_NAME),
				Pair.with("DRAFT_SIGNATURE_HTML", CLOB_DATATYPE_NAME),
				Pair.with("DRAFT_SIGNATURE_ON", BOOLEAN_DATATYPE_NAME),
				// ids of the owner's own Collaboration skills staged in their assistant chats
				Pair.with("ASSISTANT_SKILLS_JSON", CLOB_DATATYPE_NAME),
				// minutes between scheduled syncs of the owner's mail while the app is closed (BrainSync); 0 is off,
				// null the default 15
				Pair.with("SYNC_EVERY_MINUTES", INTEGER_DATATYPE_NAME),
				// hours between looks for new topics in incoming mail (BrainTopicSuggest.watch); 0 is off, null the
				// instance's COLLAB_TOPIC_WATCH_HOURS
				Pair.with("TOPIC_WATCH_HOURS", INTEGER_DATATYPE_NAME),
				// whether a done message on another thread of a topic may finish a step (WorkReconcile); null the
				// instance's COLLAB_WORK_CLOSE_ACROSS
				Pair.with("WORK_CLOSE_ACROSS", BOOLEAN_DATATYPE_NAME),
				// hours between checks of the owner's Digital Twins against what they were copied from
				// (WorkTwinUpdates.watch); 0 is off, null the default 24
				Pair.with("TWIN_CHECK_HOURS", INTEGER_DATATYPE_NAME),
				// when the owner finished setup (Open Work) or skipped the rest of it; null sends them back to it,
				// unless their import is from before this column (BrainConnection)
				Pair.with("SETUP_DONE_AT", TIMESTAMP_DATATYPE_NAME),
				// the first Work build over the imported mail: its state and the threads it has left (WorkPipeline)
				Pair.with("WORK_BUILD_JSON", CLOB_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// the owner's persona for their Digital Twins, one row per owner (WorkTwinPersona): how they
		// sound, how they decide and what a twin must leave to them, in eight short sections
		// (BRIEF_JSON). There is a persona while a section holds text. INTERVIEW_JSON is the working
		// state of the interview that writes it and never reaches a prompt. OFF_TWINS_JSON lists the
		// twins (project ids) the owner switched the persona off for; a twin not listed uses it. A
		// data reset keeps the row and clears the interview
		addTable("BRAIN_PERSONA", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("BRIEF_JSON", CLOB_DATATYPE_NAME),
				Pair.with("INTERVIEW_JSON", CLOB_DATATYPE_NAME),
				Pair.with("OFF_TWINS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// the owner's topic setup at onboarding: one resumable draft per owner, which a topic_map
		// job builds. DRAFT_JSON holds only topic profiles and bounded evidence metadata; REVISION
		// goes up on each edit, and an edit made against an older revision is refused. Applying it
		// saves the topics, records APPLIED_REVISION and the receipt (RESULT_JSON), and starts the
		// classify job that files threads under them (FILING_JOB_ID, a COLLAB_JOB)
		addTable("BRAIN_TOPIC_REVIEW", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("REVIEW_ID", VARCHAR_50),
				Pair.with("REVISION", INTEGER_DATATYPE_NAME),
				Pair.with("DRAFT_JSON", CLOB_DATATYPE_NAME),
				Pair.with("APPLIED_REVISION", INTEGER_DATATYPE_NAME),
				Pair.with("RESULT_JSON", CLOB_DATATYPE_NAME),
				Pair.with("FILING_JOB_ID", VARCHAR_50),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: people ---
		// The people the owner corresponds with and the organizations they belong to.
		// an organization the owner works with: KIND client, internal or other, with its email
		// domains (DOMAINS_JSON) and color. People and topics belong to one by ACCOUNT_ID, and
		// topic suggestions match outside senders to an account by domain
		addTable("BRAIN_ACCOUNT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ACCOUNT_ID", VARCHAR_50),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("DOMAINS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("COLOR", VARCHAR_20),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a person the owner corresponds with, made by the mail import from an address (EMAIL_NORM,
		// lowercased); the owner is one too, with RELATIONSHIP self. It holds what the Microsoft
		// directory says (DIRECTORY_ID, name, title, department, company), the relationship
		// (colleague, external, automated and so on) and who set it (RELATIONSHIP_STATE: suggested
		// by Brain's rules, directory, or confirmed by the owner), VIP, and STRENGTH (0 to 100) and
		// LAST_CONTACT_AT from the stored headers. Its other addresses are in BRAIN_PERSON_ADDRESS
		addTable("BRAIN_PERSON", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("EMAIL_NORM", VARCHAR_255),
				Pair.with("DIRECTORY_ID", VARCHAR_255),
				Pair.with("DISPLAY_NAME", VARCHAR_255),
				Pair.with("GIVEN_NAME", VARCHAR_255),
				Pair.with("SURNAME", VARCHAR_255),
				Pair.with("JOB_TITLE", VARCHAR_255),
				Pair.with("DEPARTMENT", VARCHAR_255),
				Pair.with("COMPANY", VARCHAR_255),
				Pair.with("DIRECTORY_CHECKED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ACCOUNT_ID", VARCHAR_50),
				Pair.with("RELATIONSHIP", VARCHAR_50),
				Pair.with("RELATIONSHIP_STATE", VARCHAR_20),
				Pair.with("IS_VIP", BOOLEAN_DATATYPE_NAME),
				Pair.with("NEVER_INGEST", BOOLEAN_DATATYPE_NAME),
				Pair.with("CHANNELS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STRENGTH", INTEGER_DATATYPE_NAME),
				Pair.with("LAST_CONTACT_AT", TIMESTAMP_DATATYPE_NAME),
				// following (the owner's people), suggested (Brain proposes), declined (not again); null otherwise
				Pair.with("FOLLOW_STATE", VARCHAR_20),
				Pair.with("FOLLOW_REASON", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// an address a person is known by, one row each (KIND email, VALUE_NORM lowercased). Each
		// address belongs to one BRAIN_PERSON, and the owner's aliases all point at their own row;
		// the import and the rules gate find a sender's person here
		addTable("BRAIN_PERSON_ADDRESS", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("VALUE_NORM", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: topics ---
		// A topic is something the owner's work is about; threads, chats, folders, files, people
		// and work items link to it by TOPIC_ID.
		// one topic: its name, KIND (client, internal, event or personal), and the description and
		// keywords the classifier matches threads against. STATUS is suggested (Brain proposed it,
		// ORIGIN brain, with SUGGEST_REASON), active, dormant (30 days with no activity or owner
		// edit) or archived. Topics are flat and group by ACCOUNT_ID
		addTable("BRAIN_TOPIC", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("NAME", VARCHAR_255),
				// defaults to the name, so as wide as NAME
				Pair.with("SHORT_NAME", VARCHAR_255),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("ACCOUNT_ID", VARCHAR_50),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("KEYWORDS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CALENDAR_SERIES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("COLOR", VARCHAR_20),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("SUGGEST_REASON", CLOB_DATATYPE_NAME),
				Pair.with("MERGE_CANDIDATE_ID", VARCHAR_50),
				Pair.with("LAST_ACTIVITY_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a note on a topic: its text, KIND, STATE, resolution and where it came from (ORIGIN,
		// SOURCE_REF). At startup BrainMemoryMigration moves each row of KIND note into
		// BRAIN_MEMORY, linked to its topic; rows follow their topic through a merge and go with it
		// on a delete
		addTable("BRAIN_TOPIC_NOTE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("NOTE_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("RESOLUTION", CLOB_DATATYPE_NAME),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("SOURCE_REF", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// who is in a topic, one row per topic and person: STATE member, suggested (Brain proposes
		// them, with a REASON) or removed (so they are not proposed again), with their role in it
		// and a topic-only VIP flag. The classifier files a thread under a topic that has people
		// only when one of them is on the thread
		addTable("BRAIN_TOPIC_PERSON", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("ROLE_LABEL", VARCHAR_255),
				Pair.with("ENGAGEMENT", INTEGER_DATATYPE_NAME),
				Pair.with("REASON", CLOB_DATATYPE_NAME),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME),
				// VIP in this topic only; read for members (BrainRulesGate.standing)
				Pair.with("IS_VIP", BOOLEAN_DATATYPE_NAME)));
		// a chat's topics, one row per assistant chat room and topic: linked, suggested (a soft
		// tag) or dismissed (not suggested again in that chat). The owner picks them or the
		// assistant tags them as the owner works; only linked topics apply to the chat
		addTable("BRAIN_TOPIC_ROOM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME)));
		// Outlook folders a topic stands for, one row per topic and folder (a BRAIN_MAIL_FOLDER
		// id): mail in one of them is filed under the topic, unless two topics share the folder
		addTable("BRAIN_TOPIC_FOLDER", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("FOLDER_ID", VARCHAR_255),
				// outlook when onboarding found it from the folder, you when the owner linked it
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// OneDrive and Teams files the owner linked to a topic, by the ids Microsoft 365 keeps for
		// them, never a copy: the topic page opens the file where it lives, and the assistant is
		// told of it in the topic's brief
		addTable("BRAIN_TOPIC_FILE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("DRIVE_ID", VARCHAR_255),
				Pair.with("ITEM_ID", VARCHAR_255),
				// as Microsoft 365 had them when the file was linked
				Pair.with("NAME", VARCHAR_255),
				Pair.with("WEB_URL", CLOB_DATATYPE_NAME),
				// onedrive or teams: the app the owner picked the file in
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// OneDrive, Teams and SharePoint folders the owner linked to a topic; stores pointers, never contents
		addTable("BRAIN_TOPIC_LINK", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("LINK_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_50),
				Pair.with("DRIVE_ID", VARCHAR_255),
				Pair.with("ITEM_ID", VARCHAR_255),
				Pair.with("TEAM_ID", VARCHAR_255),
				Pair.with("CHANNEL_ID", VARCHAR_255),
				Pair.with("SITE_ID", VARCHAR_255),
				Pair.with("DISPLAY_NAME", VARCHAR_255),
				Pair.with("WEB_URL", CLOB_DATATYPE_NAME),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("CHECKED_AT", TIMESTAMP_DATATYPE_NAME)));
		// mail Collaboration sent the owner (KIND routine or notice), by the key the import gives
		// it, so the import skips it. A data reset keeps these rows, so a later import skips it too
		addTable("BRAIN_OWN_MAIL", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MESSAGE_KEY", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("SENT_AT", TIMESTAMP_DATATYPE_NAME)));
		// the owner's Outlook folder tree, one row per folder, saved whole each time it is read
		// again (after 12 hours, or sooner when mail turns up in a folder it does not know). KIND
		// is inbox, sentitems, archive or scheduled (system folders mail is read from), custom
		// (one the owner made) or skipped (any other system folder and all under it; not imported)
		addTable("BRAIN_MAIL_FOLDER", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("FOLDER_ID", VARCHAR_255),
				Pair.with("PARENT_ID", VARCHAR_255),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("PATH", CLOB_DATATYPE_NAME),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TOTAL", INTEGER_DATATYPE_NAME),
				// what a folder the owner made is for, judged at onboarding: project, area, status, automated,
				// person, time or other; null until judged
				Pair.with("ROLE", VARCHAR_20),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: threads ---
		// A thread is one conversation; its topics, people, messages and chats link by THREAD_ID.
		// one conversation, an email conversation or a Teams chat, found by THREAD_KEY
		// (source:conversationId): its subject, the MUTED and AUTOMATED flags, the summary Brain
		// wrote and its message count. Topics and participants live in the link tables below (one
		// IS_PRIMARY topic per thread), and its messages in BRAIN_MESSAGE
		addTable("BRAIN_THREAD", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("THREAD_KEY", VARCHAR_255),
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("SUBJECT", VARCHAR_255),
				Pair.with("MUTED", BOOLEAN_DATATYPE_NAME),
				Pair.with("AUTOMATED", BOOLEAN_DATATYPE_NAME),
				// the owner said this thread is not automated; no later run may flag it again
				Pair.with("AUTOMATED_OVERRIDE", BOOLEAN_DATATYPE_NAME),
				Pair.with("ROOM_ID", VARCHAR_50),
				// unused: nothing reads or writes it, since a thread's goals are its topics' goals
				Pair.with("GOAL", CLOB_DATATYPE_NAME),
				Pair.with("SUMMARY", CLOB_DATATYPE_NAME),
				// the newest message the summary and generated steps were made from, and when
				Pair.with("SUMMARY_REF", VARCHAR_255),
				Pair.with("SUMMARY_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("MESSAGE_COUNT", INTEGER_DATATYPE_NAME),
				Pair.with("LAST_MESSAGE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				// the newest message work candidates were pulled from
				Pair.with("WORK_REF", VARCHAR_255),
				// sort attempts that failed in a row; a thread is tried again on each sync until it succeeds or
				// has failed BrainThreadClassifier.SORT_TRIES times
				Pair.with("SORT_FAILS", INTEGER_DATATYPE_NAME)));
		// a thread's topics, one row per thread and topic. SOURCE is confirmed (Brain filed it),
		// suggested (Brain asks the owner) or you (the owner filed it), with the CONFIDENCE and
		// classifier signals behind it; one link per thread is IS_PRIMARY
		addTable("BRAIN_THREAD_TOPIC", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("CONFIDENCE", INTEGER_DATATYPE_NAME),
				Pair.with("IS_PRIMARY", BOOLEAN_DATATYPE_NAME),
				Pair.with("SIGNALS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CLASSIFIER_VERSION", VARCHAR_50),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a topic the owner said a thread does not belong to, one row per thread and topic. The
		// classifier and a re-sort never file the thread there again, and a rejected thread left
		// with no topic is one of the classifier's examples of mail with no topic
		addTable("BRAIN_THREAD_TOPIC_REJECTION", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a person on a thread, one row per thread and person: their roles (ROLES_JSON from, to,
		// cc) and whether what they write reaches Brain and the assistant (INCLUDED). EXCLUDED_BY
		// is you (the owner took them out) or rule (EXCLUDED_RULE_ID, a BRAIN_RULE); HIDDEN_COUNT
		// counts their messages a rule kept out
		addTable("BRAIN_THREAD_PARTICIPANT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("ROLES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("INCLUDED", BOOLEAN_DATATYPE_NAME),
				Pair.with("EXCLUDED_BY", VARCHAR_20),
				Pair.with("EXCLUDED_RULE_ID", VARCHAR_50),
				Pair.with("EXCLUDED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("HIDDEN_COUNT", INTEGER_DATATYPE_NAME),
				Pair.with("FIRST_SEEN_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAST_SEEN_AT", TIMESTAMP_DATATYPE_NAME)));
		// a conversation Outlook started for a reply (an outside mail client, a subject a gateway tagged) that
		// belongs to the thread of the email it answers; THREAD_KEY is source:conversationId as on BRAIN_THREAD
		// the import files the rest of that conversation under THREAD_ID
		addTable("BRAIN_THREAD_ALIAS", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_KEY", VARCHAR_255),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a chat's email threads, one row per chat room and thread: linked, or removed (not added
		// back). A chat opened from a thread includes it; the owner adds and removes others.
		// SEEN_REF is the newest email of the thread the chat's assistant has been given, so later
		// ones are new to the chat
		addTable("BRAIN_THREAD_ROOM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("SEEN_REF", VARCHAR_255),
				Pair.with("SEEN_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME)));
		// one imported message (an email or a Teams message), by MESSAGE_KEY (an id made from the
		// source's message id): its thread, sender, folder, time, Outlook flags and the rules
		// gate's DECISION (ingested, never, excluded or muted) with the RULE_ID behind it. Metadata
		// and gate decision only, never a message body; a body is read from the source when needed
		addTable("BRAIN_MESSAGE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MESSAGE_KEY", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("GRAPH_ID", VARCHAR_255),
				Pair.with("SENDER_PERSON_ID", VARCHAR_50),
				Pair.with("FOLDER", VARCHAR_255),
				Pair.with("RECEIVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DECISION", VARCHAR_20),
				Pair.with("RULE_ID", VARCHAR_50),
				Pair.with("CLASSIFIED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CLASSIFIER_VERSION", VARCHAR_50),
				// the owner was on To or Cc; null for their own messages
				Pair.with("TO_ME", BOOLEAN_DATATYPE_NAME),
				// Focused Inbox put it in Other, or it was sent on behalf of another mailbox
				Pair.with("BULK", BOOLEAN_DATATYPE_NAME),
				// a calendar message: invite, reply, or cancellation
				Pair.with("MEETING", BOOLEAN_DATATYPE_NAME),
				// the sending system marked it machine-sent (Auto-Submitted, List-Unsubscribe, Precedence bulk)
				Pair.with("AUTO", BOOLEAN_DATATYPE_NAME),
				// the owner's Outlook categories on it, their follow-up flag, the sender's importance, attachments
				Pair.with("CATEGORIES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("FLAGGED", BOOLEAN_DATATYPE_NAME),
				Pair.with("IMPORTANCE", VARCHAR_20),
				Pair.with("ATTACHMENTS", BOOLEAN_DATATYPE_NAME),
				// imported by a sync, a chat's pull or the webhook, and still owed its after-import steps (reply
				// cards, the expense review): a run cut off before them leaves it to the next sync; null when nothing is owed
				Pair.with("SYNC_PENDING", BOOLEAN_DATATYPE_NAME)));

		// --- Brain: control ---
		// The owner's say over Brain: keep-out rules, the questions Brain asks, and a change log.
		// one of the owner's keep-out rules, which the rules gate applies to each message before
		// any body is read. KIND never_sender, never_domain, never_folder or never_keyword keeps
		// matching mail off every thread; exclude_topic, exclude_channel or exclude_everywhere
		// hides a person on the threads it covers; mute_sender mutes. VALUE, TOPIC_ID, PERSON_ID
		// and CHANNEL say what it covers; a deleted rule keeps its row, with DISABLED_AT set
		addTable("BRAIN_RULE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("RULE_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_50),
				Pair.with("VALUE", VARCHAR_255),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("CHANNEL", VARCHAR_20),
				Pair.with("NOTE", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_BY", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DISABLED_AT", TIMESTAMP_DATATYPE_NAME)));
		// one question Brain asks the owner in the review queue: KIND new_topic, topic_choice,
		// add_person or unassigned, about a thread, person or topic (REF_TYPE, REF_ID), with its
		// text and actions in DETAIL_JSON. STATUS is open, accepted or dismissed, and the owner's
		// answer is kept in RESOLUTION_JSON
		addTable("BRAIN_REVIEW", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("REVIEW_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_50),
				Pair.with("REF_TYPE", VARCHAR_50),
				Pair.with("REF_ID", VARCHAR_50),
				Pair.with("DETAIL_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RESOLVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RESOLVED_BY", VARCHAR_20),
				Pair.with("RESOLUTION_JSON", CLOB_DATATYPE_NAME)));
		// a log of the owner's changes to Brain, one row per change: a topic delete, merge or undo,
		// or a thread's not-automated correction, with the field, old and new value, ACTOR and
		// time. A topic delete or merge also saves the rows it touched, so Undo can put them back
		addTable("BRAIN_CHANGE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("CHANGE_ID", VARCHAR_50),
				Pair.with("ENTITY_TYPE", VARCHAR_50),
				Pair.with("ENTITY_ID", VARCHAR_50),
				Pair.with("FIELD", VARCHAR_50),
				Pair.with("OLD_VALUE", VARCHAR_255),
				Pair.with("NEW_VALUE", VARCHAR_255),
				Pair.with("ACTOR", VARCHAR_20),
				Pair.with("AT", TIMESTAMP_DATATYPE_NAME),
				// before/after rows for undo of a topic delete or merge; cleared after a day
				Pair.with("SNAPSHOT_JSON", CLOB_DATATYPE_NAME)));

		// --- Brain: memory ---
		// What the assistant remembers for the owner across chats and threads.
		// one statement the assistant keeps across threads, a preference or a fact (KIND). STATE is
		// active (recalled), suggested (waits for the owner), superseded or dismissed; ORIGIN is
		// you, assistant (Remember in a chat) or brain (the review of finished chats). Links in
		// BRAIN_MEMORY_LINK say who or what it is about
		addTable("BRAIN_MEMORY", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MEMORY_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				// typed, accepted, edited, or confirmed by the owner; only then a preference is an instruction
				Pair.with("CONFIRMED", BOOLEAN_DATATYPE_NAME),
				Pair.with("PINNED", BOOLEAN_DATATYPE_NAME),
				// the memory this one superseded, or would supersede once a suggestion is accepted
				Pair.with("REPLACES_ID", VARCHAR_50),
				Pair.with("EXPIRES_AT", TIMESTAMP_DATATYPE_NAME),
				// where it came from: ui, chat, chat_review, or a migrated topic_note or thread_fact
				Pair.with("SOURCE_KIND", VARCHAR_20),
				Pair.with("SOURCE_THREAD_ID", VARCHAR_50),
				Pair.with("SOURCE_ROOM_ID", VARCHAR_50),
				Pair.with("SOURCE_REF", VARCHAR_255),
				// who said it; recall leaves the memory out while that person is excluded
				Pair.with("SOURCE_PERSON_ID", VARCHAR_50),
				Pair.with("SOURCE_LABEL", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CONFIRMED_AT", TIMESTAMP_DATATYPE_NAME)));
		// what a memory is about, one row per memory and person, topic, account or thread
		// (REF_TYPE, REF_ID); a memory without links applies everywhere
		addTable("BRAIN_MEMORY_LINK", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MEMORY_ID", VARCHAR_50),
				Pair.with("REF_TYPE", VARCHAR_20),
				Pair.with("REF_ID", VARCHAR_50),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// how far the review of finished chats has read each assistant room, one row per room: the
		// last message read, the outcome and when. Style learning samples the recently read rooms
		addTable("BRAIN_MEMORY_SCAN", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("LAST_MESSAGE_ID", VARCHAR_255),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("ERROR", CLOB_DATATYPE_NAME),
				Pair.with("SCANNED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Work ---
		// What needs the owner: queue items raised from threads (WORK_ITEM), and the work items
		// they drive (WORK_TASK) with their steps (WORK_THREAD_STEP).
		// one Work queue item: something a thread asks of the owner (ASK_TYPE reply, approve,
		// attend, review or errand), something they wait on (waiting_on), or an fyi. Brain's
		// classifier raises it from a thread's newest message, or the owner or the assistant adds
		// it. STATUS is open, waiting, snoozed, done or dismissed; the queue views (needs_me,
		// waiting, suggested, done_today) read it, and WORK_ITEM_HISTORY logs its changes
		addTable("WORK_ITEM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ITEM_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("SOURCE_REF", VARCHAR_255),
				Pair.with("ACTOR_TYPE", VARCHAR_20),
				Pair.with("ACTOR_ID", VARCHAR_255),
				Pair.with("ACTOR_NAME", VARCHAR_255),
				Pair.with("TITLE", VARCHAR_255),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("ASK_TYPE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("PRIORITY", VARCHAR_20),
				Pair.with("SCORE", INTEGER_DATATYPE_NAME),
				Pair.with("REASONS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("DUE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RECEIVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROCESSED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CLOSED_REASON", VARCHAR_20),
				// the keep-out rule that closed it; deleting the rule reopens it
				Pair.with("CLOSED_RULE_ID", VARCHAR_50),
				Pair.with("SNOOZE_UNTIL", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("ASSIGNEE_PERSON_ID", VARCHAR_50),
				Pair.with("LINK_TOPIC_ID", VARCHAR_50),
				Pair.with("CLASSIFIER_VERSION", VARCHAR_50),
				// the classifier's raw scores, kept so its cutoffs can be re-tuned from owner corrections
				Pair.with("SIGNALS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("DEDUPE_KEY", VARCHAR_255),
				Pair.with("SUGGESTED", BOOLEAN_DATATYPE_NAME),
				Pair.with("CLOSED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a WORK_ITEM's change log, one row per field an update changed (FIELD created for a new
		// item): the old and new value, who changed it (you, brain or assistant) and why. Brain
		// leaves alone a field the owner set, and a sync reports from it what new mail did
		addTable("WORK_ITEM_HISTORY", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("HISTORY_ID", VARCHAR_50),
				Pair.with("ITEM_ID", VARCHAR_50),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("FIELD", VARCHAR_50),
				Pair.with("OLD_VALUE", VARCHAR_255),
				Pair.with("NEW_VALUE", VARCHAR_255),
				Pair.with("REASON", CLOB_DATATYPE_NAME),
				// rows written by one update share a CHANGE_ID
				Pair.with("CHANGE_ID", VARCHAR_50)));
		// one row per open workspace tab: the thread the owner has open and the chat room behind it
		// (ROOM_ID; the room lives in the inference logs database). Pinned tabs list first, then
		// the most recently active; closing a tab removes the row and keeps the room
		addTable("WORK_OPEN_ROOM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("OPENED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAST_ACTIVE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PINNED", BOOLEAN_DATATYPE_NAME)));
		// one step: a single thing to do (KIND reply, task, waiting_on, errand or approve) and
		// whose it is (STEP_OWNER_ID, a BRAIN_PERSON). A step with TASK_ID serves that work item;
		// one without is a loose step under its topic (LINK_TOPIC_ID). A thread's workspace lists
		// the steps with its THREAD_ID, and WORK_STEP_EVIDENCE holds the messages behind each step
		addTable("WORK_THREAD_STEP", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("STEP_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("STEP_OWNER_ID", VARCHAR_50),
				Pair.with("DUE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ITEM_ID", VARCHAR_50),
				Pair.with("LINK_TOPIC_ID", VARCHAR_50),
				// brain when thread insights made it; EDITED once the owner changes its text, owner, due or kind
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("EDITED", BOOLEAN_DATATYPE_NAME),
				// when the owner last changed its status or whose it is: mail older than that does not undo it
				Pair.with("OWNER_SET_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME),
				// the work item it serves; null is a loose step under its topic. THREAD_ID is null for one added in chat
				Pair.with("TASK_ID", VARCHAR_50),
				// what finished it: owner, or the candidate whose message showed it done
				Pair.with("CLOSED_BY", VARCHAR_50),
				Pair.with("STALE_AT", TIMESTAMP_DATATYPE_NAME),
				// a discussion point's notes and outcome
				Pair.with("NOTE", CLOB_DATATYPE_NAME)));
		// a work item: an outcome the owner is driving, such as shipping a release, as opposed to a
		// single ask (a step). STATUS is active, waiting, blocked, done or dropped. Brain proposes
		// them from mail (ORIGIN brain, STATE proposed until the owner accepts one), and the owner
		// and the assistant add their own. Its links and timeline are in the WORK_TASK_* tables
		addTable("WORK_TASK", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TASK_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TITLE", VARCHAR_255),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_20),
				// a long-running item with no end date: it stays open when all its steps are finished
				Pair.with("ONGOING", BOOLEAN_DATATYPE_NAME),
				Pair.with("NEXT_STEP", CLOB_DATATYPE_NAME),
				Pair.with("WAITING_ON_PERSON_ID", VARCHAR_50),
				// the person the item is about, when WorkTaskUtils.saveWork is given a personId;
				// WorkTaskUtils.listMyWork can filter on it
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("DUE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PRIORITY", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				// proposed until the owner accepts one Brain made
				Pair.with("STATE", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CLOSED_AT", TIMESTAMP_DATATYPE_NAME),
				// the owner's order on the Work page; null sorts after, newest first
				Pair.with("SORT_ORDER", INTEGER_DATATYPE_NAME),
				// the chat room the owner works this item in
				Pair.with("ROOM_ID", VARCHAR_50)));
		// a work item can sit under several topics: the products and the clients it touches. One
		// row per work item and topic; IS_PRIMARY marks the topic a new step takes by default
		addTable("WORK_TASK_TOPIC", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TASK_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("IS_PRIMARY", BOOLEAN_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// the threads that move a work item, one row per work item and thread: STATE proposed
		// (Brain's link), accepted or rejected; rejected links stay so they are not proposed again
		addTable("WORK_TASK_THREAD", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TASK_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// the chat rooms a work item is discussed in; many rooms per task; newest first by
		// CREATED_AT DESC. Each turn, a linked chat's assistant is told of the item (WorkTaskBrief)
		addTable("WORK_TASK_ROOM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TASK_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a work item's timeline: updates, decisions, asks, date and status changes, each with its
		// source. KIND is update, decision, ask, date, status or note; THREAD_ID and MESSAGE_AT
		// point at the mail behind a line; ORIGIN says who wrote it, and Brain's start proposed
		addTable("WORK_TASK_UPDATE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("UPDATE_ID", VARCHAR_50),
				Pair.with("TASK_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("SOURCE_REF", VARCHAR_255),
				Pair.with("MESSAGE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// raw extract output, one row per thing a thread asks, decides or reports; reconcile turns
		// it into steps. Extract (WorkPipeline) pulls tasks, commitments, decisions, updates and
		// done claims from a thread's new messages; reconcile (WorkReconcile) reads the pending
		// rows topic by topic (TOPIC_ID), and RESULT and RESULT_REF record what each became
		addTable("WORK_CANDIDATE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("CANDIDATE_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("MESSAGE_REF", VARCHAR_255),
				Pair.with("MESSAGE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("QUOTE", CLOB_DATATYPE_NAME),
				Pair.with("DUE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ASSIGNEE", VARCHAR_50),
				Pair.with("ASKED_BY", VARCHAR_50),
				Pair.with("ADDRESSING_JSON", CLOB_DATATYPE_NAME),
				Pair.with("TOPIC_ID", VARCHAR_50),
				// evidence, new_step, new_task, closes, waiting_on, update or drop, and what it pointed at
				Pair.with("RESULT", VARCHAR_20),
				Pair.with("RESULT_REF", VARCHAR_50),
				Pair.with("DROP_REASON", VARCHAR_50),
				Pair.with("EXTRACTED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RECONCILED_AT", TIMESTAMP_DATATYPE_NAME)));
		// every message behind a step: one ask however many threads repeat it. One row per step and
		// candidate, with the thread and message; ROLE is source, repeat, reassigned or closed
		addTable("WORK_STEP_EVIDENCE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("STEP_ID", VARCHAR_50),
				Pair.with("CANDIDATE_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("MESSAGE_REF", VARCHAR_255),
				Pair.with("ROLE", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// a fact noted on a thread: its text, who said it (FROM_LABEL, SOURCE_PERSON_ID) and a
		// STATUS. At startup BrainMemoryMigration moves each row into BRAIN_MEMORY, linked to its
		// thread, and deletes it here
		addTable("WORK_THREAD_FACT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("FACT_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("FROM_LABEL", VARCHAR_255),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("SOURCE_PERSON_ID", VARCHAR_50),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// one row per copy in an owner's Digital Twin that a check can compare with what it was copied
		// from (WorkTwinUpdates): COPY_ID names the twin (PROJECT_ID) and the copy's path. COPIED_AT is
		// when the copy was last known current: the file's time when a check first saw it, then each
		// Update. CHANGE_KIND is what the last check found, changed, gone or duplicate, null when
		// nothing; SOURCE_AT is the source's newest change that finding is about. STATUS is open while
		// it waits for the owner and dismissed once they kept the copy as it is. DETAIL_JSON holds the
		// copy's path, kind, item key and drive, and what the page says of the change
		addTable("WORK_TWIN_COPY", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("COPY_ID", VARCHAR_50),
				Pair.with("PROJECT_ID", VARCHAR_50),
				Pair.with("COPIED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CHANGE_KIND", VARCHAR_20),
				Pair.with("SOURCE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("DETAIL_JSON", CLOB_DATATYPE_NAME),
				Pair.with("FOUND_AT", TIMESTAMP_DATATYPE_NAME)));
		// @formatter:on
	}
}
