# Review / Optimize Brain local test guide

## What is available

Both repos have a separate `feat/collaboration-optimize-brain` branch based on `origin/dev`. Build/restart the backend and run or deploy the Collaboration UI from these branches together. No schema migration is required. Existing global-search work is on its own branch and is not included.

Generated mailbox copy on this machine:

`/Users/imahaba/Documents/SEMOSS/workspace/collaboration-docs/review-fixture/mail.json`

The directory also contains `teams.json`, `manifest.json`, `setup.pixel`, and `import-90-days.pixel`. It has **682 messages**, including **18 added messages in 17 conversations**. The original search mailbox remains unchanged. Updating a JSON file does not import it into an account.

To regenerate with dates relative to the day of testing, run from the backend repo:

```sh
python3 test/resources/collaboration/review/generate_review_fixture.py \
  --base-mail /Users/imahaba/Documents/SEMOSS/workspace/collaboration-docs/search-fixture/mail.json \
  --output /Users/imahaba/Documents/SEMOSS/workspace/collaboration-docs/review-fixture
```

Keep the output directory separate from the input. The generator refuses to overwrite the input mailbox and verifies its hash. Dates in the copied base mailbox and Teams fixture move together. The generated fixture is local; the generator is versioned so another developer can produce it from their base mailbox.

## Connect the local fixture

1. Set the backend's existing `COLLABORATION_FIXTURE_MAIL` setting in local `RDF_Map.prop` to the full Review `mail.json` path above. Restart the backend so the setting is loaded. This local configuration has deliberately not been changed by this implementation.
2. Use a local test account with Collaboration enabled and email enabled. A fresh account gives predictable state; no database reset is required. A configured, accessible topic/classifier model is needed for model-generated filing, new-topic and merge proposals. Inline filing and memory decisions do not require Optimize to succeed.
3. Import the mailbox. New accounts can use onboarding; choose last month. Existing accounts must start a new import, rather than relying on the old completed import. In an authenticated SEMOSS Pixel runner execute `BrainImportMail(days=[90]);` and wait for completion. The supplied `import-90-days.pixel` contains that command. Ninety days includes the old audit conversation; a 30-day import deliberately excludes it. Source settings are preserved.
4. In the same authenticated account, run the contents of generated `setup.pixel` **once**. It uses real owner-scoped APIs to save three topics and two synthetic memories. Running it again can create duplicate topics or confirm the existing memories. A mailbox by itself cannot create these saved review contexts.
5. Open **Brain → Review**. Do this before running any extra classifier/sort if you want to see all nine freshly imported unfiled targets. Import saves mail but does not itself classify it. Existing background classification or earlier decisions can change how many of them remain unfiled.

You can verify import through the existing Threads view using the `ReviewCase` subject prefix. Do not use global search as a prerequisite on this branch, because its separate fix is not in dev. If the Threads view shows only its first page, use its paging/filtering or verify with the owner-scoped `BrainListThreads` API. `manifest.json` lists the exact added subjects and conversation IDs.

## UI scenarios and expected behavior

### 1. Previously silent unfiled conversations

In Threads without a topic, locate `ReviewCase Unfiled:` conversations: Cedar Relay, Maple Observatory and Saffron Logistics, three each. Use **Load more questions** if needed; old mailbox questions also appear.

Expected: each unfiled conversation has an inline topic selector and File thread action. You do not need to leave Review. Save one into an existing topic. Its card disappears, recent-mail unfiled count decreases by one and filing coverage rises. The automatic percentage must not rise just because you manually filed it. The questions-answered count increases.

Switch to **Learned recently**. Only your answers appear, not every confirmed classifier link. Undo the answer. The filing is restored to its previous state, the question returns, and the health numbers reflect the reversal. Reload the page to confirm persistence.

The total unfiled count is not necessarily nine: the base mailbox contributes additional conversations. Nine is the exact number of added unfiled targets before any classification, verified separately in the database regression test.

### 2. Explicit wrong-topic correction

Open Topic changes you requested → Start a topic change. In Thread to move, select a saved conversation and click Open topic change. This action opens a request; it is not a filter. Its current topic must remain unchanged until you choose a replacement on the inline correction card and click Save topic change. Then open the conversation/topic context to verify the new filing. Undo restores the original filing, including the primary topic. Task statuses must remain unchanged.

### 3. Optimize progress and proposal-only behavior

Click **Optimize Brain**. Expect a disabled running button, progress and a changing step. Completion shows counts of newly created filing, new-topic, merge, people and quiet-topic proposals. Browse the sections for suggested topic matches, threads without a topic, requested topic changes, new topics, duplicate topics, quiet topics, topic members, frequent senders and memories.

Before accepting anything, existing topic links, topics and memberships must remain unchanged. Run Optimize again: unchanged pending or already answered proposals must not duplicate. A completed repeat can report zero newly found suggestions while earlier questions remain visible.

New-topic names/counts and likely merges depend on the model. The two saved Northwind topics deliberately overlap; the three unfiled groups provide material for new-topic discovery. Do not expect an exact “6 filings, 2 topics, 1 merge” from a real model. Deterministic automated tests cover those action types without relying on model wording.

### 4. People

Before Optimize, file the three `ReviewCase Membership: Northwind migration delivery` conversations into `ReviewCase Northwind Migration`, leaving Casey Ng outside the topic's membership. Keep Riley Park's three `ReviewCase Unknown:` conversations unfiled.

Optimize should propose adding Casey to that topic and prompt for frequent unknown sender Riley (at least three recent conversations, no membership). Accept a membership proposal or select a topic for the unknown sender. Check the topic's People and then Undo from Learned recently. An explicit previously removed membership is respected and is not proposed again automatically.

### 5. Quiet topic

Import 90 days, then use inline Filing to file `ReviewCase Legacy Audit: historical handover` into `ReviewCase Legacy Audit`. The two messages belong to one conversation dated 45 days ago. Run Optimize.

Expected: Topics offers Archive for the quiet topic. It remains saved until accepted. Archive keeps its history. Undo restores its status. The old conversation is outside the last-seven-days health denominator, so filing it does not increase recent-mail coverage.

### 6. Memory

The one-time setup creates one suggested preference and one unconfirmed assistant-learned preference, both prefixed `ReviewCase`.

Expected: Memories to review offers Keep for the suggestion and Confirm for the learned preference, plus Dismiss. Accept one, check its decision log entry, then Undo. The original memory state returns. An older question cannot confirm changed memory text; refresh replaces it with a question about the current text. Replaced memories are restored by Undo too.

### 7. Failure, keyboard and paging

- Slow the network: loading is distinct from empty sections. Start Optimize and reload Review; it reconnects to the exact current owner job.
- Block the Review overview request in browser developer tools: an error and Retry appear, not an empty “all caught up” state. Unblock and retry.
- Make the configured model unavailable: Optimize reports failure; any already created questions remain available. No proposed filing should have been applied automatically.
- Use Tab/Shift+Tab and Enter/Space for Optimize, tabs, topic selectors, answers, Load more and Undo. Answering retains a useful focus location in its section. Missing required topic choices show an associated field error.
- Use Load more questions beyond 50 and Load more decisions beyond 20. Counts reflect all matching rows; each section and the overall list explicitly say Showing X of Y questions. Section totals include all open questions, including older ones; the health line still uses its seven-day window.
- After a filing answer, change that filing elsewhere, then Undo: expect a stale-change error and the newer edit to remain intact.
- A different account must not see these questions or decisions or answer them by ID.

## Automated verification

From the backend repo (the Java agent avoids dynamic-attach restrictions):

```sh
mvn -q -Dtest=BrainReviewWorkflowTest \
  -DbrainReviewFixture=/Users/imahaba/Documents/SEMOSS/workspace/collaboration-docs/review-fixture/mail.json \
  -DargLine=-javaagent:/Users/imahaba/.m2/repository/org/mockito/mockito-core/5.18.0/mockito-core-5.18.0.jar test
```

From the UI repo:

```sh
pnpm --filter @semoss/collaboration test src/features/brain-review src/features/collaboration src/features/onboarding src/pages/router.test.ts
pnpm --filter @semoss/collaboration build
pnpm --filter @semoss/collaboration type-check
```

Type-check currently has 13 pre-existing missing-`messageId` errors in five unrelated test files. The Work page has a pre-existing goal strike-through assertion failure. The full suite also exposes an email-draft stale-recipient failure reproduced on clean dev and a router lazy-load timeout under load; the focused router test passes. These are documented in the implementation record. No live browser end-to-end run or import into your account has been performed by this implementation.

## If onboarding reports threads could not be sorted

The October 16 local run failed because the configured Python virtual environment lacked the TypeSafe dependencies. Check the first current backend exception before retrying repeatedly. For `ModuleNotFoundError: httpx2`, use the interpreter configured in `PYTHONHOME` (not the terminal's default Python). This machine was repaired with:

```sh
/Users/imahaba/Documents/SEMOSS/.venvs/semoss-py312/bin/python -m pip install httpx2==2.12.0 typesafe-sdk==0.7.1
```

Restart Tomcat after installation and Retry the sorting step. The package import and 32 existing TypeSafe client tests passed after this repair. A live model call can still expose a separate access, network or service issue. No reset or reimport is needed to retry classification of the saved threads.

## Clarity checks after the October 16 revision

- Section headings contain no bare “(50)” count. Each section shows “Showing X of Y questions.” Load more must increase X while retaining existing suggested-topic buttons.
- Owner-requested changes appear separately from Brain suggestions and threads without a topic. Existing requests should show the thread subject and its current topic names after the backend restart; no reseeding is needed.
- “Start a topic change” opens an explicit form, not a filter. Opening a request must not move the thread. Only Save topic change applies the new selection; Cancel request leaves the thread as it is.
- A failed Optimize request shows a short message. Technical details starts closed and can be opened with keyboard or pointer to inspect the original error.
