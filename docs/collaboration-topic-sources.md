# Topic sources

A source belongs to the signed-in Collaboration owner. Explicit many-to-many links make the same source available to several topics; linking does not copy it or grant another user access. Topics remain personal owner-scoped records, consistent with the existing Brain model.

## Source ownership and lifecycle

- **Documents:** uploading or importing an owned room file creates a durable, user-owned snapshot. The original is copied into the owner's existing user asset storage and the bounded extracted text is stored in the Collaboration database. Deleting the temporary upload or closing a room does not remove the topic copy. A file downloaded from M365 into a room is imported as a saved copy, not a live provider reference.
- **Notes:** reference text stored independently of optional assistant memory. Turning memory off does not disable explicitly linked topic reference notes.
- **Links:** HTTP/HTTPS references. SEMOSS never fetches arbitrary link URLs. A link without explicitly saved text is `reference`, not evidence of the remote document's contents. Explicit text accompanying a link is a user-provided snapshot; it does not claim live synchronization or continuing provider permission.
- Updating creates an immutable version. `expectedVersion` rejects a stale editor; database compare-and-set and unique keys protect concurrent version writes. Updating a shared source updates its current version in every topic that links it. Historical citations retain their saved version. When their original topic has been merged, inspection follows the owner-scoped, non-undone merge history to a destination that still explicitly links the source; unlinking there invalidates inspection rather than falling back to another topic.
- Unlink removes only one topic association. Canonical sources and their versions remain in the owner's source library for explicit reuse. Topic deletion removes associations, preserving source records. This deliberately does not implement permanent source deletion or retention expiry.
- Renaming uses stable topic IDs. Merge unions source links and removes duplicates. Merge/delete undo restores the associations through the existing conflict-checked topic snapshot mechanism. Archive preserves source links for inspection but excludes the topic from assistant scope and rejects source changes until restored. Existing archive behavior removes chat-topic links; restoration does not automatically relink chats.

## Pixel API

| Reactor | Arguments | Result |
| --- | --- | --- |
| `BrainListTopicSources` | optional `topicId` | `{topicId, sources}`; omission lists all sources belonging to the owner |
| `BrainGetTopicSource` | `topicId`, `sourceId`, optional `version` | full metadata, `content`, `citation`; rechecks ownership and topic association |
| `BrainSaveTopicSource` | `topicId`, `title`, `kind=document|note|link`, optional `sourceId`, `content`, `filePath`, `url`, `expectedVersion`, `roomId` | saved source/version and topic association |
| `BrainLinkTopicSource` | `topicId`, `sourceId`, optional `remove` | updated topic source list; no source copy or source deletion |
| `BrainListTopicSourceFiles` | `roomId` | `{roomId,files:[{name,path,size}]}` for an owned active room, at most 200 supported files and five directory levels |
| `BrainSearchTopicSources` | optional `query`, `topicId` | bounded lexical results within the authorized insight's assistant-room topics |
| `BrainReadTopicSource` | `sourceId`, optional `topicId`, `version`, `offset` | up to 12,000 characters, `hasMore`, `nextOffset`, exact locator and citation marker |

`filePath` is relative to the current insight, or to the explicitly selected owned active room. Absolute paths, traversal and symlink escapes are rejected using real path containment. Monolith uploads return virtual-root `fileLocation` values such as `/report.pdf`; clients remove that leading slash before passing the relative path. Files are capped at 10 MiB. Supported extraction includes PDF, Office/mail using the existing attachment extractor, and UTF-8 text/Markdown/CSV/JSON/XML/HTML/log files. Saved text is capped at 120,000 characters; document truncation is reported with `truncated=true`. Notes and explicitly provided link text exceeding the cap are rejected. Documents with no readable text are rejected with an OCR explanation; OCR is not silently assumed.

## Assistant scope and citations

The server derives scope from the owned assistant room's applying topics, using the existing confirmed links, included threads and exclusion rules. The model cannot widen scope by supplying an arbitrary topic ID. Source search is bounded lexical retrieval, with short-term fallback for multiword questions; no vector engine or embeddings service is required. `ReadTopicSource` supports subsequent passages through `nextOffset`.

Topic-bound owner assistant runs suppress general OneDrive/Teams file discovery/download and direct unclassified Outlook reads. The execution path also rejects those tools when called by stale name. Brain mail search and thread/attachment reads are constrained to the room's topics. `ListTopics` and explicit memory searches in these runs cannot widen to unrelated topics. Existing profile/preferences and automatic memory recall retain their prior independent product semantics; this feature does not convert documents into memories or replace the agent's general code-execution sandbox.

Source link/version and applying-topic changes create a new provider-context boundary at the next ordinary user turn. The stored audit transcript remains intact, but the model receives only messages after that boundary, preventing earlier source tool payloads and assistant paraphrases from silently returning after unlink/archive/update. An already-running model request cannot be retracted; source tools reauthorize every read and final citation attachment rechecks current scope. Paused runs restore evidence from their persisted, server-owned tool results.

Only evidence actually returned by the trusted topic source tools and referenced by an exact `[[source:<sourceId>:v<version>]]` marker becomes a final response citation. Reference-only URL metadata cannot become evidence of remote contents. Final `ResponseMessage.ornaments.topicSources` contains `{sourceId,version,topicId,title,excerpt,locator,citationId}`; existing room message persistence retains it across reloads. Citation inspection uses `BrainGetTopicSource(topicId,sourceId,version)` and rechecks the current association.

## Deployment and checks

The existing Collaboration schema initializer adds the source, immutable-version and association tables and their unique keys during backend startup. Restart the backend after deployment so schema synchronization and reactor discovery run. Focused tests cover cross-owner access, scoped retrieval, source sharing/unlink, versions/conflicts, durable upload and room imports, path/symlink checks, archive/merge/delete/undo, provider-context boundaries and persisted citation ornaments.

Focused validation completed with 41 tests, zero failures/errors: `BrainTopicSourcesUnitTests` (12), `BrainTopicBriefUnitTests` (6), `BrainThreadRoomDbUnitTests` (15), `RoomMessageStoreUnitTests` (5), `TopicSourceCitationsUnitTests` (2), and `SemossAgentPromptUnitTests` (1). Command from the Semoss checkout:

```sh
JAVA_HOME=/Users/ibrahim/.sdkman/candidates/java/21.0.6-tem ../apache-maven-3.9.12/bin/mvn -q -Dlicense.skip=true -Dtest=BrainTopicSourcesUnitTests,BrainTopicBriefUnitTests,BrainThreadRoomDbUnitTests,RoomMessageStoreUnitTests,TopicSourceCitationsUnitTests,SemossAgentPromptUnitTests test
```

The checks use an H2 Collaboration schema and mocked provider/model boundaries. Live Tomcat startup/schema migration, cloud storage synchronization and model-generated tool/citation behavior require deployment validation; these tests do not claim live M365 or model verification.
