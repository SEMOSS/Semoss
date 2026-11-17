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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;
import prerna.io.connector.mail.AttachmentText;

// One email attachment for the owner: read live, only while today's rules still show its email,
// and written into the caller's folder. That is an insight folder, which is the room folder once
// the insight is bound to a room. Nothing about the file is kept in the Collaboration database.
public final class BrainAttachments {

	private static final Logger classLogger = LogManager.getLogger(BrainAttachments.class);

	// RDF_Map, else an environment variable: the largest attachment read, in MB
	static final String MAX_MB_SETTING = "COLLAB_ATTACHMENT_MAX_MB";
	private static final int DEFAULT_MAX_MB = 10;
	private static final long MB = 1024L * 1024L;
	// leaves room for the text copy's ".txt" within common file name limits
	private static final int MAX_NAME_CHARS = 150;
	// what this chat's folder already holds; downloads never land on a dot path, so it cannot be overwritten
	private static final String STAGED_FILE = ".claude/attachments.json";
	private static final Object STAGED_LOCK = new Object();
	private static final int DOWNLOADS_AT_ONCE = 2;
	private static final Map<String, Semaphore> DOWNLOAD_SLOTS = new ConcurrentHashMap<>();

	private BrainAttachments() {

	}

	public static Map<String, Object> stage(User user, String folder, String threadId, String messageId,
			String attachmentId, String attachmentName, String fileName, boolean includeText) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		// the assistant asks for many files at once; Graph throttles a mailbox past about 4 requests
		Semaphore slots = DOWNLOAD_SLOTS.computeIfAbsent(owner.getValue0() + "|" + owner.getValue1(),
				key -> new Semaphore(DOWNLOADS_AT_ONCE));
		try {
			slots.acquire();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("The download was stopped.", e);
		}
		try {
			return stage(user, owner.getValue0(), owner.getValue1(), folder, threadId, messageId, attachmentId,
					attachmentName, fileName, includeText, BrainMessageSource.current(), maxBytes());
		} finally {
			slots.release();
		}
	}

	/**
	 * Writes one file attachment of an email on the owner's thread into folder,
	 * under fileName (or the attachment's own name), and with includeText a
	 * {@code <file>.txt} copy of an Office or mail file. The email must still pass
	 * today's rules; excluded senders pass, never-ingest ones do not.
	 */
	static Map<String, Object> stage(User user, String ownerId, String ownerType, String folder, String threadId,
			String messageId, String attachmentId, String fileName, boolean includeText, BrainMessageSource messages,
			long maxBytes) {
		return stage(user, ownerId, ownerType, folder, threadId, messageId, attachmentId, null, fileName, includeText,
				messages, maxBytes);
	}

	// the attachment is named by id (the UI) or by its file name (the assistant, which sees names)
	static Map<String, Object> stage(User user, String ownerId, String ownerType, String folder, String threadId,
			String messageId, String attachmentId, String attachmentName, String fileName, boolean includeText,
			BrainMessageSource messages, long maxBytes) {
		// the email first: a copy staged from an older email on the thread is not a revised file of the same name
		if ((attachmentId == null || attachmentId.isBlank()) && attachmentName != null && !attachmentName.isBlank()) {
			messageId = emailWithFile(user, ownerId, ownerType, threadId, messageId, attachmentName, messages);
		}
		Map<String, Object> reused = reuse(user, ownerId, ownerType, folder, threadId, messageId, attachmentId,
				attachmentName, includeText, messages);
		if (reused != null) {
			return reused;
		}
		BrainThreadMessages.Readable email = BrainThreadMessages.readable(user, ownerId, ownerType, threadId,
				messageId, messages);
		if (email == null) {
			throw new IllegalArgumentException("This email is not available in the thread.");
		}
		messageId = email.graphId();
		if (!"email".equals(email.source())) {
			throw new IllegalArgumentException("Only email attachments can be opened for now.");
		}
		if (attachmentId == null || attachmentId.isBlank()) {
			attachmentId = idByName(user, email.source(), messageId, attachmentName, messages);
		}
		Map<String, Object> attachment;
		try {
			attachment = messages.attachment(user, email.source(), messageId, attachmentId);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Could not describe an attachment on thread {}", threadId, e);
			throw new IllegalStateException("Microsoft could not describe this attachment. Try again.", e);
		}
		if (attachment == null) {
			throw new IllegalArgumentException("This attachment is no longer on the email.");
		}
		Map<String, Object> described = describe(attachment);
		String name = (String) described.get("name");
		if (!"file".equals(described.get("kind"))) {
			throw new IllegalArgumentException("Only attached files can be opened here. Open " + name + " in Outlook.");
		}
		long listedSize = described.get("size") instanceof Long size ? size : 0L;
		if (listedSize > maxBytes) {
			throw new IllegalArgumentException(tooLarge(name, maxBytes));
		}
		String target = baseName(fileName);
		if (target == null) {
			target = baseName(name);
		}
		if (target == null) {
			target = "attachment";
		}

		Path dir = Path.of(folder).toAbsolutePath().normalize();
		Path path = dir.resolve(target).normalize();
		if (!dir.equals(path.getParent()) || Files.isDirectory(path)) {
			throw new IllegalArgumentException("Invalid file name: " + target);
		}
		Path written = null;
		try {
			Files.createDirectories(dir);
			// a partial or oversized download never takes the requested name
			try {
				written = messages.download(user, email.source(), messageId, attachmentId, dir,
						".attachment-" + UUID.randomUUID());
			} catch (SemossPixelException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error("Could not download an attachment on thread {}", threadId, e);
				throw new IllegalStateException(String.valueOf(e.getMessage()).contains("Virus scan blocked")
						? "The virus scan blocked " + name + "."
						: "Microsoft could not send " + name + ". Try again.", e);
			}
			long size = Files.size(written);
			if (size > maxBytes) {
				throw new IllegalArgumentException(tooLarge(name, maxBytes));
			}
			Files.move(written, path, StandardCopyOption.REPLACE_EXISTING);
			written = null;

			Map<String, Object> result = new LinkedHashMap<>();
			result.put("threadId", threadId);
			result.put("messageId", messageId);
			result.put("attachmentId", attachmentId);
			result.put("name", name);
			if (described.get("contentType") != null) {
				result.put("contentType", described.get("contentType"));
			}
			result.put("size", size);
			result.put("filePath", target);
			if (includeText && AttachmentText.supports(name)) {
				addText(result, path, name, email);
			}
			record(dir, result);
			return result;
		} catch (IOException e) {
			classLogger.error("Could not save an attachment on thread {}", threadId, e);
			throw new IllegalStateException("Could not save " + name + ". Try again.", e);
		} finally {
			if (written != null) {
				try {
					Files.deleteIfExists(written);
				} catch (IOException e) {
					classLogger.warn("Could not remove a partial attachment download", e);
				}
			}
		}
	}

	// the attachment this chat already downloaded, so asking again reuses the file instead of Graph;
	// the email must still pass today's rules, as for a fresh download
	private static Map<String, Object> reuse(User user, String ownerId, String ownerType, String folder,
			String threadId, String messageId, String attachmentId, String attachmentName, boolean includeText,
			BrainMessageSource messages) {
		Path dir = Path.of(folder).toAbsolutePath().normalize();
		Map<String, Object> entry = staged(dir, threadId, messageId, attachmentId, attachmentName);
		if (entry == null) {
			return null;
		}
		Path path = dir.resolve(String.valueOf(entry.get("filePath"))).normalize();
		if (!dir.equals(path.getParent()) || !Files.isRegularFile(path)) {
			return null;
		}
		BrainThreadMessages.Readable email = BrainThreadMessages.readable(user, ownerId, ownerType, threadId,
				String.valueOf(entry.get("messageId")), messages);
		if (email == null) {
			throw new IllegalArgumentException("This email is not available in the thread.");
		}
		Map<String, Object> result = new LinkedHashMap<>(entry);
		result.remove("textPath");
		try {
			result.put("size", Files.size(path));
		} catch (IOException e) {
			return null;
		}
		result.put("alreadyInChat", true);
		String name = String.valueOf(entry.get("name"));
		if (includeText && AttachmentText.supports(name)) {
			Path text = path.resolveSibling(path.getFileName() + ".txt");
			if (Files.isRegularFile(text)) {
				result.put("textPath", text.getFileName().toString());
			} else {
				addText(result, path, name, email);
			}
		}
		return result;
	}

	// by attachment id when one is passed, else by the file name the assistant sees on the thread, from the same email
	// when it is known
	@SuppressWarnings("unchecked")
	private static Map<String, Object> staged(Path dir, String threadId, String messageId, String attachmentId,
			String attachmentName) {
		boolean byId = attachmentId != null && !attachmentId.isBlank();
		if (!byId && (attachmentName == null || attachmentName.isBlank())) {
			return null;
		}
		for (Object item : readStaged(dir)) {
			if (item instanceof Map<?, ?> map) {
				Map<String, Object> entry = (Map<String, Object>) map;
				if (!threadId.equals(entry.get("threadId"))) {
					continue;
				}
				if (byId ? attachmentId.equals(entry.get("attachmentId"))
						: looseName(attachmentName).equals(looseName(String.valueOf(entry.get("name"))))
								&& (messageId == null || messageId.equalsIgnoreCase(String.valueOf(entry.get("messageId"))))) {
					return entry;
				}
			}
		}
		return null;
	}

	// a file saved under the same name replaces the older entry
	private static void record(Path dir, Map<String, Object> result) {
		synchronized (STAGED_LOCK) {
			List<Object> entries = new ArrayList<>();
			for (Object item : readStaged(dir)) {
				if (item instanceof Map<?, ?> map && !result.get("filePath").equals(map.get("filePath"))) {
					entries.add(item);
				}
			}
			Map<String, Object> entry = new LinkedHashMap<>();
			for (String key : List.of("threadId", "messageId", "attachmentId", "name", "contentType", "filePath")) {
				if (result.get(key) != null) {
					entry.put(key, result.get(key));
				}
			}
			entries.add(entry);
			Path file = dir.resolve(STAGED_FILE);
			try {
				Files.createDirectories(file.getParent());
				Files.writeString(file, CollaborationDbUtils.toJson(entries), StandardCharsets.UTF_8);
			} catch (IOException e) {
				// the download itself still worked; the next ask just downloads again
				classLogger.warn("Could not record a downloaded attachment", e);
			}
		}
	}

	private static List<Object> readStaged(Path dir) {
		Path file = dir.resolve(STAGED_FILE);
		if (!Files.isRegularFile(file)) {
			return List.of();
		}
		synchronized (STAGED_LOCK) {
			try {
				List<Object> entries = CollaborationDbUtils.parseList(Files.readString(file, StandardCharsets.UTF_8));
				return entries == null ? List.of() : entries;
			} catch (Exception e) {
				classLogger.warn("Could not read the downloaded attachments list", e);
				return List.of();
			}
		}
	}

	// the id of the one file attachment on the email with this name; the error lists the names there are
	private static String idByName(User user, String source, String messageId, String attachmentName,
			BrainMessageSource messages) {
		if (attachmentName == null || attachmentName.isBlank()) {
			throw new IllegalArgumentException("Pass the attachment's name or id.");
		}
		List<Map<String, Object>> listed;
		try {
			listed = messages.attachments(user, source, List.of(messageId)).getOrDefault(messageId, List.of());
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Could not list the attachments of an email", e);
			throw new IllegalStateException("Microsoft could not list this email's attachments. Try again.", e);
		}
		List<String> names = new ArrayList<>();
		List<Object> ids = new ArrayList<>();
		List<Object> loose = new ArrayList<>();
		for (Map<String, Object> item : listed) {
			if ("file".equals(item.get("kind"))) {
				names.add(String.valueOf(item.get("name")));
				if (attachmentName.strip().equalsIgnoreCase(String.valueOf(item.get("name")))) {
					ids.add(item.get("id"));
				}
				if (looseName(attachmentName).equals(looseName(String.valueOf(item.get("name"))))) {
					loose.add(item.get("id"));
				}
			}
		}
		// models retype "Q3_Plan 1.pdf" as "Q3 Plan 1.pdf"; an exact name still wins
		if (ids.isEmpty() && loose.size() == 1) {
			ids = loose;
		}
		if (ids.size() == 1) {
			return String.valueOf(ids.get(0));
		}
		throw new IllegalArgumentException((ids.isEmpty() ? "This email has no attached file named " + attachmentName
				: "More than one attached file on this email is named " + attachmentName)
				+ ". Its attached files: " + (names.isEmpty() ? "none" : String.join(", ", names)) + ".");
	}

	/**
	 * The thread's newest email carrying a file of that name. The assistant names files
	 * reliably but cannot copy 150-character email ids, so its id is only a hint; the
	 * passed id is returned unchanged when it is on the thread or nothing matches.
	 */
	private static String emailWithFile(User user, String ownerId, String ownerType, String threadId,
			String messageId, String attachmentName, BrainMessageSource messages) {
		List<String> ids = BrainThreadMessages.emailGraphIds(ownerId, ownerType, threadId);
		if (ids.isEmpty() || messageId != null && ids.stream().anyMatch(messageId::equalsIgnoreCase)) {
			return messageId;
		}
		Map<String, List<Map<String, Object>>> listed;
		try {
			listed = messages.attachments(user, "email", ids);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Could not list the attachments of thread {}", threadId, e);
			if (messageId == null || messageId.isBlank()) {
				throw new IllegalStateException("Microsoft could not list this thread's attachments. Try again.", e);
			}
			return messageId;
		}
		String wanted = looseName(attachmentName);
		String found = ids.stream()
				.filter(id -> listed.getOrDefault(id, List.of()).stream().anyMatch(
						item -> "file".equals(item.get("kind")) && wanted.equals(looseName(String.valueOf(item.get("name"))))))
				.findFirst().orElse(null);
		if (found != null || (messageId != null && !messageId.isBlank())) {
			return found != null ? found : messageId;
		}
		// Graph leaves out an email it could not list (still throttled after retries)
		if (!listed.keySet().containsAll(ids)) {
			throw new IllegalStateException("Microsoft is busy and could not list this thread's attachments. Try again.");
		}
		throw new IllegalArgumentException("No email on this thread has an attached file named " + attachmentName + ".");
	}

	private static String looseName(String name) {
		return name.strip().toLowerCase(java.util.Locale.ROOT).replaceAll("[\\s_-]+", " ");
	}

	// a failed text copy still leaves the file itself usable
	private static void addText(Map<String, Object> result, Path file, String name,
			BrainThreadMessages.Readable email) {
		try {
			AttachmentText.Extracted text = AttachmentText.extract(file, name, header(name, email),
					AttachmentText.MAX_CHARS);
			if (text == null) {
				return;
			}
			String textName = file.getFileName() + ".txt";
			Files.writeString(file.resolveSibling(textName), text.text(), StandardCharsets.UTF_8);
			result.put("textPath", textName);
			if (text.truncated()) {
				result.put("textTruncated", true);
			}
		} catch (EncryptedDocumentException e) {
			result.put("textError", "This file is password protected, so its text could not be read.");
		} catch (Exception e) {
			classLogger.warn("Could not read the text of an attachment", e);
			result.put("textError", "The text of this file could not be read.");
		}
	}

	// names the file and its email, since a text document reaches the model without a file name
	static String header(String name, BrainThreadMessages.Readable email) {
		StringBuilder out = new StringBuilder("Attachment: ").append(name).append("\nFrom the email");
		if (email.message().get("subject") instanceof String subject && !subject.isBlank()) {
			out.append(" \"").append(subject.strip()).append('"');
		}
		Object from = email.sender().get("name") instanceof String sender && !sender.isBlank() ? sender
				: email.sender().get("address");
		if (from != null) {
			out.append(" sent by ").append(from);
		}
		if (email.at() != null) {
			out.append(" on ").append(email.at());
		}
		return out.append("\n\n").toString();
	}

	/**
	 * A Graph attachment of any kind as the UI and the model see it, never with
	 * bytes: kind is file (bytes on the message), item (an attached email or
	 * event) or link (a file in OneDrive or SharePoint).
	 */
	static Map<String, Object> describe(Map<String, Object> attachment) {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("id", attachment.get("id"));
		out.put("name", attachment.get("name") instanceof String name && !name.isBlank() ? name.strip() : "Attachment");
		if (attachment.get("contentType") instanceof String type && !type.isBlank()) {
			out.put("contentType", type);
		}
		if (attachment.get("size") instanceof Number size) {
			out.put("size", size.longValue());
		}
		Object type = attachment.get("@odata.type");
		String kind = "file";
		if (type != null) {
			String value = type.toString();
			kind = value.endsWith("fileAttachment") ? "file"
					: value.endsWith("itemAttachment") ? "item" : value.endsWith("referenceAttachment") ? "link" : "other";
		}
		out.put("kind", kind);
		out.put("isInline", Boolean.TRUE.equals(attachment.get("isInline")));
		return out;
	}

	/**
	 * The last segment of a requested name, without leading dots or control
	 * characters, so a write can neither leave the folder nor land on a hidden
	 * working file; null when nothing usable is left.
	 */
	static String baseName(String fileName) {
		if (fileName == null) {
			return null;
		}
		String name = fileName.replace('\\', '/');
		name = name.substring(name.lastIndexOf('/') + 1).replaceAll("\\p{Cntrl}", "").strip();
		while (name.startsWith(".")) {
			name = name.substring(1);
		}
		if (name.length() > MAX_NAME_CHARS) {
			int dot = name.lastIndexOf('.');
			String extension = dot > 0 && name.length() - dot <= 10 ? name.substring(dot) : "";
			name = name.substring(0, MAX_NAME_CHARS - extension.length()) + extension;
		}
		return name.isBlank() ? null : name;
	}

	static long maxBytes() {
		String value = Utility.getDIHelperProperty(MAX_MB_SETTING);
		if (value == null || value.isBlank()) {
			value = System.getenv(MAX_MB_SETTING);
		}
		if (value == null || value.isBlank()) {
			return DEFAULT_MAX_MB * MB;
		}
		try {
			return Math.max(1, Integer.parseInt(value.trim())) * MB;
		} catch (NumberFormatException e) {
			classLogger.warn("{} is not a whole number of MB, using {}", MAX_MB_SETTING, DEFAULT_MAX_MB);
			return DEFAULT_MAX_MB * MB;
		}
	}

	private static String tooLarge(String name, long maxBytes) {
		return name + " is larger than the " + (maxBytes / MB) + " MB limit.";
	}
}
