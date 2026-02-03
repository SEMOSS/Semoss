/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class DuplicateRoomReactor extends AbstractReactor {

 private static final Logger classLogger = LogManager.getLogger(DuplicateRoomReactor.class);

 public DuplicateRoomReactor() {
  this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), "newRoomId", "newRoomName", "copyFiles",
    "copyCompaction" };
  this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
 }

 @Override
 public NounMetadata execute() {
  organizeKeys();
  User user = this.insight.getUser();
  if (user == null) {
   throw new IllegalArgumentException("You are not properly logged in");
  }

  String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
  if (roomId == null || roomId.trim().isEmpty()) {
   throw new IllegalArgumentException("Room id is required");
  }

  Room sourceRoom = RoomUtils.getOrLoadRoom(roomId, this.insight);
  String newRoomId = this.keyValue.get("newRoomId");
  if (newRoomId == null || newRoomId.trim().isEmpty()) {
   newRoomId = GUID.v7().toUUID().toString();
  }

  if (ModelInferenceLogsUtils.doCheckRoomExists(newRoomId)) {
   throw new IllegalArgumentException("Room id already exists: " + newRoomId);
  }

  boolean copyFiles = parseBoolean(this.keyValue.get("copyFiles"), true);
  boolean copyCompaction = parseBoolean(this.keyValue.get("copyCompaction"), false);

  String newRoomName = this.keyValue.get("newRoomName");
  if (newRoomName == null || newRoomName.trim().isEmpty()) {
   String baseName = sourceRoom.getRoomName();
   if (baseName == null || baseName.trim().isEmpty()) {
    baseName = "Room";
   }
   newRoomName = "Copy of " + baseName;
  }

  Map<String, Object> options = new HashMap<>(sourceRoom.getOptionsMap());
  if (!copyCompaction) {
   options.remove("compaction");
  }

  String workspaceId = extractWorkspaceId(options);
  String roomContext = readRoomContext(user.getPrimaryLoginToken().getId(), roomId);
  String modelId = sourceRoom.getModelId();
  String projectId = sourceRoom.getProjectId();
  String projectName = null;
  if (projectId != null && !projectId.trim().isEmpty()) {
   IProject project = Utility.getProject(projectId);
   projectName = project != null ? project.getProjectName() : null;
  }

  ModelInferenceLogsUtils.doCreateNewConversation(this.insight.getInsightId(), newRoomId, newRoomName,
    roomContext, user.getPrimaryLoginToken().getId(), user.getPrimaryLoginToken().getName(),
    user.getPrimaryLoginToken().getEmail(), null, modelId, true, projectId, projectName, workspaceId,
    options);

  if (sourceRoom.isPinned()) {
   ModelInferenceLogsUtils.doSetRoomToPinned(user.getPrimaryLoginToken().getId(), newRoomId, true);
  }

  String messagesJson = MessageUtils.toJsonArray(sourceRoom.getMessages());
  if (modelId != null && !modelId.trim().isEmpty()) {
   ModelInferenceLogsUtils.llm2_updateRoomMessages(newRoomId, user.getPrimaryLoginToken().getId(),
     messagesJson, newRoomName, modelId);
  } else {
   ModelInferenceLogsUtils.llm2_updateRoomMessages(newRoomId, user.getPrimaryLoginToken().getId(),
     messagesJson);
  }

  int copiedFiles = 0;
  if (copyFiles) {
   copiedFiles = copyRoomFiles(roomId, newRoomId);
  }

  Map<String, Object> result = new HashMap<>();
  result.put("roomId", roomId);
  result.put("newRoomId", newRoomId);
  result.put("newRoomName", newRoomName);
  result.put("copiedFiles", copiedFiles);
  result.put("copiedCompaction", copyCompaction);
  return new NounMetadata(result, PixelDataType.MAP);
 }

 private static boolean parseBoolean(String value, boolean defaultValue) {
  if (value == null || value.trim().isEmpty()) {
   return defaultValue;
  }
  return Boolean.parseBoolean(value.trim());
 }

 private static String readRoomContext(String userId, String roomId) {
  List<Map<String, Object>> output = ModelInferenceLogsUtils.getRoomContext(userId, roomId);
  if (output == null || output.isEmpty()) {
   return null;
  }
  Map<String, Object> row = output.get(0);
  if (row.containsKey("ROOM_CONTEXT")) {
   Object value = row.get("ROOM_CONTEXT");
   return value == null ? null : String.valueOf(value);
  }
  if (row.containsKey("room_context")) {
   Object value = row.get("room_context");
   return value == null ? null : String.valueOf(value);
  }
  return null;
 }

 private static String extractWorkspaceId(Map<String, Object> options) {
  if (options == null || options.isEmpty()) {
   return null;
  }
  Object workspaceObj = options.get("workspace");
  if (workspaceObj instanceof String) {
   String val = ((String) workspaceObj).trim();
   return val.isEmpty() ? null : val;
  }
  if (workspaceObj instanceof Map) {
   Object idObj = ((Map<?, ?>) workspaceObj).get("workspace_id");
   if (idObj != null) {
    String val = String.valueOf(idObj).trim();
    return val.isEmpty() ? null : val;
   }
  }
  return null;
 }

 private static int copyRoomFiles(String sourceRoomId, String targetRoomId) {
  ClusterUtil.pullRoom(sourceRoomId);
  Path source = Paths.get(Utility.getBaseFolder(), "room", sourceRoomId);
  if (!Files.exists(source)) {
   return 0;
  }
  Path target = Paths.get(Utility.getBaseFolder(), "room", targetRoomId);
  try {
   Files.createDirectories(target);
  } catch (IOException e) {
   classLogger.warn("Failed to create room folder {}", target, e);
   return 0;
  }

  CopyCounter counter = new CopyCounter();
  try {
   Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
     Path rel = source.relativize(dir);
     Path dest = target.resolve(rel);
     Files.createDirectories(dest);
     return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
     Path rel = source.relativize(file);
     Path dest = target.resolve(rel);
     Files.copy(file, dest, StandardCopyOption.REPLACE_EXISTING);
     counter.count++;
     return FileVisitResult.CONTINUE;
    }
   });
  } catch (IOException e) {
   classLogger.warn("Failed to copy room files from {} to {}", source, target, e);
  }

  ClusterUtil.pushRoom(targetRoomId);
  return counter.count;
 }

 private static class CopyCounter {
  private int count = 0;
 }
}
 