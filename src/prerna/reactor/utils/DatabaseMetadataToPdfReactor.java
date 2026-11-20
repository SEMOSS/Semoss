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
package prerna.reactor.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.w3c.dom.Document;
import org.xhtmlrenderer.pdf.ITextRenderer;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.masterdatabase.utility.MetamodelVertex;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class DatabaseMetadataToPdfReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DatabaseMetadataToPdfReactor.class);
	private static final String CLASS_NAME = DatabaseMetadataToPdfReactor.class.getName();

	private static final DateTimeFormatter REPORT_TIME_FORMATTER = DateTimeFormatter
			.ofPattern("MMMM dd, yyyy 'at' hh:mm a z");
	private static final String REPORT_STYLES = """
			body { font-family: Arial, Helvetica, sans-serif; margin: 25px; }
			.header { text-align: center; padding-bottom: 20px; border-bottom: 1px solid #ddd; margin-bottom: 20px; }
			.header h1 { margin: 0; font-size: 22px; }
			.header h2 { margin: 5px 0; font-size: 14px; color: #555; }
			.summary { background-color: #f9f9f9; border: 1px solid #ddd; padding: 15px; margin-bottom: 30px; border-radius: 5px; }
			.summary h3 { margin-top: 0; border-bottom: 1px solid #ccc; padding-bottom: 5px; font-size: 18px; }
			.summary p { margin: 5px 0; font-size: 13px; line-height: 1.25; }
			.summary .tags { display: inline-block; background-color: #e1e1e1; color: #333; padding: 5px 10px; margin: 5px 5px 5px 0; border-radius: 15px; font-size: 12px; }
			.content h3 { font-size: 18px; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; margin-top: 30px; }
			.content h4 { font-size: 13px; margin-top: 25px; }
			table { width: 100%; border-collapse: collapse; margin-top: 15px; table-layout: fixed; font-size: 9px; }
			th, td { padding: 6px; text-align: left; border: 1px solid #ddd; word-wrap: break-word; line-height: 1.1; }
			thead th { font-size: 9px !important; font-weight: 600; }
			tbody td { font-size: 9px !important; font-weight: 400; }
			thead { background-color: #4CAF50; color: white; }
			tbody tr:nth-child(even) { background-color: #f2f2f2; }
			th.col-name { width: 15%; }
			th.col-logical-type { width: 15%; }
			th.col-physical-type { width: 15%; }
			th.col-logical-names { width: 20%; }
			th.col-description { width: 35%; }
			.footer { text-align: center; margin-top: 30px; font-size: 12px; color: #777; }
			""";
	private static final String TABLE_HEADER = """
			<thead><tr>
			<th class='col-name'>Name</th>
			<th class='col-logical-type'>Logical Data Type</th>
			<th class='col-physical-type'>Physical Data Type</th>
			<th class='col-logical-names'>Logical Names</th>
			<th class='col-description'>Description</th>
			</tr></thead>
			""";

	public DatabaseMetadataToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);

		// security
		User user = this.insight.getUser();
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			boolean isOwner = SecurityEngineUtils.userIsOwner(user, databaseId);
			if (!isOwner) {
				throw new IllegalArgumentException("Database " + databaseId
						+ " does not exist or user does not have permissions to database. User must be the owner to perform this function.");
			}
		}

		Map<String, Object> engineInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), databaseId, null)
				.get(0);
		engineInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(databaseId, null, true));
		engineInfo.putIfAbsent("description", "");
		engineInfo.putIfAbsent("tags", new ArrayList<String>());

		logger.info("Pulling database metadata for database " + databaseId);
		Map<String, Object> metamodelObject = new HashMap<>();
		{
			Map<String, Object> cacheMetamodel = EngineSyncUtility.getMetamodel(databaseId);
			if (cacheMetamodel != null) {
				metamodelObject.putAll(cacheMetamodel);
			} else {
				Map<String, Object> metamodel = MasterDatabaseUtility.getMetamodelRDBMS(databaseId, true);
				metamodelObject.putAll(metamodel);
				EngineSyncUtility.setMetamodel(databaseId, metamodel);
			}
		}

		logger.info("Pulling database logical names for database " + databaseId);
		Map<String, List<String>> logicalNames = EngineSyncUtility.getMetamodelLogicalNamesCache(databaseId);
		if (logicalNames == null) {
			logicalNames = MasterDatabaseUtility.getDatabaseLogicalNames(databaseId);
			EngineSyncUtility.setMetamodelLogicalNames(databaseId, logicalNames);
		}
		if (logicalNames == null) {
			logicalNames = Collections.emptyMap();
		}
		logger.info("Pulling database descriptions for database " + databaseId);
		Map<String, String> descriptions = EngineSyncUtility.getMetamodelDescriptionsCache(databaseId);
		if (descriptions == null) {
			descriptions = MasterDatabaseUtility.getDatabaseDescriptions(databaseId);
			EngineSyncUtility.setMetamodelDescriptions(databaseId, descriptions);
		}
		if (descriptions == null) {
			descriptions = Collections.emptyMap();
		}

		// now we will create the html of what we want to export
		String databaseName = getPreferredEngineValue(engineInfo, "engine_name", "database_name");
		String databaseSubType = getPreferredEngineValue(engineInfo, "engine_subtype", "database_subtype");
		String descriptionText = getPreferredEngineValue(engineInfo, "description", "description");
		Collection<?> tags = getCollectionValue(engineInfo, "tags");

		StringBuilder htmlBuilder = new StringBuilder();
		htmlBuilder.append("""
				<html>
				<head>
				<style>
				""");
		htmlBuilder.append(REPORT_STYLES);
		htmlBuilder.append("""
				</style>
				</head>
				<body>
				""");

		// Header
		htmlBuilder.append("""
				<div class='header'>
				<h1>Database Metadata Report</h1>
				<h2>%s</h2>
				</div>
				""".formatted(escapeHtml(databaseName)));

		// Summary Section
		htmlBuilder.append("""
				<div class='summary'>
				<h3>Summary</h3>
				<p><b>Database ID:</b> %s</p>
				<p><b>Database Name:</b> %s</p>
				<p><b>Database Type:</b> %s</p>
				""".formatted(escapeHtml(databaseId), escapeHtml(databaseName), escapeHtml(databaseSubType)));
		if (!descriptionText.isEmpty()) {
			htmlBuilder.append("<p><b>Description:</b> ").append(escapeHtml(descriptionText)).append("</p>");
		}
		if (tags != null && !tags.isEmpty()) {
			htmlBuilder.append("<div><b>Tags:</b> ");
			for (Object tag : tags) {
				htmlBuilder.append("<span class='tags'>").append(escapeHtml(tag)).append("</span>");
			}
			htmlBuilder.append("</div>");
		}
		htmlBuilder.append("</div>");

		// Data Definitions
		htmlBuilder.append("<div class='content'>");
		htmlBuilder.append("<h3>Data Definitions</h3>");
		Object[] nodes = (Object[]) metamodelObject.get("nodes");
		if (nodes == null) {
			nodes = new Object[0];
		}
		Map<String, String> logicalDataTypes = (Map<String, String>) metamodelObject.getOrDefault("dataTypes",
				Collections.emptyMap());
		Map<String, String> physicalDataTypes = (Map<String, String>) metamodelObject.getOrDefault("physicalTypes",
				Collections.emptyMap());

		List<MetamodelVertex> sortedNodes = new ArrayList<>();
		for (Object nodeObject : nodes) {
			if (nodeObject instanceof MetamodelVertex) {
				sortedNodes.add((MetamodelVertex) nodeObject);
			}
		}
		sortedNodes.sort(
				Comparator.comparing(node -> getValueOrEmpty(node.getConceptualName()), String.CASE_INSENSITIVE_ORDER));

		for (MetamodelVertex nodeMap : sortedNodes) {
			String conceptName = nodeMap.getConceptualName();
			Set<String> propNames = nodeMap.getPropSet();
			if (propNames == null) {
				propNames = Collections.emptySet();
			}
			List<String> sortedPropNames = new ArrayList<>(propNames);
			sortedPropNames.sort(String.CASE_INSENSITIVE_ORDER);
			htmlBuilder.append("<h4>Table: ").append(escapeHtml(conceptName)).append("</h4>");

			htmlBuilder.append("<table>");
			htmlBuilder.append(TABLE_HEADER);
			htmlBuilder.append("<tbody>");
			for (String prop : sortedPropNames) {
				String uid = conceptName + "__" + prop;

				String logicalDataType = logicalDataTypes.get(uid);
				String physicalDataType = physicalDataTypes.get(uid);
				String logicalNamesConcat = Strings.join(logicalNames.get(uid), ',');
				if (logicalNamesConcat == null) {
					logicalNamesConcat = "";
				}
				String description = descriptions.get(uid);
				if (description == null) {
					description = "";
				}

				htmlBuilder.append("""
						<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>
						""".formatted(escapeHtml(prop), escapeHtml(logicalDataType), escapeHtml(physicalDataType),
						escapeHtml(logicalNamesConcat), escapeHtml(description)));
			}
			htmlBuilder.append("</tbody></table>");
		}
		htmlBuilder.append("</div>");

		// Footer
		htmlBuilder.append("""
				<div class='footer'>
				<p>Generated on: %s</p>
				</div>
				</body>
				</html>
				""".formatted(escapeHtml(ZonedDateTime.now().format(REPORT_TIME_FORMATTER))));

		// keep track for deleting at the end
		List<String> tempPaths = new ArrayList<>();

		String insightFolder = this.insight.getInsightFolder();
		String random = Utility.getRandomString(5);
		String tempXhtmlPath = insightFolder + DIR_SEPARATOR + random + ".html";
		String outputFileLocation = insightFolder + DIR_SEPARATOR + "Database_Metadata.pdf";
		File tempXhtml = new File(tempXhtmlPath);

		try {
			try {
				FileUtils.writeStringToFile(tempXhtml, htmlBuilder.toString(), StandardCharsets.UTF_8);
				tempPaths.add(tempXhtmlPath);
			} catch (IOException ex) {
				classLogger.error("Error writing html file", ex);
				throw new IllegalArgumentException("Error saving the database metamodel as an html file", ex);
			}

			// Convert from xhtml to pdf
			try (FileOutputStream fos = new FileOutputStream(outputFileLocation)) {
				DocumentBuilderFactory factory = Utility.getDocumentBuilderFactory();
				DocumentBuilder builder = factory.newDocumentBuilder();
				Document document = builder.parse(tempXhtml);

				logger.info("Converting html to PDF...");
				ITextRenderer renderer = new ITextRenderer();
				renderer.setDocument(document);
				renderer.layout();
				renderer.createPDF(fos);
				logger.info("Done converting html to PDF...");
			} catch (Exception ex) {
				classLogger.error("Unable to convert from html to pdf due to error {}", ex.getMessage(), ex);
				throw new IllegalArgumentException("Error converting the database metamodel html file to pdf", ex);
			}
		} finally {
			// delete temp files
			for (String path : tempPaths) {
				try {
					File f = new File(Utility.normalizePath(path));
					if (f.exists()) {
						FileUtils.forceDelete(f);
					}
				} catch (IOException e) {
					classLogger.error("Unable to delete temp file {}", path, e);
				}
			}
		}

		// store it in the insight so the FE can download it
		// only from the given insight
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(outputFileLocation);
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	private static String getPreferredEngineValue(Map<String, Object> engineInfo, String preferredKey,
			String fallbackKey) {
		Object value = engineInfo.get(preferredKey);
		if (value == null && fallbackKey != null) {
			value = engineInfo.get(fallbackKey);
		}
		return getValueOrEmpty(value);
	}

	private static Collection<?> getCollectionValue(Map<String, Object> map, String key) {
		Object value = map.get(key);
		if (value instanceof Collection) {
			return (Collection<?>) value;
		}
		return Collections.emptyList();
	}

	private static String getValueOrEmpty(Object value) {
		return value == null ? "" : value.toString();
	}

	private static String escapeHtml(Object value) {
		return StringEscapeUtils.escapeHtml4(getValueOrEmpty(value));
	}

}
