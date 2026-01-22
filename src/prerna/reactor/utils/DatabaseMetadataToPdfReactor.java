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
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
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
		engineInfo.putIfAbsent("tags", new Vector<String>());

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
		logger.info("Pulling database descriptions for database " + databaseId);
		Map<String, String> descriptions = EngineSyncUtility.getMetamodelDescriptionsCache(databaseId);
		if (descriptions == null) {
			descriptions = MasterDatabaseUtility.getDatabaseDescriptions(databaseId);
			EngineSyncUtility.setMetamodelDescriptions(databaseId, descriptions);
		}

		// now we will create the html of what we want to export
		StringBuilder htmlBuilder = new StringBuilder("<html><head>");
		htmlBuilder.append("<style>" + "body { font-family: Arial, Helvetica, sans-serif; margin: 25px; }"
				+ ".header { text-align: center; padding-bottom: 20px; border-bottom: 1px solid #ddd; margin-bottom: 20px; }"
				+ ".header h1 { margin: 0; font-size: 28px; }"
				+ ".header h2 { margin: 5px 0; font-size: 16px; color: #555; }"
				+ ".summary { background-color: #f9f9f9; border: 1px solid #ddd; padding: 15px; margin-bottom: 30px; border-radius: 5px; }"
				+ ".summary h3 { margin-top: 0; border-bottom: 1px solid #ccc; padding-bottom: 5px; }"
				+ ".summary p { margin: 5px 0; }"
				+ ".summary .tags { display: inline-block; background-color: #e1e1e1; color: #333; padding: 5px 10px; margin: 5px 5px 5px 0; border-radius: 15px; font-size: 12px; }"
				+ ".content h3 { font-size: 22px; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; margin-top: 30px; }"
				+ ".content h4 { font-size: 18px; margin-top: 25px; }"
				+ "table { width: 100%; border-collapse: collapse; margin-top: 15px; table-layout: fixed; }"
				+ "th, td { padding: 12px; text-align: left; border: 1px solid #ddd; word-wrap: break-word; }"
				+ "thead { background-color: #4CAF50; color: white; }"
				+ "tbody tr:nth-child(even) { background-color: #f2f2f2; }" + "th.col-name { width: 15%; }"
				+ "th.col-logical-type { width: 15%; }" + "th.col-physical-type { width: 15%; }"
				+ "th.col-logical-names { width: 20%; }" + "th.col-description { width: 35%; }"
				+ ".footer { text-align: center; margin-top: 30px; font-size: 12px; color: #777; }" + "</style>");
		htmlBuilder.append("</head><body>");

		// Header
		htmlBuilder.append("<div class='header'>");
		htmlBuilder.append("<h1>Database Metadata Report</h1>");
		htmlBuilder.append("<h2>" + engineInfo.get("database_name") + "</h2>");
		htmlBuilder.append("</div>");

		// Summary Section
		htmlBuilder.append("<div class='summary'>");
		htmlBuilder.append("<h3>Summary</h3>");
		htmlBuilder.append("<p><b>Database ID:</b> " + databaseId + "</p>");
		htmlBuilder.append("<p><b>Database Name:</b> " + engineInfo.get("database_name") + "</p>");
		htmlBuilder.append("<p><b>Database Type:</b> " + engineInfo.get("database_subtype") + "</p>");
		if (engineInfo.containsKey("description") && !((String) engineInfo.get("description")).isEmpty()) {
			htmlBuilder.append("<p><b>Description:</b> " + engineInfo.get("description") + "</p>");
		}
		if (engineInfo.containsKey("tags") && !((Collection<String>) engineInfo.get("tags")).isEmpty()) {
			htmlBuilder.append("<div><b>Tags:</b> ");
			Collection<String> tags = (Collection<String>) engineInfo.get("tags");
			for (String tag : tags) {
				htmlBuilder.append("<span class='tags'>" + tag + "</span>");
			}
			htmlBuilder.append("</div>");
		}
		htmlBuilder.append("</div>");

		// Data Definitions
		htmlBuilder.append("<div class='content'>");
		htmlBuilder.append("<h3>Data Definitions</h3>");
		Object[] nodes = (Object[]) metamodelObject.get("nodes");
		for (Object nodeObject : nodes) {
			MetamodelVertex nodeMap = (MetamodelVertex) nodeObject;
			String conceptName = nodeMap.getConceptualName();
			Set<String> propNames = nodeMap.getPropSet();
			htmlBuilder.append("<h4>Table: " + conceptName + "</h4>");

			htmlBuilder.append("<table>");
			htmlBuilder.append("<thead><tr>" + "<th class='col-name'>Name</th>"
					+ "<th class='col-logical-type'>Logical Data Type</th>"
					+ "<th class='col-physical-type'>Physical Data Type</th>"
					+ "<th class='col-logical-names'>Logical Names</th>"
					+ "<th class='col-description'>Description</th>" + "</tr></thead>");
			htmlBuilder.append("<tbody>");
			for (String prop : propNames) {
				String uid = conceptName + "__" + prop;

				String logicalDataType = ((Map<String, String>) metamodelObject.get("dataTypes")).get(uid);
				String physicalDataType = ((Map<String, String>) metamodelObject.get("physicalTypes")).get(uid);
				String logicalNamesConcat = Strings.join(logicalNames.get(uid), ',');
				if (logicalNamesConcat == null) {
					logicalNamesConcat = "";
				}
				String description = descriptions.get(uid);
				if (description == null) {
					description = "";
				}

				htmlBuilder.append("<tr><td>" + prop + "</td><td>" + logicalDataType + "</td><td>" + physicalDataType
						+ "</td><td>" + logicalNamesConcat + "</td><td>" + description + "</td></tr>");
			}
			htmlBuilder.append("</tbody></table>");
		}
		htmlBuilder.append("</div>");

		// Footer
		htmlBuilder.append("<div class='footer'>");
		htmlBuilder.append("<p>Generated on: "
				+ ZonedDateTime.now().format(DateTimeFormatter.ofPattern("MMMM dd, yyyy 'at' hh:mm a z")) + "</p>");
		htmlBuilder.append("</div>");

		htmlBuilder.append("</body></html>");

		// keep track for deleting at the end
		List<String> tempPaths = new ArrayList<>();

		String insightFolder = this.insight.getInsightFolder();
		String random = Utility.getRandomString(5);
		String tempXhtmlPath = insightFolder + DIR_SEPARATOR + random + ".html";
		String outputFileLocation = insightFolder + DIR_SEPARATOR + "Database_Metadata.pdf";
		File tempXhtml = new File(tempXhtmlPath);

		try {
			try {
				FileUtils.writeStringToFile(tempXhtml, htmlBuilder.toString(), Charset.forName("UTF-8"));
				tempPaths.add(tempXhtmlPath);
			} catch (IOException ex) {
				classLogger.error("Error writing html file", ex.getMessage(), ex);
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

}
