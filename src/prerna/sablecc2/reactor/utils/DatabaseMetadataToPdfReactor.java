package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.xhtmlrenderer.pdf.ITextRenderer;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.nameserver.utility.MetamodelVertex;
import prerna.om.InsightFile;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class DatabaseMetadataToPdfReactor extends AbstractReactor {

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
				throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have permissions to database. User must be the owner to perform this function.");
			}
		}
		
		Map<String, Object> databaseInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), databaseId, null).get(0);
		databaseInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(databaseId, null, true));
		databaseInfo.putIfAbsent("description", "");
		databaseInfo.putIfAbsent("tags", new Vector<String>());
		
		logger.info("Pulling database metadata for database " + databaseId);
		Map<String, Object> metamodelObject = new HashMap<>();
		{
			Map<String, Object> cacheMetamodel = EngineSyncUtility.getMetamodel(databaseId);
			if(cacheMetamodel != null) {
				metamodelObject.putAll(cacheMetamodel);
			} else {
				Map<String, Object> metamodel = MasterDatabaseUtility.getMetamodelRDBMS(databaseId, true);
				metamodelObject.putAll(metamodel);
				EngineSyncUtility.setMetamodel(databaseId, metamodel);
			}
		}

		logger.info("Pulling database logical names for database " + databaseId);
		Map<String, List<String>> logicalNames = EngineSyncUtility.getMetamodelLogicalNamesCache(databaseId);
		if(logicalNames == null) {
			logicalNames = MasterDatabaseUtility.getDatabaseLogicalNames(databaseId);
			EngineSyncUtility.setMetamodelLogicalNames(databaseId, logicalNames);
		}
		logger.info("Pulling database descriptions for database " + databaseId);
		Map<String, String> descriptions = EngineSyncUtility.getMetamodelDescriptionsCache(databaseId);
		if(descriptions == null) {
			descriptions = MasterDatabaseUtility.getDatabaseDescriptions(databaseId);
			EngineSyncUtility.setMetamodelDescriptions(databaseId, descriptions);
		}
	
		// now we will create the html of what we want to export
		StringBuilder htmlBuilder = new StringBuilder("<html>");
		htmlBuilder.append("<head>");
		htmlBuilder.append("<style>table, th, td { padding: 10px; border: 1px solid black; border-collapse: collapse; } </style>");
		htmlBuilder.append("</head>");
		htmlBuilder.append("<body>");
		htmlBuilder.append("<h3>Database Id: " + databaseId + "</h3>");
		htmlBuilder.append("<h3>Database Name: " + databaseInfo.get("database_name") + "</h3>");
		htmlBuilder.append("<h3>Database Type: " + databaseInfo.get("database_type") + "</h3>");
		if(databaseInfo.containsKey("description") && !((String) databaseInfo.get("description")).isEmpty()) {
			htmlBuilder.append("<h3>Description: " + databaseInfo.get("description") + "</h3>");
		}
		if(databaseInfo.containsKey("tags") && !((Collection<String>) databaseInfo.get("tags")).isEmpty()) {
			htmlBuilder.append("<table><tr><th>Tags</th></tr>");
			Collection<String> tags = (Collection<String>) databaseInfo.get("tags");
			for(String tag : tags) {
				htmlBuilder.append("<tr><td>" + tag + "</td></tr>");
			}
			htmlBuilder.append("</table>");
		}
		htmlBuilder.append("<h3>Data Definitions:</h3>");
		Object[] nodes = (Object[]) metamodelObject.get("nodes");
		for(Object nodeObject : nodes) {
			MetamodelVertex nodeMap = (MetamodelVertex) nodeObject;
			String conceptName = nodeMap.getConceptualName();
			Set<String> propNames = nodeMap.getPropSet();
			htmlBuilder.append("<h4>" + conceptName + "</h4>");

			htmlBuilder.append("<table><tr><th>Name</th><th>Logical Data Type</th><th>Physical Data Type</th><th>Logical Names</th><th>Description</th></tr>");
			for(String prop : propNames) {
				String uid = conceptName + "__" + prop;
				
				String logicalDataType = ((Map<String, String>) metamodelObject.get("dataTypes")).get(uid);
				String physicalDataType = ((Map<String, String>) metamodelObject.get("physicalTypes")).get(uid);
				String logicalNamesConcat = Strings.join(logicalNames.get(uid), ',');
				if(logicalNamesConcat == null) {
					logicalNamesConcat = "";
				}
				String description = descriptions.get(uid);
				if(description == null) {
					description = "";
				}
				
				htmlBuilder.append("<tr><td>" + prop + "</td><td>" + logicalDataType + "</td><td>" + physicalDataType + "</td><td>" + logicalNamesConcat + "</td><td>" + description + "</td></tr>");
			}
			htmlBuilder.append("</table>");
		}
		htmlBuilder.append("<p>Generated on: " + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")) + "</p>");
		htmlBuilder.append("</body></html>");
		
		// keep track for deleting at the end
		List<String> tempPaths = new ArrayList<>();
				
		String insightFolder = this.insight.getInsightFolder();
		String random = Utility.getRandomString(5);
		String tempXhtmlPath = insightFolder + DIR_SEPARATOR + random + ".html";
		String outputFileLocation = insightFolder + DIR_SEPARATOR + "Database_Metadata.pdf";
		File tempXhtml = new File(tempXhtmlPath);
		try {
			FileUtils.writeStringToFile(tempXhtml, htmlBuilder.toString());
			tempPaths.add(tempXhtmlPath);
		} catch (IOException e1) {
			logger.error(Constants.STACKTRACE, e1);
		}
		
		// Convert from xhtml to pdf
		FileOutputStream fos = null;
		try {
			logger.info("Converting html to PDF...");
			fos = new FileOutputStream(outputFileLocation);
			ITextRenderer renderer = new ITextRenderer();
	        renderer.setDocument(tempXhtml.getAbsoluteFile());
	        renderer.layout();
	        renderer.createPDF(fos);
			logger.info("Done converting html to PDF...");
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		} finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// delete temp files
		for (String path : tempPaths) {
			try {
				File f = new File(Utility.normalizePath(path));
				if (f.exists()) {
					FileUtils.forceDelete(f);
				}
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
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
