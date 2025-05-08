package prerna.reactor.export.snowflake;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class SnowflakeCopyIntoReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SnowflakePutReactor.class);

	public SnowflakeCopyIntoReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey(), 
				"userStage", "tableStage", "namedStage",
				"files", "pattern", "formatName", "fileType", "fileCompression", "parseHeader", 
				"dateFormat",  "timeFormat", "timestampFormat", "fieldOptionallyEnclosedBy",
				"matchByColumnName", "force"
				};
		this.keyRequired = new int[] {
				1, 1, 
				0, 0, 0,
				0, 0, 0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0
				};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to database");
		}

		String tableName = this.keyValue.get(this.keysToGet[1]);
		if(tableName == null || (tableName=tableName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the table we are inserting into");
		}
		String userStage = this.keyValue.get(this.keysToGet[2]);
		String tableStage = this.keyValue.get(this.keysToGet[3]);
		String namedStage = this.keyValue.get(this.keysToGet[4]);
		
		String stageQualifier = "";
		String stageDestination = "";
		if(userStage != null && !(userStage=userStage.trim()).isEmpty()) {
			stageQualifier = "@~";
			stageDestination = userStage;
		} else if(tableStage != null && !(tableStage=tableStage.trim()).isEmpty()) {
			stageQualifier = "@%";
			stageDestination = tableStage;
		} else if(namedStage != null && !(namedStage=namedStage.trim()).isEmpty()) {
			stageQualifier = "@";
			stageDestination = namedStage;
		} else {
			throw new IllegalArgumentException("Must pass in userStage, tableStage, or namedStage. All values were null or empty");
		}
		
		String fromFiles = getFiles();
		String fromPattern = this.keyValue.get("pattern");

		String sql = "COPY INTO " + tableName + " FROM " +stageQualifier+stageDestination;
		if(fromFiles != null && !fromFiles.isEmpty()) {
			sql += " FILES = " + fromFiles;
		} else if(fromPattern != null && !(fromPattern=fromPattern.trim()).isEmpty()) {
			sql += " PATTERN = '" + fromPattern + "'";
		} else {
			throw new IllegalArgumentException("Must pass in files or pattern for the FROM statement in the COPY FILES command");
		}

		{
			String fileFormat = "FILE_FORMAT = (";
			String formatName = this.keyValue.get("formatName");
			if(formatName != null && !(formatName=formatName.trim()).isEmpty()) {
				fileFormat += " FORMAT_NAME = '" + formatName + "'";
			}
			String fileType = this.keyValue.get("fileType");
			if(fileType != null && !(fileType=fileType.trim()).isEmpty()) {
				fileFormat += " TYPE = " + fileType.toUpperCase();
			}
			String fileCompression = this.keyValue.get("fileCompression");
			if(fileCompression != null && !(fileCompression=fileCompression.trim()).isEmpty()) {
				fileFormat += " COMPRESSION = " + fileCompression.toUpperCase();
			}
			Boolean parseHeader = Boolean.parseBoolean(this.keyValue.getOrDefault("parseHeader", "true")+"");
			if(parseHeader != null) {
				fileFormat += " PARSE_HEADER = " + parseHeader.toString().toUpperCase();
			}
			String dateFormat = this.keyValue.get("dateFormat");
			if(dateFormat != null && !(dateFormat=dateFormat.trim()).isEmpty()) {
				fileFormat += " DATE_FORMAT = '" + dateFormat + "'";
			}
			String timeFormat = this.keyValue.get("timeFormat");
			if(timeFormat != null && !(timeFormat=timeFormat.trim()).isEmpty()) {
				fileFormat += " TIME_FORMAT = '" + timeFormat + "'";
			}
			String timestampFormat = this.keyValue.get("timestampFormat");
			if(timestampFormat != null && !(timestampFormat=timestampFormat.trim()).isEmpty()) {
				fileFormat += " TIMESTAMP_FORMAT = '" + timestampFormat + "'";
			}
			String fieldOptionallyEnclosedBy = this.keyValue.getOrDefault("fieldOptionallyEnclosedBy", "NONE");
			if(fieldOptionallyEnclosedBy != null && !(fieldOptionallyEnclosedBy=fieldOptionallyEnclosedBy.trim()).isEmpty()) {
				if(fieldOptionallyEnclosedBy.equals("'")) {
					fileFormat += " FIELD_OPTIONALLY_ENCLOSED_BY = ''''";
				} else {
					fileFormat += " FIELD_OPTIONALLY_ENCLOSED_BY = '" + fieldOptionallyEnclosedBy + "'";
				}
			}
			sql += " " + fileFormat + ")";
		}
		String matchByColumnName = this.keyValue.getOrDefault("matchByColumnName", "CASE_SENSITIVE");
		if(matchByColumnName != null && !(matchByColumnName=matchByColumnName.trim()).isEmpty()) {
			sql += " MATCH_BY_COLUMN_NAME = " + matchByColumnName.toUpperCase();
		}
		Boolean force = Boolean.parseBoolean(this.keyValue.get("force")+"");
		if(force != null) {
			sql += " FORCE = " + force.toString().toUpperCase();
		}
		
		IDatabaseEngine snowflake = Utility.getDatabase(databaseId);
		if(!(snowflake instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("Database is not a snowlfake db");
		}
		IRDBMSEngine snowflakeRdbms = (IRDBMSEngine) snowflake;
		if(snowflakeRdbms.getDbType() != RdbmsTypeEnum.SNOWFLAKE) {
			throw new IllegalArgumentException("Database is not a snowlfake db");
		}
		
		Statement stmt = null;
		try {
			stmt = snowflakeRdbms.getConnection().createStatement();
			stmt.execute(sql);
			int updatedRows = stmt.getUpdateCount();
			return new NounMetadata("Updated rows = " + updatedRows, PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("A SQL exception was thrown. Detailed error = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(snowflakeRdbms, stmt);
		}
	}

	/**
	 * 
	 * @return
	 */
	private String getFiles() {
		List<String> files = new ArrayList<String>();

		GenRowStruct colGrs = this.store.getNoun("files");
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				// wrap in quotes as well
				files.add("'"+column+"'");
			}
		}
		
		if(files.isEmpty()) {
			return null;
		}
		
		StringBuilder builder = new StringBuilder("(");
		builder.append(String.join(",", files));
		builder.append(")");
		return builder.toString();
	}
	
	@Override
	public String getReactorDescription() {
		return "Utility method to execute the COPY INTO command from a file in a stage. Snowflake docs found here: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id for the snowflake db";
		} else if(key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "This is a required value containing the table we are inserting into";
		} else if(key.equals("userStage")) {
			return "The path prefix for the user stage. Do not include the '@~' qualifier";
		} else if(key.equals("tableStage")) {
			return "The table name and path prefix. Do not enter the '@%' qualifier"; 
		} else if(key.equals("namedStage")) {
			return "The named stage and path prefix. This will not create a stage if it does not exist. Do not enter the '@' qualifier "; 
		} else if(key.equals("files")) {
			return "Specifies a list of one or more comma-separated file names to copy";
		} else if(key.equals("pattern")) {
			return "Specifies a regular expression pattern for filtering the list of files to copy. This command applies the regular expression to the entire stage location";
		} else if(key.equals("formatName")) {
			return "Specifies an existing named file format to use for loading data into the table. Do not use with fileType. Please reference https://docs.snowflake.com/en/sql-reference/sql/create-file-format on creating your own file format";
		} else if(key.equals("fileType")) {
			return "Specifies the type of files to load into the table. Do not use with formatName. Supported values include: CSV | JSON | AVRO | ORC | PARQUET | XML. Additional options depend on the type of file selected.";
		} else if(key.equals("fileCompression")) {
			return "Specifies the current compression algorithm for the data files to be loaded. Supported values include: AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE. Default is AUTO";
		} else if(key.equals("parseHeader")) {
			return "Boolean that specifies whether to use the first row headers in the data files to determine column names. Default value is true.";
		} else if(key.equals("dateFormat")) {
			return "String that defines the format of date values in the data files to be loaded";
		} else if(key.equals("timeFormat")) {
			return "String that defines the format of time values in the data files to be loaded";
		} else if(key.equals("timestampFormat")) {
			return "String that defines the format of timestamp values in the data files to be loaded";
		} else if(key.equals("fieldOptionallyEnclosedBy")) {
			return "Character used to enclose strings. Supported values include: NONE, single quote character ('), or double quote character (\"). Default is NONE.";
		} else if(key.equals("matchByColumnName")) {
			return "String that specifies whether to load semi-structured data into columns in the target table that match corresponding columns represented in the data. Supported values are: CASE_SENSITIVE | CASE_INSENSITIVE | NONE. Default is CASE_SENSITIVE";
		} else if(key.equals("force")) {
			return "Boolean that specifies to load all files, regardless of whether they’ve been loaded previously and have not changed since they were loaded. Default is false";
		}
		
		return super.getDescriptionForKey(key);
	}

}

