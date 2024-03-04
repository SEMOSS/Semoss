package prerna.rpa.reporting.kickout.specific.anthem;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.FileHeader;
import prerna.rpa.reporting.ReportProcessingException;
import prerna.rpa.reporting.kickout.AbstractKickoutReportProcess;
import prerna.rpa.reporting.kickout.KickoutJedisKeys;

public class WGSPReportProcess extends AbstractKickoutReportProcess {
	
	private static final String QT = "\"";
	private static final String DLMTR = QT + "," + QT;
	
	private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd-HH.mm.ss";
	
	private static final int NCOL = 24;
	
	private final Set<String> ignoreSystems;
	
	public WGSPReportProcess(String reportPath, String prefix, Set<String> ignoreSystems) throws ParseException {
		super(reportPath, prefix, true);
		this.ignoreSystems = ignoreSystems;
	}

	@Override
	protected String determineReportTimestamp(String reportName) throws ParseException {
		return KickoutJedisKeys.timestampFormatter().format(parseKickoutDate(reportName));
	}
	
	// Public helper method for parsing the kickout date of a WGSP report
	public static Date parseKickoutDate(String reportName) throws ParseException {
		String timestampString = FilenameUtils.getBaseName(reportName).replaceFirst("DELTA_KO_RPT", "");
		SimpleDateFormat timestampFormatter = new SimpleDateFormat(TIMESTAMP_FORMAT);
		return timestampFormatter.parse(timestampString);
	}

	@Override
	protected BufferedReader readReport() throws ReportProcessingException {
		try {
			ZipFile zipFile = new ZipFile(reportPath);
			
			// For each file contained within the zip file,
			// check whether it should be added to reader
			@SuppressWarnings("unchecked")
			List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			Vector<InputStream> streams = new Vector<>();
			for (FileHeader fileHeader : fileHeaders) {
				
				// Figure out the source system and type of file
				String fileName = fileHeader.getFileName();
				if (!fileName.contains(".")) {
					continue;
				}
				int extensionIndex = fileName.lastIndexOf('.');
				String delta = fileName.substring(extensionIndex - 3, extensionIndex);
				String system = fileName.substring(extensionIndex - 9, extensionIndex - 7);
				
				// Filter out files that:
				//		a) Aren't excel spreadsheets
				//		b) Aren't delta loads
				//		c) Represent a source system that is set to be ignored
				if (!fileHeader.getFileName().endsWith(".XLS") || !delta.equals("DTL") || ignoreSystems.contains(system)) {
					continue;
				}
				
				// Finally, if the file qualifies, add its stream to the vector
				InputStream stream = zipFile.getInputStream(fileHeader);
				streams.add(stream);
			}
			
			// Combine all applicable input streams into one and return
			return new BufferedReader(new InputStreamReader(new SequenceInputStream(streams.elements())));
		} catch (Exception e) {
			throw new ReportProcessingException("Failed to unzip " + reportName + ".", e);
		}
	}

	@Override
	protected String formatLine(String line) {
		
		// Sometimes when there is missing data at the end of the record,
		// the length of the split line is less than the number of columns
		String[] splitLine = line.split("\t");
		int splitLength = splitLine.length;

		// If the line has no records, then skip over it
		// (The end of the file is a SUB character)
		if (splitLength == 1) {
			return null;
		}
		
		// Trim each element
		String[] rawRecord = new String[NCOL];
		for (int i = 0; i < NCOL; i++) {
			if (i < splitLength) {
				rawRecord[i] = splitLine[i].trim();
			} else {

				// Fill any blank elements with an empty string
				// Avoids a null pointer when calculating the error code
				rawRecord[i] = "";
			}
		}

		// Determine the error code
		String errorCode;
		if (!rawRecord[NCOL - 4].trim().isEmpty()) {

			// Critical
			errorCode = rawRecord[NCOL - 4];
		} else if (!rawRecord[NCOL - 3].trim().isEmpty()) {

			// Review
			errorCode = rawRecord[NCOL - 3];
		} else {

			// Informational
			errorCode = rawRecord[NCOL - 2];
		}

		// Because Sanja wanted it
		if (rawRecord[0].equals("VIRGINIA")) {
			rawRecord[0] = "CPMF";
		}

		// Comma separated string with each element in quotes
		String rawRecordString = String.join(DLMTR, rawRecord);
		
		// Add in error code
		StringBuilder fullRecordString = new StringBuilder();
		fullRecordString.append(QT);
		fullRecordString.append(rawRecordString);
		fullRecordString.append(DLMTR);
		fullRecordString.append(errorCode);
		fullRecordString.append(QT);

		// Return the formatted record
		return fullRecordString.toString();
	}

}
