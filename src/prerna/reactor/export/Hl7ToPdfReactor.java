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
package prerna.reactor.export;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xhtmlrenderer.pdf.ITextRenderer;

import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Hl7ToPdfReactor — parses an HL7 v2.x message and renders a structured PDF.
 *
 * Pixel syntax:
 *   Hl7ToPdf(content=["MSH|^~\&|EPIC|ADT|..."])
 *
 * Returns a FILE_DOWNLOAD NounMetadata. The frontend receives the download key
 * and fetches the file via GET /api/engine/downloadFile?insightId=...&fileKey=...
 */
public class Hl7ToPdfReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(Hl7ToPdfReactor.class);
	private static final String CLASS_NAME = Hl7ToPdfReactor.class.getName();

	private static final DateTimeFormatter REPORT_TIME_FORMATTER =
			DateTimeFormatter.ofPattern("MMMM dd, yyyy 'at' hh:mm a z");

	private static final String STYLES = """
			body { font-family: Arial, Helvetica, sans-serif; margin: 20px; color: #222; }
			.page-header { background: #1a5276; color: white; padding: 16px 20px; margin-bottom: 20px; border-radius: 4px; }
			.page-header h1 { margin: 0; font-size: 20px; font-weight: bold; }
			.page-header h2 { margin: 4px 0 0; font-size: 13px; font-weight: normal; opacity: 0.85; }
			.section { border: 1px solid #ccd; border-radius: 4px; margin-bottom: 18px; }
			.section-title { background: #2e86c1; color: white; padding: 7px 12px; font-size: 13px; font-weight: bold; border-radius: 3px 3px 0 0; }
			.section-body { padding: 10px 12px; }
			.field-grid { display: table; width: 100%; border-collapse: collapse; }
			.field-row { display: table-row; }
			.field-label { display: table-cell; width: 32%; font-weight: bold; font-size: 11px; color: #555; padding: 4px 6px 4px 0; vertical-align: top; }
			.field-value { display: table-cell; font-size: 11px; padding: 4px 0; vertical-align: top; }
			table.data-table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 10px; }
			table.data-table th { background: #2e86c1; color: white; padding: 5px 7px; text-align: left; font-weight: 600; }
			table.data-table td { padding: 4px 7px; border-bottom: 1px solid #e0e0e0; }
			table.data-table tr:nth-child(even) td { background: #f4f8fc; }
			.raw-block { font-family: monospace; font-size: 9px; background: #f8f8f8; border: 1px solid #dde; padding: 8px; border-radius: 3px; word-break: break-all; line-height: 1.5; }
			.footer { text-align: center; margin-top: 24px; font-size: 10px; color: #888; }
			.badge { display: inline-block; background: #e8f4fd; border: 1px solid #2e86c1; color: #1a5276;
			         padding: 2px 8px; border-radius: 10px; font-size: 10px; margin: 2px 2px 2px 0; }
			""";

	public Hl7ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONTENT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();

		String rawMessage = this.keyValue.get(this.keysToGet[0]);
		if (rawMessage == null || rawMessage.isBlank()) {
			throw new IllegalArgumentException("HL7 message content is required");
		}

		logger.info("Parsing HL7 message");
		Hl7Message msg = Hl7Message.parse(rawMessage);

		logger.info("Building HTML report");
		String html = buildHtml(msg, rawMessage);

		String insightFolder = this.insight.getInsightFolder();
		String random = Utility.getRandomString(6);
		String tempHtmlPath = insightFolder + DIR_SEPARATOR + random + "_hl7.html";
		String outputPdfPath = insightFolder + DIR_SEPARATOR + "HL7_Report.pdf";
		File tempHtml = new File(tempHtmlPath);

		try {
			try {
				FileUtils.writeStringToFile(tempHtml, html, StandardCharsets.UTF_8);
			} catch (IOException ex) {
				classLogger.error("Error writing temp HTML", ex);
				throw new IllegalArgumentException("Error writing HL7 HTML file", ex);
			}

			logger.info("Converting HTML to PDF");
			try (FileOutputStream fos = new FileOutputStream(outputPdfPath)) {
				DocumentBuilderFactory factory = Utility.getDocumentBuilderFactory();
				DocumentBuilder builder = factory.newDocumentBuilder();
				Document document = builder.parse(tempHtml);

				ITextRenderer renderer = new ITextRenderer();
				renderer.setDocument(document);
				renderer.layout();
				renderer.createPDF(fos);
			} catch (Exception ex) {
				classLogger.error("Error converting HTML to PDF", ex);
				throw new IllegalArgumentException("Error converting HL7 report to PDF", ex);
			}
		} finally {
			if (tempHtml.exists()) {
				try {
					FileUtils.forceDelete(tempHtml);
				} catch (IOException e) {
					classLogger.warn("Could not delete temp HTML file: {}", tempHtmlPath);
				}
			}
		}

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(outputPdfPath);
		insightFile.setDeleteOnInsightClose(true);
		this.insight.addExportFile(downloadKey, insightFile);

		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	// -------------------------------------------------------------------------
	// HTML builder
	// -------------------------------------------------------------------------

	private String buildHtml(Hl7Message msg, String rawMessage) {
		StringBuilder sb = new StringBuilder();
		sb.append("<!DOCTYPE html><html><head><meta charset='UTF-8'/><style>")
		  .append(STYLES)
		  .append("</style></head><body>");

		// Page header
		String msgType = msg.getField("MSH", 9, 0);
		String msgSubType = msg.getField("MSH", 9, 1);
		String displayType = msgSubType.isEmpty() ? msgType : msgType + "^" + msgSubType;
		sb.append("<div class='page-header'>")
		  .append("<h1>HL7 Clinical Message Report</h1>")
		  .append("<h2>Message Type: ").append(esc(displayType))
		  .append(" &nbsp;|&nbsp; Generated: ").append(esc(ZonedDateTime.now().format(REPORT_TIME_FORMATTER)))
		  .append("</h2></div>");

		appendMessageHeader(sb, msg);
		appendPatientInfo(sb, msg);
		appendPatientVisit(sb, msg);
		appendDiagnoses(sb, msg);
		appendObservations(sb, msg);
		appendAllergies(sb, msg);
		appendNextOfKin(sb, msg);
		appendInsurance(sb, msg);
		appendRawMessage(sb, rawMessage);

		sb.append("<div class='footer'><p>HL7 v2.x Message Report &mdash; Confidential &mdash; For authorized use only</p></div>");
		sb.append("</body></html>");
		return sb.toString();
	}

	private void appendMessageHeader(StringBuilder sb, Hl7Message msg) {
		section(sb, "Message Header (MSH)", () -> {
			grid(sb, () -> {
				row(sb, "Sending Application", msg.getField("MSH", 3, 0));
				row(sb, "Sending Facility", msg.getField("MSH", 4, 0));
				row(sb, "Receiving Application", msg.getField("MSH", 5, 0));
				row(sb, "Receiving Facility", msg.getField("MSH", 6, 0));
				row(sb, "Message Date/Time", formatHl7Datetime(msg.getField("MSH", 7, 0)));
				row(sb, "Message Type", msg.getField("MSH", 9, 0) + (msg.getField("MSH", 9, 1).isEmpty() ? "" : "^" + msg.getField("MSH", 9, 1)));
				row(sb, "Message Control ID", msg.getField("MSH", 10, 0));
				row(sb, "Processing ID", msg.getField("MSH", 11, 0));
				row(sb, "HL7 Version", msg.getField("MSH", 12, 0));
			});
		});
	}

	private void appendPatientInfo(StringBuilder sb, Hl7Message msg) {
		if (!msg.hasSegment("PID")) return;
		section(sb, "Patient Identification (PID)", () -> {
			grid(sb, () -> {
				row(sb, "Patient ID", msg.getField("PID", 3, 0));
				row(sb, "Patient Name", formatName(msg.getField("PID", 5)));
				row(sb, "Date of Birth", formatHl7Date(msg.getField("PID", 7, 0)));
				row(sb, "Sex", decodeSex(msg.getField("PID", 8, 0)));
				row(sb, "Race", msg.getField("PID", 10, 0));
				row(sb, "Address", formatAddress(msg.getSegmentField("PID", 11)));
				row(sb, "Home Phone", msg.getField("PID", 13, 0));
				row(sb, "Business Phone", msg.getField("PID", 14, 0));
				row(sb, "Language", msg.getField("PID", 15, 0));
				row(sb, "Marital Status", msg.getField("PID", 16, 0));
				row(sb, "SSN", msg.getField("PID", 19, 0));
				row(sb, "Ethnicity", msg.getField("PID", 22, 0));
			});
		});
	}

	private void appendPatientVisit(StringBuilder sb, Hl7Message msg) {
		if (!msg.hasSegment("PV1")) return;
		section(sb, "Patient Visit (PV1)", () -> {
			grid(sb, () -> {
				row(sb, "Patient Class", decodePatientClass(msg.getField("PV1", 2, 0)));
				row(sb, "Assigned Location", formatLocation(msg.getSegmentField("PV1", 3)));
				row(sb, "Admission Type", msg.getField("PV1", 4, 0));
				row(sb, "Attending Physician", formatName(msg.getField("PV1", 7)));
				row(sb, "Referring Physician", formatName(msg.getField("PV1", 8)));
				row(sb, "Consulting Physician", formatName(msg.getField("PV1", 9)));
				row(sb, "Hospital Service", msg.getField("PV1", 10, 0));
				row(sb, "Admit Datetime", formatHl7Datetime(msg.getField("PV1", 44, 0)));
				row(sb, "Discharge Datetime", formatHl7Datetime(msg.getField("PV1", 45, 0)));
				row(sb, "Account Status", msg.getField("PV1", 41, 0));
				row(sb, "Visit Number", msg.getField("PV1", 19, 0));
			});
		});
	}

	private void appendDiagnoses(StringBuilder sb, Hl7Message msg) {
		List<String[]> segs = msg.getAllSegments("DG1");
		if (segs.isEmpty()) return;
		section(sb, "Diagnoses (DG1)", () -> {
			sb.append("<table class='data-table'><thead><tr>")
			  .append("<th>#</th><th>Code</th><th>Description</th><th>Type</th><th>Date</th>")
			  .append("</tr></thead><tbody>");
			for (String[] fields : segs) {
				String setId = safeGet(fields, 1);
				String code = safeGet(fields, 3);
				String coding = safeGet(fields, 3, 1);   // subfield 1 = coding system, subfield 0 = code itself
				String desc = safeGet(fields, 4);        // DG1.4 is display name (but may be in 3.1 for ICD codes)
				// HL7 DG1.3 is CWE: code^display^coding_system
				String[] codeParts = code.split("\\^", -1);
				String codeVal = codeParts.length > 0 ? codeParts[0] : "";
				String codeDesc = codeParts.length > 1 ? codeParts[1] : desc;
				String codeSystem = codeParts.length > 2 ? codeParts[2] : "";
				String type = safeGet(fields, 6);
				String date = formatHl7Date(safeGet(fields, 5));
				sb.append("<tr><td>").append(esc(setId)).append("</td>")
				  .append("<td><span class='badge'>").append(esc(codeSystem)).append("</span> ").append(esc(codeVal)).append("</td>")
				  .append("<td>").append(esc(codeDesc)).append("</td>")
				  .append("<td>").append(esc(type)).append("</td>")
				  .append("<td>").append(esc(date)).append("</td></tr>");
			}
			sb.append("</tbody></table>");
		});
	}

	private void appendObservations(StringBuilder sb, Hl7Message msg) {
		List<String[]> obxSegs = msg.getAllSegments("OBX");
		if (obxSegs.isEmpty()) return;
		section(sb, "Observations / Results (OBX)", () -> {
			sb.append("<table class='data-table'><thead><tr>")
			  .append("<th>#</th><th>Identifier</th><th>Value</th><th>Units</th><th>Reference Range</th><th>Status</th>")
			  .append("</tr></thead><tbody>");
			for (String[] fields : obxSegs) {
				String setId = safeGet(fields, 1);
				String[] idParts = safeGet(fields, 3).split("\\^", -1);
				String identifier = idParts.length > 1 ? idParts[1] : (idParts.length > 0 ? idParts[0] : "");
				String value = safeGet(fields, 5);
				String units = safeGet(fields, 6).replace("^", " ");
				String refRange = safeGet(fields, 7);
				String status = safeGet(fields, 11);
				sb.append("<tr><td>").append(esc(setId)).append("</td>")
				  .append("<td>").append(esc(identifier)).append("</td>")
				  .append("<td>").append(esc(value)).append("</td>")
				  .append("<td>").append(esc(units)).append("</td>")
				  .append("<td>").append(esc(refRange)).append("</td>")
				  .append("<td>").append(esc(decodeObxStatus(status))).append("</td></tr>");
			}
			sb.append("</tbody></table>");
		});
	}

	private void appendAllergies(StringBuilder sb, Hl7Message msg) {
		List<String[]> segs = msg.getAllSegments("AL1");
		if (segs.isEmpty()) return;
		section(sb, "Allergies (AL1)", () -> {
			sb.append("<table class='data-table'><thead><tr>")
			  .append("<th>#</th><th>Type</th><th>Allergen</th><th>Severity</th><th>Reaction</th>")
			  .append("</tr></thead><tbody>");
			for (String[] fields : segs) {
				String setId = safeGet(fields, 1);
				String type = safeGet(fields, 2);
				String allergen = safeGet(fields, 3).replace("^", " ").trim();
				String severity = safeGet(fields, 4);
				String reaction = safeGet(fields, 5);
				sb.append("<tr><td>").append(esc(setId)).append("</td>")
				  .append("<td>").append(esc(type)).append("</td>")
				  .append("<td>").append(esc(allergen)).append("</td>")
				  .append("<td>").append(esc(severity)).append("</td>")
				  .append("<td>").append(esc(reaction)).append("</td></tr>");
			}
			sb.append("</tbody></table>");
		});
	}

	private void appendNextOfKin(StringBuilder sb, Hl7Message msg) {
		List<String[]> segs = msg.getAllSegments("NK1");
		if (segs.isEmpty()) return;
		section(sb, "Next of Kin / Associated Parties (NK1)", () -> {
			sb.append("<table class='data-table'><thead><tr>")
			  .append("<th>#</th><th>Name</th><th>Relationship</th><th>Address</th><th>Phone</th>")
			  .append("</tr></thead><tbody>");
			for (String[] fields : segs) {
				String setId = safeGet(fields, 1);
				String name = formatName(safeGet(fields, 2));
				String rel = safeGet(fields, 3).replace("^", " ").trim();
				String addr = formatAddress(safeGet(fields, 4));
				String phone = safeGet(fields, 5);
				sb.append("<tr><td>").append(esc(setId)).append("</td>")
				  .append("<td>").append(esc(name)).append("</td>")
				  .append("<td>").append(esc(rel)).append("</td>")
				  .append("<td>").append(esc(addr)).append("</td>")
				  .append("<td>").append(esc(phone)).append("</td></tr>");
			}
			sb.append("</tbody></table>");
		});
	}

	private void appendInsurance(StringBuilder sb, Hl7Message msg) {
		List<String[]> segs = msg.getAllSegments("IN1");
		if (segs.isEmpty()) return;
		section(sb, "Insurance (IN1)", () -> {
			for (String[] fields : segs) {
				grid(sb, () -> {
					row(sb, "Plan ID", safeGet(fields, 2).replace("^", " ").trim());
					row(sb, "Insurance Company", safeGet(fields, 4));
					row(sb, "Group Number", safeGet(fields, 8));
					row(sb, "Group Name", safeGet(fields, 9));
					row(sb, "Insured Name", formatName(safeGet(fields, 16)));
					row(sb, "Insured DOB", formatHl7Date(safeGet(fields, 18)));
					row(sb, "Plan Effective Date", formatHl7Date(safeGet(fields, 12)));
					row(sb, "Plan Expiration Date", formatHl7Date(safeGet(fields, 13)));
				});
			}
		});
	}

	private void appendRawMessage(StringBuilder sb, String raw) {
		section(sb, "Raw HL7 Message", () -> {
			sb.append("<div class='raw-block'>");
			for (String line : raw.split("\\r\\n|\\r|\\n")) {
				if (!line.isBlank()) {
					sb.append(esc(line)).append("<br/>");
				}
			}
			sb.append("</div>");
		});
	}

	// -------------------------------------------------------------------------
	// Layout helpers
	// -------------------------------------------------------------------------

	private void section(StringBuilder sb, String title, Runnable body) {
		sb.append("<div class='section'><div class='section-title'>").append(esc(title)).append("</div><div class='section-body'>");
		body.run();
		sb.append("</div></div>");
	}

	private void grid(StringBuilder sb, Runnable rows) {
		sb.append("<div class='field-grid'>");
		rows.run();
		sb.append("</div>");
	}

	private void row(StringBuilder sb, String label, String value) {
		if (value == null || value.isBlank()) return;
		sb.append("<div class='field-row'>")
		  .append("<div class='field-label'>").append(esc(label)).append(":</div>")
		  .append("<div class='field-value'>").append(esc(value)).append("</div>")
		  .append("</div>");
	}

	// -------------------------------------------------------------------------
	// HL7 field formatting helpers
	// -------------------------------------------------------------------------

	/** HL7 name field: family^given^middle^suffix^prefix → "Prefix Given Middle Family, Suffix" */
	private String formatName(String raw) {
		if (raw == null || raw.isBlank()) return "";
		String[] parts = raw.split("\\^", -1);
		String family = parts.length > 0 ? parts[0] : "";
		String given  = parts.length > 1 ? parts[1] : "";
		String middle = parts.length > 2 ? parts[2] : "";
		String suffix = parts.length > 3 ? parts[3] : "";
		String prefix = parts.length > 4 ? parts[4] : "";
		StringBuilder name = new StringBuilder();
		if (!prefix.isBlank()) name.append(prefix).append(" ");
		if (!given.isBlank())  name.append(given).append(" ");
		if (!middle.isBlank()) name.append(middle).append(" ");
		name.append(family);
		if (!suffix.isBlank()) name.append(", ").append(suffix);
		return name.toString().trim();
	}

	/** HL7 address: street^other^city^state^zip^country → "Street, City, State Zip, Country" */
	private String formatAddress(String raw) {
		if (raw == null || raw.isBlank()) return "";
		String[] parts = raw.split("\\^", -1);
		StringBuilder addr = new StringBuilder();
		String street = parts.length > 0 ? parts[0] : "";
		String other  = parts.length > 1 ? parts[1] : "";
		String city   = parts.length > 2 ? parts[2] : "";
		String state  = parts.length > 3 ? parts[3] : "";
		String zip    = parts.length > 4 ? parts[4] : "";
		String country= parts.length > 5 ? parts[5] : "";
		if (!street.isBlank()) addr.append(street);
		if (!other.isBlank())  addr.append(", ").append(other);
		if (!city.isBlank())   addr.append(", ").append(city);
		if (!state.isBlank())  addr.append(", ").append(state);
		if (!zip.isBlank())    addr.append(" ").append(zip);
		if (!country.isBlank()) addr.append(", ").append(country);
		return addr.toString().replaceAll("^,\\s*", "").trim();
	}

	/** HL7 location: point-of-care^room^bed^facility → "Facility / Point-of-Care Room Bed" */
	private String formatLocation(String raw) {
		if (raw == null || raw.isBlank()) return "";
		String[] parts = raw.split("\\^", -1);
		String poc      = parts.length > 0 ? parts[0] : "";
		String room     = parts.length > 1 ? parts[1] : "";
		String bed      = parts.length > 2 ? parts[2] : "";
		String facility = parts.length > 3 ? parts[3] : "";
		StringBuilder loc = new StringBuilder();
		if (!facility.isBlank()) loc.append(facility).append(" / ");
		if (!poc.isBlank())  loc.append(poc);
		if (!room.isBlank()) loc.append(" ").append(room);
		if (!bed.isBlank())  loc.append("-").append(bed);
		return loc.toString().trim();
	}

	/** Format HL7 datetime YYYYMMDDHHMMSS[.SSSS][+ZZZZ] to human-readable */
	private String formatHl7Datetime(String raw) {
		if (raw == null || raw.isBlank()) return "";
		raw = raw.replaceAll("[+\\-]\\d{4}$", "").trim();
		try {
			if (raw.length() >= 12) {
				return raw.substring(0, 4) + "-" + raw.substring(4, 6) + "-" + raw.substring(6, 8)
						+ " " + raw.substring(8, 10) + ":" + raw.substring(10, 12);
			} else if (raw.length() == 8) {
				return formatHl7Date(raw);
			}
		} catch (Exception e) {
			// fall through
		}
		return raw;
	}

	private String formatHl7Date(String raw) {
		if (raw == null || raw.isBlank()) return "";
		try {
			if (raw.length() == 8) {
				return raw.substring(0, 4) + "-" + raw.substring(4, 6) + "-" + raw.substring(6, 8);
			}
		} catch (Exception e) {
			// fall through
		}
		return raw;
	}

	private String decodeSex(String code) {
		if (code == null) return "";
		return switch (code.trim().toUpperCase()) {
			case "M" -> "Male";
			case "F" -> "Female";
			case "O" -> "Other";
			case "U" -> "Unknown";
			case "A" -> "Ambiguous";
			case "N" -> "Not applicable";
			default  -> code;
		};
	}

	private String decodePatientClass(String code) {
		if (code == null) return "";
		return switch (code.trim().toUpperCase()) {
			case "I" -> "Inpatient";
			case "O" -> "Outpatient";
			case "E" -> "Emergency";
			case "P" -> "Preadmit";
			case "R" -> "Recurring";
			case "B" -> "Obstetrics";
			case "C" -> "Commercial Account";
			case "N" -> "Not Applicable";
			case "U" -> "Unknown";
			default  -> code;
		};
	}

	private String decodeObxStatus(String code) {
		if (code == null) return "";
		return switch (code.trim().toUpperCase()) {
			case "C" -> "Corrected";
			case "D" -> "Deleted";
			case "F" -> "Final";
			case "I" -> "Pending";
			case "P" -> "Preliminary";
			case "R" -> "Not yet verified";
			case "S" -> "Partial";
			case "U" -> "Result Changed";
			case "W" -> "Post original as wrong";
			case "X" -> "Not available";
			default  -> code;
		};
	}

	private static String esc(String value) {
		return StringEscapeUtils.escapeHtml4(value == null ? "" : value);
	}

	// -------------------------------------------------------------------------
	// Lightweight HL7 v2.x message model — no external library required
	// -------------------------------------------------------------------------

	private static class Hl7Message {

		/**
		 * Segments indexed by name. Multiple segments with the same name (e.g. DG1, OBX) are
		 * stored in order in allSegments.
		 */
		private final List<String[]> allSegments = new ArrayList<>();

		/** Parse an HL7 v2.x message. Handles \r\n, \r, or \n line endings. */
		static Hl7Message parse(String raw) {
			Hl7Message msg = new Hl7Message();
			String[] lines = raw.split("\\r\\n|\\r|\\n");
			for (String line : lines) {
				line = line.trim();
				if (line.isEmpty()) continue;
				// Field separator is always | in HL7 v2
				String[] fields = line.split("\\|", -1);
				msg.allSegments.add(fields);
			}
			return msg;
		}

		boolean hasSegment(String segmentId) {
			for (String[] seg : allSegments) {
				if (seg.length > 0 && segmentId.equalsIgnoreCase(seg[0])) return true;
			}
			return false;
		}

		/** Get all segments with the given ID (e.g. multiple DG1 or OBX segments). */
		List<String[]> getAllSegments(String segmentId) {
			List<String[]> result = new ArrayList<>();
			for (String[] seg : allSegments) {
				if (seg.length > 0 && segmentId.equalsIgnoreCase(seg[0])) {
					result.add(seg);
				}
			}
			return result;
		}

		/**
		 * Get the raw field string at fieldIndex (1-based per HL7 spec).
		 *
		 * MSH is the one special case: MSH.1 is the field separator character ("|")
		 * itself, so it never appears as a token when the line is split on "|".
		 * That shifts all MSH array indices by -1 relative to every other segment:
		 *   other segments: seg[0]="PID", seg[1]=PID.1, seg[N]=PID.N
		 *   MSH:            seg[0]="MSH", seg[1]=MSH.2, seg[N]=MSH.(N+1)
		 * So to retrieve MSH.N we use array index N-1.
		 */
		String getSegmentField(String segmentId, int fieldIndex) {
			for (String[] seg : allSegments) {
				if (seg.length > 0 && segmentId.equalsIgnoreCase(seg[0])) {
					int idx = "MSH".equalsIgnoreCase(segmentId) ? fieldIndex - 1 : fieldIndex;
					return safeGet(seg, idx);
				}
			}
			return "";
		}

		/**
		 * Get a component (0-based) from a field (1-based field index).
		 * Components are separated by ^ in HL7.
		 */
		String getField(String segmentId, int fieldIndex, int componentIndex) {
			String raw = getSegmentField(segmentId, fieldIndex);
			if (raw.isEmpty()) return "";
			String[] parts = raw.split("\\^", -1);
			return componentIndex < parts.length ? parts[componentIndex].trim() : "";
		}

		/** Get the entire raw field value (with ^ subfields intact) for the first matching segment. */
		String getField(String segmentId, int fieldIndex) {
			return getSegmentField(segmentId, fieldIndex);
		}
	}

	// -------------------------------------------------------------------------
	// Safe array accessor helpers
	// -------------------------------------------------------------------------

	private static String safeGet(String[] arr, int index) {
		return (arr != null && index < arr.length) ? arr[index].trim() : "";
	}

	/** Get component [subIndex] (0-based, ^-split) from arr[index]. */
	private static String safeGet(String[] arr, int index, int subIndex) {
		String raw = safeGet(arr, index);
		if (raw.isEmpty()) return "";
		String[] parts = raw.split("\\^", -1);
		return subIndex < parts.length ? parts[subIndex].trim() : "";
	}

	@Override
	public String getReactorDescription() {
		return "Parses an HL7 v2.x message and generates a structured, downloadable PDF report. "
				+ "Returns a FILE_DOWNLOAD key the UI uses via GET /api/engine/downloadFile.";
	}

}
