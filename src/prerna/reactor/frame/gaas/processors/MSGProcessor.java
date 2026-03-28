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
package prerna.reactor.frame.gaas.processors;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.datatypes.MAPIProperty;
import org.apache.poi.hsmf.datatypes.PropertyValue;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class MSGProcessor extends AbstractFileProcessor {

    private static final Logger classLogger = LogManager.getLogger(MSGProcessor.class);

    public MSGProcessor(String filePath, VectorDatabaseCSVWriter writer) {
        super(filePath, writer);
    }

    @Override
    public void process() throws IOException {
        FileInputStream is = null;
        MAPIMessage msg = null;
        try {
            is = new FileInputStream(this.filePath);
            msg = new MAPIMessage(is);
            processMessage(msg);
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw e;
        } finally {
            if (msg != null) {
                try {
                    msg.close();
                } catch (IOException e) {
                    classLogger.error("Error closing MAPIMessage", e);
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    classLogger.error("Error closing FileInputStream", e);
                }
            }
        }
    }

    private void processMessage(MAPIMessage msg) {
        String source = getSource(this.filePath);
        StringBuilder content = new StringBuilder();

        // Email Header
        content.append("EMAIL HEADER\n");
        appendHeader(content, "Subject", getStringChunk(msg, "subject"));
        appendHeader(content, "From", getStringChunk(msg, "from"));
        appendHeader(content, "To", getStringChunk(msg, "to"));
        appendHeader(content, "CC", getStringChunk(msg, "cc"));
        appendHeader(content, "BCC", getStringChunk(msg, "bcc"));
        appendHeader(content, "Date", getMessageDate(msg));
        appendHeader(content, "Importance", getImportance(msg));
        content.append("\n");

        // Email Body
        content.append("EMAIL BODY\n");
        String body = getBody(msg);
        if (body == null || body.isEmpty()) {
            content.append("No body content found\n");
        } else {
            content.append(body).append("\n");
        }
        content.append("\n");

        // Attachments
        AttachmentChunks[] attachments = msg.getAttachmentFiles();
        if (attachments != null && attachments.length > 0) {
            content.append("EMAIL ATTACHMENTS\n");
            for (int i = 0; i < attachments.length; i++) {
                AttachmentChunks attachment = attachments[i];
                String fileName = "Unknown filename";
                if (attachment.getAttachLongFileName() != null) {
                    fileName = attachment.getAttachLongFileName().getValue();
                } else if (attachment.getAttachFileName() != null) {
                    fileName = attachment.getAttachFileName().getValue();
                }
                content.append("Attachment ").append(i + 1).append(": ").append(fileName);
                if (attachment.getAttachData() != null) {
                    byte[] data = attachment.getAttachData().getValue();
                    if (data != null) {
                        content.append(" (Size: ").append(data.length).append(" bytes)");
                    }
                }
                content.append("\n");
            }
            content.append("\n");
        }

        // Write as a single page
        this.writer.writeRow(source, "1", content.toString());
    }

    private void appendHeader(StringBuilder sb, String name, String value) {
        if (value != null && !value.isEmpty()) {
            sb.append(name).append(": ").append(value).append("\n");
        }
    }

    private String getStringChunk(MAPIMessage msg, String field) {
        try {
            switch (field) {
                case "subject":
                    return msg.getSubject();
                case "from":
                    return msg.getDisplayFrom();
                case "to":
                    return msg.getDisplayTo();
                case "cc":
                    return msg.getDisplayCC();
                case "bcc":
                    return msg.getDisplayBCC();
                default:
                    return null;
            }
        } catch (ChunkNotFoundException e) {
            classLogger.debug("Chunk not found for field: " + field, e);
            return null;
        }
    }

    private String getMessageDate(MAPIMessage msg) {
        try {
            Calendar cal = msg.getMessageDate();
            if (cal != null) {
                return cal.getTime().toString();
            }
        } catch (ChunkNotFoundException e) {
            classLogger.debug("Date chunk not found", e);
        }
        return null;
    }

    private String getImportance(MAPIMessage msg) {
        try {
            List<PropertyValue> values = msg.getMainChunks().getProperties().get(MAPIProperty.IMPORTANCE);
            if (values != null && !values.isEmpty()) {
                Object val = values.get(0).getValue();
                if (val instanceof Number) {
                    int importance = ((Number) val).intValue();
                    switch (importance) {
                        case 0:
                            return "Low";
                        case 1:
                            return "Normal";
                        case 2:
                            return "High";
                        default:
                            return "Normal";
                    }
                }
            }
        } catch (Exception e) {
            classLogger.debug("Importance property not found", e);
        }
        return "Normal";
    }

    private String getBody(MAPIMessage msg) {
        // Prefer plain text body, fall back to HTML with tag stripping
        try {
            String textBody = msg.getTextBody();
            if (textBody != null && !textBody.trim().isEmpty()) {
                return textBody.trim();
            }
        } catch (ChunkNotFoundException e) {
            classLogger.debug("Text body chunk not found", e);
        }

        try {
            String htmlBody = msg.getHtmlBody();
            if (htmlBody != null && !htmlBody.trim().isEmpty()) {
                return stripHtmlTags(htmlBody).trim();
            }
        } catch (ChunkNotFoundException e) {
            classLogger.debug("HTML body chunk not found", e);
        }

        return null;
    }

    private String stripHtmlTags(String html) {
        if (html == null) {
            return "";
        }
        String text = html.replaceAll("<[^>]+>", "");
        text = text.replace("&nbsp;", " ");
        text = text.replace("&amp;", "&");
        text = text.replace("&lt;", "<");
        text = text.replace("&gt;", ">");
        text = text.replace("&quot;", "\"");
        text = text.replace("&#39;", "'");
        text = text.replaceAll("\\s+", " ");
        return text.trim();
    }

}
