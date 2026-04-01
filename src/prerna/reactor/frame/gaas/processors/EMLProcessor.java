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
import java.util.Date;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Address;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class EMLProcessor extends AbstractFileProcessor {

    private static final Logger classLogger = LogManager.getLogger(EMLProcessor.class);

    public EMLProcessor(String filePath, VectorDatabaseCSVWriter writer) {
        super(filePath, writer);
    }

    @Override
    public void process() throws IOException {
        FileInputStream is = null;
        try {
            is = new FileInputStream(this.filePath);
            Session session = Session.getDefaultInstance(new Properties());
            MimeMessage message = new MimeMessage(session, is);
            processMessage(message);
        } catch (MessagingException e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IOException("Failed to parse EML file: " + this.filePath, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    classLogger.error("Error closing FileInputStream", e);
                }
            }
        }
    }

    private void processMessage(MimeMessage message) throws MessagingException, IOException {
        String source = getSource(this.filePath);
        StringBuilder content = new StringBuilder();

        // Email Header
        content.append("EMAIL HEADER\n");
        appendHeader(content, "Subject", message.getSubject());
        appendHeader(content, "From", addressesToString(message.getFrom()));
        appendHeader(content, "To", addressesToString(message.getRecipients(Message.RecipientType.TO)));
        appendHeader(content, "CC", addressesToString(message.getRecipients(Message.RecipientType.CC)));
        appendHeader(content, "BCC", addressesToString(message.getRecipients(Message.RecipientType.BCC)));
        appendHeader(content, "Date", formatDate(message.getSentDate()));
        appendHeader(content, "Importance", extractImportance(message));
        content.append("\n");

        // Email Body
        content.append("EMAIL BODY\n");
        StringBuilder bodyText = new StringBuilder();
        extractTextContent(message, bodyText);
        String body = bodyText.toString().trim();
        if (body.isEmpty()) {
            content.append("No body content found\n");
        } else {
            content.append(body).append("\n");
        }
        content.append("\n");

        // Attachments summary
        int attachmentCount = countAttachments(message);
        if (attachmentCount > 0) {
            content.append("EMAIL ATTACHMENTS\n");
            appendAttachmentInfo(message, content, new int[] { 1 });
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

    private String addressesToString(Address[] addresses) {
        if (addresses == null || addresses.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < addresses.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(addresses[i].toString());
        }
        return sb.toString();
    }

    private String formatDate(Date date) {
        if (date == null) {
            return null;
        }
        return date.toString();
    }

    private String extractImportance(MimeMessage message) throws MessagingException {
        // Check standard importance/priority headers
        String[] headerNames = { "Importance", "Priority", "X-Priority", "X-MSMail-Priority" };
        for (String headerName : headerNames) {
            String[] values = message.getHeader(headerName);
            if (values != null && values.length > 0) {
                return normalizePriority(values[0].trim(), headerName);
            }
        }
        return "Normal";
    }

    private String normalizePriority(String value, String headerName) {
        if (value == null || value.isEmpty()) {
            return "Normal";
        }
        String lower = value.toLowerCase().trim();

        if (headerName.equalsIgnoreCase("Importance")) {
            if ("high".equals(lower))
                return "High";
            if ("low".equals(lower))
                return "Low";
            return "Normal";
        }

        // Numeric priority headers (1=highest, 5=lowest)
        if ("1".equals(lower) || "urgent".equals(lower) || "high".equals(lower) || "2".equals(lower)) {
            return "High";
        }
        if ("5".equals(lower) || "non-urgent".equals(lower) || "low".equals(lower) || "4".equals(lower)) {
            return "Low";
        }
        return "Normal";
    }

    private void extractTextContent(Part part, StringBuilder textContent) throws MessagingException, IOException {
        if (part.isMimeType("text/plain")) {
            String text = (String) part.getContent();
            if (text != null) {
                textContent.append(text);
            }
        } else if (part.isMimeType("text/html")) {
            // Only use HTML if we haven't found plain text yet
            if (textContent.length() == 0) {
                String html = (String) part.getContent();
                if (html != null) {
                    textContent.append(stripHtmlTags(html));
                }
            }
        } else if (part.isMimeType("multipart/*")) {
            Multipart multipart = (Multipart) part.getContent();
            // First pass: look for text/plain
            for (int i = 0; i < multipart.getCount(); i++) {
                Part bodyPart = multipart.getBodyPart(i);
                String disposition = bodyPart.getDisposition();
                if (disposition == null || !disposition.equalsIgnoreCase(Part.ATTACHMENT)) {
                    if (bodyPart.isMimeType("text/plain")) {
                        extractTextContent(bodyPart, textContent);
                    }
                }
            }
            // Second pass: if no plain text found, try other parts (html, nested multipart)
            if (textContent.length() == 0) {
                for (int i = 0; i < multipart.getCount(); i++) {
                    Part bodyPart = multipart.getBodyPart(i);
                    String disposition = bodyPart.getDisposition();
                    if (disposition == null || !disposition.equalsIgnoreCase(Part.ATTACHMENT)) {
                        extractTextContent(bodyPart, textContent);
                        if (textContent.length() > 0) {
                            break;
                        }
                    }
                }
            }
        } else if (part.isMimeType("message/rfc822")) {
            // Nested email message
            Object nestedContent = part.getContent();
            if (nestedContent instanceof MimeMessage) {
                extractTextContent((MimeMessage) nestedContent, textContent);
            }
        }
    }

    private int countAttachments(Part part) throws MessagingException, IOException {
        int count = 0;
        if (part.isMimeType("multipart/*")) {
            Multipart multipart = (Multipart) part.getContent();
            for (int i = 0; i < multipart.getCount(); i++) {
                Part bodyPart = multipart.getBodyPart(i);
                String disposition = bodyPart.getDisposition();
                if (disposition != null && (disposition.equalsIgnoreCase(Part.ATTACHMENT)
                        || disposition.equalsIgnoreCase(Part.INLINE))) {
                    String fileName = bodyPart.getFileName();
                    if (fileName != null) {
                        count++;
                    }
                }
                // Recurse into nested multipart
                if (bodyPart.isMimeType("multipart/*")) {
                    count += countAttachments(bodyPart);
                }
            }
        }
        return count;
    }

    private void appendAttachmentInfo(Part part, StringBuilder content, int[] counter)
            throws MessagingException, IOException {
        if (part.isMimeType("multipart/*")) {
            Multipart multipart = (Multipart) part.getContent();
            for (int i = 0; i < multipart.getCount(); i++) {
                Part bodyPart = multipart.getBodyPart(i);
                String disposition = bodyPart.getDisposition();
                if (disposition != null && (disposition.equalsIgnoreCase(Part.ATTACHMENT)
                        || disposition.equalsIgnoreCase(Part.INLINE))) {
                    String fileName = bodyPart.getFileName();
                    if (fileName != null) {
                        content.append("Attachment ").append(counter[0]).append(": ").append(fileName);
                        int size = bodyPart.getSize();
                        if (size > 0) {
                            content.append(" (Size: ").append(size).append(" bytes)");
                        }
                        content.append("\n");
                        counter[0]++;
                    }
                }
                // Recurse into nested multipart
                if (bodyPart.isMimeType("multipart/*")) {
                    appendAttachmentInfo(bodyPart, content, counter);
                }
            }
        }
    }

    private String stripHtmlTags(String html) {
        if (html == null) {
            return "";
        }
        // Remove HTML tags
        String text = html.replaceAll("<[^>]+>", "");
        // Replace common HTML entities
        text = text.replace("&nbsp;", " ");
        text = text.replace("&amp;", "&");
        text = text.replace("&lt;", "<");
        text = text.replace("&gt;", ">");
        text = text.replace("&quot;", "\"");
        text = text.replace("&#39;", "'");
        // Collapse whitespace
        text = text.replaceAll("\\s+", " ");
        return text.trim();
    }

}
