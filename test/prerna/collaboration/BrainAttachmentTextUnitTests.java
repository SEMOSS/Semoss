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

import static org.junit.jupiter.api.Assertions.*;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFNotes;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextBox;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

class BrainAttachmentTextUnitTests {

    @TempDir
    Path dir;

    private String read(Path file, String name) throws Exception {
        BrainAttachmentText.Extracted text = BrainAttachmentText.extract(file, name, "HEADER\n\n",
                BrainAttachmentText.MAX_CHARS);
        assertNotNull(text);
        assertTrue(text.text().startsWith("HEADER\n\n"));
        return text.text();
    }

    @Test
    void word() throws Exception {
        Path file = dir.resolve("a.docx");
        try (XWPFDocument doc = new XWPFDocument(); OutputStream out = Files.newOutputStream(file)) {
            doc.createParagraph().createRun().setText("Scope of work");
            doc.createParagraph().createRun().setText("Deliver by Friday");
            doc.write(out);
        }
        String text = read(file, "Plan.docx");
        assertTrue(text.contains("Scope of work"));
        assertTrue(text.contains("Deliver by Friday"));
    }

    @Test
    void excelKeepsSheetNamesAndCachedFormulaResults() throws Exception {
        Path file = dir.resolve("a.xlsx");
        try (XSSFWorkbook book = new XSSFWorkbook(); OutputStream out = Files.newOutputStream(file)) {
            XSSFSheet sheet = book.createSheet("Budget");
            sheet.createRow(0).createCell(0).setCellValue("Travel");
            sheet.getRow(0).createCell(1).setCellValue(1200);
            sheet.createRow(1).createCell(0).setCellValue("Total");
            sheet.getRow(1).createCell(1).setCellFormula("B1*2");
            book.getCreationHelper().createFormulaEvaluator().evaluateAll();
            book.write(out);
        }
        String text = read(file, "Budget.xlsx");
        assertTrue(text.contains("Budget"));
        assertTrue(text.contains("Travel"));
        assertTrue(text.contains("2400"), text);
        assertFalse(text.contains("B1*2"), text);
    }

    @Test
    void powerPointIncludesSpeakerNotes() throws Exception {
        Path file = dir.resolve("a.pptx");
        try (XMLSlideShow show = new XMLSlideShow(); OutputStream out = Files.newOutputStream(file)) {
            XSLFSlide slide = show.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setText("Roadmap 2027");
            XSLFNotes notes = show.getNotesSlide(slide);
            notes.getPlaceholder(1).setText("Mention the hiring plan");
            show.write(out);
        }
        String text = read(file, "Deck.pptx");
        assertTrue(text.contains("Roadmap 2027"), text);
        assertTrue(text.contains("Mention the hiring plan"), text);
    }

    @Test
    void mailFileGivesHeadersBodyAndAttachedNames() throws Exception {
        Path file = dir.resolve("a.eml");
        MimeMessage message = new MimeMessage(Session.getInstance(new Properties()));
        message.setFrom(new InternetAddress("jane@example.com", "Jane Doe"));
        message.setSubject("Contract review");
        MimeBodyPart body = new MimeBodyPart();
        body.setText("Please sign by Monday.");
        MimeBodyPart attachment = new MimeBodyPart();
        attachment.setContent(new byte[] { 1, 2, 3 }, "application/octet-stream");
        attachment.setFileName("contract.pdf");
        attachment.setDisposition(MimeBodyPart.ATTACHMENT);
        message.setContent(new MimeMultipart(body, attachment));
        message.saveChanges();
        try (OutputStream out = Files.newOutputStream(file)) {
            message.writeTo(out);
        }
        String text = read(file, "Forwarded.eml");
        assertTrue(text.contains("From: Jane Doe <jane@example.com>"), text);
        assertTrue(text.contains("Subject: Contract review"), text);
        assertTrue(text.contains("Please sign by Monday."), text);
        assertTrue(text.contains("Attached to this email: contract.pdf"), text);
    }

    @Test
    void longTextIsCutAndSaysSo() throws Exception {
        Path file = dir.resolve("a.docx");
        try (XWPFDocument doc = new XWPFDocument(); OutputStream out = Files.newOutputStream(file)) {
            doc.createParagraph().createRun().setText("x".repeat(500));
            doc.write(out);
        }
        BrainAttachmentText.Extracted text = BrainAttachmentText.extract(file, "Long.docx", "", 100);
        assertTrue(text.truncated());
        assertTrue(text.text().endsWith("[Cut at 100 characters. Open the original file for the rest.]"));
    }

    @Test
    void onlyOfficeAndMailFilesAreRead() throws Exception {
        assertTrue(BrainAttachmentText.supports("A.DOCX"));
        assertTrue(BrainAttachmentText.supports("note.msg"));
        assertTrue(BrainAttachmentText.supports("fwd.eml"));
        assertFalse(BrainAttachmentText.supports("report.pdf"));
        assertFalse(BrainAttachmentText.supports("photo.png"));
        assertFalse(BrainAttachmentText.supports("README"));
        assertNull(BrainAttachmentText.extract(dir.resolve("none.pdf"), "none.pdf", "", 100));
    }
}
