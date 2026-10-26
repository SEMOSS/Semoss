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
package prerna.util.pptx;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import javax.imageio.ImageIO;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/** Renders a snapshot of a deck without changing its source or rebuilding its slides. */
public final class PptxRenderService {
    public static final int IMAGE_LONG_EDGE = 1600;
    public static final int MAX_SELECTED_SLIDES = 100;
    private static final long MAX_FILE_BYTES = 50L * 1024 * 1024;
    private static final int MAX_XML_BYTES = 8 * 1024 * 1024;
    private static final long MAX_EXPANDED_BYTES = 500L * 1024 * 1024;
    private static final String PROFILE = "pptx-pdf-unhide-v1";
    private static final Object[] LOCKS = new Object[32];
    static { for (int i = 0; i < LOCKS.length; i++) LOCKS[i] = new Object(); }

    public interface Converter {
        byte[] convert(Path pptx) throws Exception;
        /** Include the service address and change this value after renderer/font upgrades. */
        String cacheKey();
    }

    public record SlideImage(int slide, Path path, int width, int height) {}
    public record RenderedDeck(String sourceHash, int slideCount, List<Integer> requestedSlides,
            List<Integer> hiddenSlides, Path pdf, List<SlideImage> images, Path runDirectory, boolean cacheHit) {}
    private record DeckMetadata(List<String> slideParts, Map<String, byte[]> hiddenParts,
            List<Integer> hiddenSlides) {}

    private final Converter converter;

    public PptxRenderService(Converter converter) { this.converter = converter; }

    public RenderedDeck render(Path root, String filePath, List<Integer> selection, Runnable checkActive)
            throws Exception {
        root = root.toRealPath();
        Path source = resolveSource(root, filePath);
        long size = Files.size(source);
        if (size == 0 || size > MAX_FILE_BYTES) throw new IllegalArgumentException("PPTX must be between 1 byte and 50 MiB");
        byte[] snapshot;
        try (InputStream in = Files.newInputStream(source)) { snapshot = readLimited(in, MAX_FILE_BYTES); }
        String sourceHash = sha256(snapshot);
        Path base = safeDirectory(root, ".pptx-review");
        Path run = safeDirectory(base, "review-" + UUID.randomUUID());
        Path input = run.resolve("render-input.pptx");
        Files.write(input, snapshot);
        try {
            DeckMetadata metadata = metadata(input);
            List<Integer> selected = selectSlides(selection, metadata.slideParts.size());
            checkActive.run();
            if (!metadata.hiddenParts.isEmpty()) {
                Path expanded = run.resolve("visible-slides.pptx");
                unhide(input, expanded, metadata.hiddenParts);
                Files.delete(input);
                Files.move(expanded, input);
            }
            String key = sha256((sourceHash + "\n" + PROFILE + "\n" + converter.cacheKey()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Path cache = safeDirectory(safeDirectory(base, "cache"), key);
            Path cachedPdf = cache.resolve("deck.pdf");
            Path digest = cache.resolve("pdf.sha256");
            Path pdf = run.resolve("deck.pdf");
            boolean hit;
            synchronized (LOCKS[(key.hashCode() & Integer.MAX_VALUE) % LOCKS.length]) {
                hit = validCache(cachedPdf, digest);
                if (!hit) {
                    checkActive.run();
                    byte[] converted = converter.convert(input);
                    if (converted == null || converted.length == 0 || converted.length > MAX_EXPANDED_BYTES)
                        throw new IOException("UnoServer returned an empty or oversized PDF");
                    try (PDDocument document = Loader.loadPDF(converted)) {
                        checkPageCount(document, metadata.slideParts.size());
                    }
                    if (Files.isSymbolicLink(cachedPdf) || Files.isSymbolicLink(digest))
                        throw new IOException("Render cache cannot contain symbolic links");
                    Files.write(cachedPdf, converted);
                    Files.writeString(digest, sha256(converted));
                }
                Files.copy(cachedPdf, pdf);
            }
            List<SlideImage> images = new ArrayList<>();
            try (PDDocument document = Loader.loadPDF(pdf.toFile())) {
                checkPageCount(document, metadata.slideParts.size());
                PDFRenderer renderer = new PDFRenderer(document);
                renderer.setSubsamplingAllowed(true);
                for (int slide : selected) {
                    checkActive.run();
                    var box = document.getPage(slide - 1).getCropBox();
                    float edge = Math.max(box.getWidth(), box.getHeight());
                    if (!Float.isFinite(edge) || edge <= 0) throw new IOException("Invalid PDF page dimensions");
                    BufferedImage image = renderer.renderImage(slide - 1, IMAGE_LONG_EDGE / edge, ImageType.RGB);
                    Path imagePath = run.resolve(String.format("slide-%04d.png", slide));
                    try {
                        if (!ImageIO.write(image, "png", imagePath.toFile())) throw new IOException("PNG encoder unavailable");
                        images.add(new SlideImage(slide, imagePath, image.getWidth(), image.getHeight()));
                    } finally { image.flush(); }
                }
            }
            return new RenderedDeck(sourceHash, metadata.slideParts.size(), List.copyOf(selected),
                    List.copyOf(metadata.hiddenSlides), pdf, List.copyOf(images), run, hit);
        } finally {
            Files.deleteIfExists(input);
            Files.deleteIfExists(run.resolve("visible-slides.pptx"));
        }
    }

    public static Path resolveSource(Path root, String filePath) throws IOException {
        if (filePath == null || filePath.isBlank() || Path.of(filePath).isAbsolute())
            throw new IllegalArgumentException("filePath must be relative to the working directory");
        Path source = root.resolve(filePath).toRealPath();
        if (!source.startsWith(root.toRealPath()) || !Files.isRegularFile(source))
            throw new IllegalArgumentException("PPTX must be a file inside the working directory");
        if (!source.getFileName().toString().toLowerCase(java.util.Locale.ROOT).endsWith(".pptx"))
            throw new IllegalArgumentException("Only .pptx files are supported");
        return source;
    }

    static List<Integer> selectSlides(List<Integer> selection, int count) {
        if (count == 0) throw new IllegalArgumentException("The presentation contains no slides");
        List<Integer> selected = selection == null
                ? java.util.stream.IntStream.rangeClosed(1, count).boxed().toList() : selection;
        if (selected.isEmpty() || selected.size() > MAX_SELECTED_SLIDES)
            throw new IllegalArgumentException("Select between 1 and " + MAX_SELECTED_SLIDES + " slides per inspection");
        for (Integer slide : selected) {
            if (slide == null || slide < 1 || slide > count)
                throw new IllegalArgumentException("Slide numbers must be integers from 1 to " + count);
        }
        if (new LinkedHashSet<>(selected).size() != selected.size())
            throw new IllegalArgumentException("Slide selection must not contain duplicates");
        return selected.stream().sorted().toList();
    }

    private static DeckMetadata metadata(Path pptx) throws Exception {
        try (ZipFile zip = new ZipFile(pptx.toFile())) {
            if (zip.size() > 10000) throw new IOException("PPTX contains too many ZIP entries");
            var names = new java.util.HashSet<String>();
            long expanded = 0;
            for (var entries = zip.entries(); entries.hasMoreElements();) {
                ZipEntry entry = entries.nextElement();
                if (!names.add(entry.getName())) throw new IOException("PPTX contains duplicate ZIP entries");
                if (entry.getSize() < 0 || (expanded += entry.getSize()) > MAX_EXPANDED_BYTES)
                    throw new IOException("PPTX expanded size exceeds 500 MiB");
            }
            Document presentation = xml(readEntry(zip, "ppt/presentation.xml"));
            Document relationships = xml(readEntry(zip, "ppt/_rels/presentation.xml.rels"));
            Map<String, String> targets = new HashMap<>();
            var rels = relationships.getDocumentElement().getChildNodes();
            for (int i = 0; i < rels.getLength(); i++) {
                if (!(rels.item(i) instanceof Element rel) || !"Relationship".equals(rel.getLocalName())) continue;
                if (!rel.getAttribute("Type").endsWith("/slide")) continue;
                if ("External".equals(rel.getAttribute("TargetMode"))) throw new IOException("External slide relationship");
                String target = rel.getAttribute("Target");
                String part = (target.startsWith("/") ? Path.of(target.substring(1)) : Path.of("ppt").resolve(target))
                        .normalize().toString().replace('\\', '/');
                if (!part.startsWith("ppt/slides/") || !part.endsWith(".xml")) throw new IOException("Invalid slide relationship");
                if (targets.put(rel.getAttribute("Id"), part) != null) throw new IOException("Duplicate slide relationship");
            }
            List<String> slides = new ArrayList<>();
            List<Integer> hidden = new ArrayList<>();
            Map<String, byte[]> hiddenParts = new HashMap<>();
            var ids = presentation.getElementsByTagNameNS("*", "sldId");
            for (int i = 0; i < ids.getLength(); i++) {
                Element id = (Element) ids.item(i);
                if (!"sldIdLst".equals(id.getParentNode().getLocalName())) continue;
                String rid = "";
                for (int a = 0; a < id.getAttributes().getLength(); a++) {
                    Node attribute = id.getAttributes().item(a);
                    if ("id".equals(attribute.getLocalName()) && attribute.getNamespaceURI() != null
                            && attribute.getNamespaceURI().endsWith("/relationships")) rid = attribute.getNodeValue();
                }
                String part = targets.get(rid);
                if (part == null || slides.contains(part)) throw new IOException("Missing or duplicate slide relationship");
                slides.add(part);
                Document slide = xml(readEntry(zip, part));
                String show = slide.getDocumentElement().getAttribute("show");
                if ("0".equals(show) || "false".equals(show)) {
                    hidden.add(slides.size());
                    slide.getDocumentElement().setAttribute("show", "1");
                    TransformerFactory factory = TransformerFactory.newDefaultInstance();
                    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    factory.newTransformer().transform(new DOMSource(slide), new StreamResult(out));
                    hiddenParts.put(part, out.toByteArray());
                }
            }
            return new DeckMetadata(slides, hiddenParts, hidden);
        }
    }

    private static void unhide(Path source, Path target, Map<String, byte[]> replacements) throws IOException {
        try (ZipFile zip = new ZipFile(source.toFile()); ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(target))) {
            long expanded = 0;
            for (var entries = zip.entries(); entries.hasMoreElements();) {
                ZipEntry entry = entries.nextElement();
                out.putNextEntry(new ZipEntry(entry.getName()));
                byte[] replacement = replacements.get(entry.getName());
                if (replacement != null) out.write(replacement);
                else if (!entry.isDirectory()) {
                    try (InputStream in = zip.getInputStream(entry)) {
                        byte[] buffer = new byte[16384];
                        int read;
                        while ((read = in.read(buffer)) != -1) {
                            if ((expanded += read) > MAX_EXPANDED_BYTES) throw new IOException("PPTX expanded size exceeds 500 MiB");
                            out.write(buffer, 0, read);
                        }
                    }
                }
                out.closeEntry();
            }
        }
    }

    private static Document xml(byte[] bytes) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory.newDocumentBuilder().parse(new ByteArrayInputStream(bytes));
    }

    private static byte[] readEntry(ZipFile zip, String name) throws IOException {
        ZipEntry entry = zip.getEntry(name);
        if (entry == null) throw new IOException("Missing PPTX part: " + name);
        try (InputStream in = zip.getInputStream(entry)) { return readLimited(in, MAX_XML_BYTES); }
    }

    private static byte[] readLimited(InputStream in, long limit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[16384];
        int read;
        while ((read = in.read(buffer)) != -1) {
            if ((long) out.size() + read > limit) throw new IOException("Input exceeds size limit");
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static Path safeDirectory(Path parent, String name) throws IOException {
        Path directory = parent.resolve(name);
        if (Files.isSymbolicLink(directory)) throw new IOException("Render directory cannot be a symbolic link");
        Files.createDirectories(directory);
        return directory.toRealPath();
    }

    private static boolean validCache(Path pdf, Path digest) throws Exception {
        if (Files.isSymbolicLink(pdf) || Files.isSymbolicLink(digest)) throw new IOException("Render cache cannot contain symbolic links");
        if (!Files.isRegularFile(pdf) || !Files.isRegularFile(digest) || Files.size(pdf) > MAX_EXPANDED_BYTES
                || Files.size(digest) > 128 || System.currentTimeMillis() - Files.getLastModifiedTime(pdf).toMillis() > Duration.ofHours(24).toMillis())
            return false;
        return Files.readString(digest).equals(hashFile(pdf));
    }

    public static String hashFile(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[16384];
            int read;
            while ((read = in.read(buffer)) != -1) digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void checkPageCount(PDDocument document, int expected) throws IOException {
        if (document.getNumberOfPages() != expected)
            throw new IOException("UnoServer PDF has " + document.getNumberOfPages() + " pages for " + expected
                    + " slides; original slide numbering cannot be verified");
    }
}
