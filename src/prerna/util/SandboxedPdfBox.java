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
package prerna.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;

/**
 * The sandbox rules for PDFBox and FontBox, which project reactor code shares
 * with the platform because a project copy cannot load: PDFBox's IOUtils reads
 * sun.misc.Unsafe when it initializes. Shared code runs without the rewrite, so
 * the rules cover what PDFBox's public methods do for their caller:
 *
 * <ul>
 * <li>A path PDFBox takes as text follows the path rule of
 * {@link SandboxedJavaExecution}.</li>
 * <li>The calls that change what every document in the JVM shares, or free
 * memory other code may still read, are refused.</li>
 * <li>The page sizes PDFBox shares between every document, such as
 * {@link PDRectangle#A4}, reach project code as copies, and a new page gets a
 * media box of its own.</li>
 * </ul>
 */
public final class SandboxedPdfBox {

	/**
	 * Internal name of this class, which the rewritten calls name.
	 */
	static final String INTERNAL_NAME = "prerna/util/SandboxedPdfBox";

	/**
	 * Descriptor of PDFBox's rectangle. Its page sizes are single objects every
	 * document in the JVM shares, so project code gets a copy of one wherever it
	 * reads or is handed one.
	 */
	static final String PD_RECTANGLE_DESCRIPTOR = "Lorg/apache/pdfbox/pdmodel/common/PDRectangle;";

	/**
	 * Descriptor of PDFBox's page, whose constructors move to this class so a new
	 * page gets a media box of its own.
	 */
	static final String PD_PAGE_DESCRIPTOR = "Lorg/apache/pdfbox/pdmodel/PDPage;";

	/**
	 * The packages project reactor code shares with the platform, as name
	 * prefixes.
	 */
	static final List<String> SHARED_PACKAGES = List.of("org.apache.pdfbox.", "org.apache.fontbox.");

	/**
	 * Calls project reactor code cannot make, by owner then method name. They
	 * change the font mapper, resource cache, security handlers, security provider,
	 * name cache or built-in font charsets every document shares, free memory that
	 * other code may still read (IOUtils.unmap), or read file paths out of a map
	 * (Overlay.overlay).
	 */
	static final Map<String, Set<String>> BLOCKED_CALLS = Map.ofEntries(
			Map.entry("org/apache/pdfbox/io/IOUtils", Set.of("unmap")),
			Map.entry("org/apache/pdfbox/cos/COSName", Set.of("clearResources")),
			Map.entry("org/apache/pdfbox/pdmodel/ResourceCacheFactory", Set.of("setResourceCacheCreateFunction")),
			Map.entry("org/apache/pdfbox/pdmodel/font/FontMappers", Set.of("set")),
			Map.entry("org/apache/pdfbox/pdmodel/encryption/SecurityProvider", Set.of("setProvider")),
			Map.entry("org/apache/pdfbox/pdmodel/encryption/SecurityHandlerFactory", Set.of("registerHandler")),
			Map.entry("org/apache/pdfbox/multipdf/Overlay", Set.of("overlay")),
			Map.entry("org/apache/fontbox/cff/CFFISOAdobeCharset", Set.of("getInstance")),
			Map.entry("org/apache/fontbox/cff/CFFExpertCharset", Set.of("getInstance")),
			Map.entry("org/apache/fontbox/cff/CFFExpertSubsetCharset", Set.of("getInstance")));

	/**
	 * Calls that take a path as text, by owner, then method name, then the position
	 * of the path argument.
	 */
	static final Map<String, Map<String, Integer>> PATH_TEXT_ARGUMENTS = Map.ofEntries(
			Map.entry("org/apache/pdfbox/Loader", Map.of("loadFDF", 0, "loadXFDF", 0)),
			Map.entry("org/apache/pdfbox/pdmodel/PDDocument", Map.of("save", 0)),
			Map.entry("org/apache/pdfbox/pdmodel/fdf/FDFDocument", Map.of("save", 0, "saveXFDF", 0)),
			Map.entry("org/apache/pdfbox/multipdf/PDFMergerUtility",
					Map.of("addSource", 0, "setDestinationFileName", 0)),
			Map.entry("org/apache/pdfbox/multipdf/Overlay",
					Map.of("setInputFile", 0, "setDefaultOverlayFile", 0, "setFirstPageOverlayFile", 0,
							"setLastPageOverlayFile", 0, "setAllPagesOverlayFile", 0, "setOddPageOverlayFile", 0,
							"setEvenPageOverlayFile", 0)),
			Map.entry("org/apache/pdfbox/pdmodel/graphics/image/PDImageXObject", Map.of("createFromFile", 0)),
			Map.entry("org/apache/pdfbox/pdmodel/interactive/digitalsignature/visible/PDVisibleSignDesigner",
					Map.of("<init>", 0, "signatureImage", 0)),
			Map.entry("org/apache/pdfbox/io/RandomAccessReadBufferedFile", Map.of("<init>", 0)),
			Map.entry("org/apache/pdfbox/io/RandomAccessReadMemoryMappedFile", Map.of("<init>", 0)),
			Map.entry("org/apache/fontbox/pfb/PfbParser", Map.of("<init>", 0)),
			Map.entry("org/apache/fontbox/util/autodetect/FontFileFinder", Map.of("find", 0)));

	/**
	 * The constructors that move to the methods of this class, by class, then
	 * constructor descriptor, as {@code [method, descriptor]}.
	 */
	static final Map<String, Map<String, String[]>> CONSTRUCTOR_REDIRECTS = Map.of("org/apache/pdfbox/pdmodel/PDPage",
			Map.of("()V", factory(""), "(" + PD_RECTANGLE_DESCRIPTOR + ")V", factory(PD_RECTANGLE_DESCRIPTOR),
					"(Lorg/apache/pdfbox/cos/COSDictionary;)V", factory("Lorg/apache/pdfbox/cos/COSDictionary;")));

	private SandboxedPdfBox() {
	}

	/**
	 * The redirect to {@code pdPage} for one PDPage constructor.
	 *
	 * @param arguments the constructor's argument descriptors
	 * @return the method and its descriptor
	 */
	private static String[] factory(String arguments) {
		return new String[] { "pdPage", "(" + arguments + ")" + PD_PAGE_DESCRIPTOR };
	}

	/**
	 * A rectangle a project's code gets from PDFBox, or a copy of it when it is one
	 * of the page sizes PDFBox shares between every document in the JVM, such as
	 * {@link PDRectangle#A4}, so a change to it stays in the project's own
	 * document.
	 *
	 * @param box the rectangle
	 * @return the rectangle, or a copy of a shared page size
	 */
	public static PDRectangle unsharedPageSize(PDRectangle box) {
		if (box == null) {
			return null;
		}
		for (PDRectangle size : List.of(PDRectangle.LETTER, PDRectangle.LEGAL, PDRectangle.TABLOID, PDRectangle.A0,
				PDRectangle.A1, PDRectangle.A2, PDRectangle.A3, PDRectangle.A4, PDRectangle.A5, PDRectangle.A6)) {
			if (box == size || box.getCOSArray() == size.getCOSArray()) {
				return new PDRectangle(box.getLowerLeftX(), box.getLowerLeftY(), box.getWidth(), box.getHeight());
			}
		}
		return box;
	}

	/**
	 * Sandboxed replacement for {@code new PDPage()}: a US Letter page with a media
	 * box of its own.
	 *
	 * @return the page
	 */
	public static PDPage pdPage() {
		return new PDPage(unsharedPageSize(PDRectangle.LETTER));
	}

	/**
	 * Sandboxed replacement for {@code new PDPage(PDRectangle)}: a page whose media
	 * box is its own, also when it is given a shared page size.
	 *
	 * @param mediaBox the page's size
	 * @return the page
	 */
	public static PDPage pdPage(PDRectangle mediaBox) {
		return new PDPage(unsharedPageSize(mediaBox));
	}

	/**
	 * Sandboxed replacement for {@code new PDPage(COSDictionary)}.
	 *
	 * @param page the page's dictionary
	 * @return the page
	 */
	public static PDPage pdPage(COSDictionary page) {
		return new PDPage(page);
	}
}
