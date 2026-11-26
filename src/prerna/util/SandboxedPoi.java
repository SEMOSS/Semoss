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

/**
 * The sandbox rules for Apache POI, which project reactor code shares with the
 * platform for Excel and Office files. The POI family is shared with its schema
 * classes, and the rules cover what POI's public methods do for their caller:
 *
 * <ul>
 * <li>A path POI takes as text follows the path rule of
 * {@link SandboxedJavaExecution}.</li>
 * <li>The static settings that hold for every document POI reads are refused:
 * its size and record limits, its zip bomb checks and its temp file
 * settings.</li>
 * <li>The registries that run a project's code later, in other users' requests,
 * are refused: workbook, extractor and slide show providers, formula functions,
 * the default draw factory, the sheet width fallback, the temp file strategy and
 * thread local cleaners, along with the JVM security providers POI can
 * install.</li>
 * <li>The settings POI keeps per thread are refused, since a server thread
 * serves many users in turn: the user locale and time zone, the password for
 * encrypted workbooks, the temp file strategy and the event extractor
 * preference.</li>
 * </ul>
 */
final class SandboxedPoi {

	/**
	 * The packages project reactor code shares with the platform, as name
	 * prefixes: POI, the streaming Excel reader, and the schema classes they read
	 * documents with.
	 */
	static final List<String> SHARED_PACKAGES = List.of("org.apache.poi.", "com.github.pjfanning.xlsx.",
			"org.openxmlformats.schemas.", "com.microsoft.schemas.", "org.etsi.uri.", "org.w3.x2000.");

	/**
	 * Calls that take a path as text, by owner, then method name, then the position
	 * of the path argument.
	 */
	static final Map<String, Map<String, Integer>> PATH_TEXT_ARGUMENTS = Map.of(
			"org/apache/poi/xssf/usermodel/XSSFWorkbook", Map.of("<init>", 0),
			"org/apache/poi/hslf/usermodel/HSLFSlideShow", Map.of("<init>", 0),
			"org/apache/poi/openxml4j/opc/OPCPackage", Map.of("open", 0, "create", 0),
			"org/apache/poi/ooxml/POIXMLDocument", Map.of("openPackage", 0));

	/**
	 * Calls project reactor code cannot make, by owner then method name: the
	 * settings, registries and per thread settings above, on every POI class that
	 * declares one.
	 */
	static final Map<String, Set<String>> BLOCKED_CALLS = Map.ofEntries(
			Map.entry("org/apache/poi/ddf/EscherArrayProperty", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherBSERecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherBlipRecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherClientAnchorRecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherClientDataRecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherComplexProperty", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherMetafileBlip", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/EscherRecord", Set.of("setMaxNumberOfChildren")),
			Map.entry("org/apache/poi/ddf/EscherTextboxRecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ddf/UnknownEscherRecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/extractor/ExtractorFactory",
					Set.of("addProvider", "removeProvider", "removeThreadPrefersEventExtractorsSetting",
							"setAllThreadsPreferEventExtractors", "setThreadPrefersEventExtractors")),
			Map.entry("org/apache/poi/hdgf/chunks/ChunkFactory", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hdgf/streams/CompressedStreamStore", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hdgf/streams/PointerContainingStream", Set.of("setMaxChildrenNesting")),
			Map.entry("org/apache/poi/hdgf/streams/StreamStore", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hemf/record/emf/HemfText", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hemf/usermodel/HemfEmbeddedIterator", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hmef/attribute/MAPIAttribute", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hmef/attribute/TNEFAttribute", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hmef/dev/HMEFDumper", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpbf/model/EscherPart", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpbf/model/qcbits/QCTextBit", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpsf/Array", Set.of("setMaxNumberOfArrayScalars")),
			Map.entry("org/apache/poi/hpsf/Blob", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpsf/ClipboardData", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpsf/CodePageString", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpsf/Thumbnail", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hpsf/VariantSupport", Set.of("setLogUnsupportedTypes")),
			Map.entry("org/apache/poi/hslf/dev/SlideShowDumper", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/record/FontEmbeddedData", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/record/MasterTextPropAtom", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/record/RecordAtom", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/record/SoundData", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/record/StyleTextProp9Atom", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/usermodel/HSLFSlideShow", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hslf/usermodel/HSLFSlideShowImpl", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hsmf/datatypes/PropertiesChunk", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hssf/record/DrawingGroupRecord", Set.of("setMaxRecordSize")),
			Map.entry("org/apache/poi/hssf/record/EscherAggregate", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hssf/record/RecordFactory", Set.of("setMaxNumberOfRecords")),
			Map.entry("org/apache/poi/hssf/record/SubRecord", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hssf/record/crypto/Biff8EncryptionKey", Set.of("setCurrentUserPassword")),
			Map.entry("org/apache/poi/hssf/usermodel/HSSFWorkbook", Set.of("setMaxImageLength", "setMaxRecordLength")),
			Map.entry("org/apache/poi/hwmf/record/HwmfText", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hwmf/usermodel/HwmfPicture", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hwpf/HWPFDocument", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hwpf/HWPFDocumentCore", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hwpf/HWPFOldDocument", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hwpf/model/TextPieceTable", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/hwpf/sprm/SprmUtils", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ooxml/extractor/POIXMLExtractorFactory",
					Set.of("removeThreadPrefersEventExtractorsSetting", "setAllThreadsPreferEventExtractors",
							"setThreadPrefersEventExtractors")),
			Map.entry("org/apache/poi/openxml4j/opc/ZipPackage",
					Set.of("setEncryptTempFilePackageParts", "setUseTempFilePackageParts")),
			Map.entry("org/apache/poi/openxml4j/util/ZipArchiveFakeEntry", Set.of("setMaxEntrySize")),
			Map.entry("org/apache/poi/openxml4j/util/ZipInputStreamZipEntrySource",
					Set.of("setEncryptTempFiles", "setThresholdBytesForTempFiles")),
			Map.entry("org/apache/poi/openxml4j/util/ZipSecureFile",
					Set.of("setGraceEntrySize", "setMaxEntrySize", "setMaxFileCount", "setMaxTextSize",
							"setMinInflateRatio")),
			Map.entry("org/apache/poi/poifs/crypt/CryptoFunctions",
					Set.of("registerBouncyCastle", "setMaxRecordLength")),
			Map.entry("org/apache/poi/poifs/crypt/dsig/services/RelationshipTransformService",
					Set.of("registerDsigProvider")),
			Map.entry("org/apache/poi/poifs/crypt/dsig/services/TimeStampSimpleHttpClient",
					Set.of("setMaxTimestampResponseSize")),
			Map.entry("org/apache/poi/poifs/filesystem/Ole10Native",
					Set.of("setMaxRecordLength", "setMaxStringLength")),
			Map.entry("org/apache/poi/poifs/filesystem/POIFSFileSystem", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/sl/draw/DrawFactory", Set.of("setDefaultFactory")),
			Map.entry("org/apache/poi/sl/usermodel/SlideShowFactory", Set.of("addProvider", "removeProvider")),
			Map.entry("org/apache/poi/ss/extractor/EmbeddedExtractor", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/ss/formula/WorkbookEvaluator", Set.of("registerFunction")),
			Map.entry("org/apache/poi/ss/formula/atp/AnalysisToolPak", Set.of("registerFunction")),
			Map.entry("org/apache/poi/ss/formula/eval/FunctionEval", Set.of("registerFunction")),
			Map.entry("org/apache/poi/ss/usermodel/DateUtil", Set.of("enableThreadLocalCache")),
			Map.entry("org/apache/poi/ss/usermodel/WorkbookFactory", Set.of("addProvider", "removeProvider")),
			Map.entry("org/apache/poi/ss/util/SheetUtil", Set.of("setFailoverFunction")),
			Map.entry("org/apache/poi/util/IOUtils", Set.of("setByteArrayMaxOverride", "setMaxByteArrayInitSize")),
			Map.entry("org/apache/poi/util/LZWDecompresser", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/util/LocaleUtil", Set.of("setUserLocale", "setUserTimeZone")),
			Map.entry("org/apache/poi/util/StringUtil", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/util/TempFile",
					Set.of("setTempFileCreationStrategy", "setThreadLocalTempFileCreationStrategy")),
			Map.entry("org/apache/poi/util/ThreadLocalUtil", Set.of("clearAllThreadLocals", "registerCleaner")),
			Map.entry("org/apache/poi/xslf/usermodel/XMLSlideShow", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/xslf/usermodel/XSLFPictureData", Set.of("setMaxImageSize")),
			Map.entry("org/apache/poi/xssf/binary/XSSFBParser", Set.of("setMaxRecordLength")),
			Map.entry("org/apache/poi/xssf/usermodel/XSSFPictureData", Set.of("setMaxImageSize")),
			Map.entry("org/apache/poi/xwpf/usermodel/XWPFPictureData", Set.of("setMaxImageSize")));

	private SandboxedPoi() {
	}
}
