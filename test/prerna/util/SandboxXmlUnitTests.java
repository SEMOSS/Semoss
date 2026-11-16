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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Consumer;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * One test per rule on the XML a project's custom reactors parse and transform.
 * A file outside the user's folder holds a secret, and each test checks that
 * XML which points at it, through an external entity, a system id or a
 * stylesheet, does not get it. The sandbox page in the docs lists each rule
 * with the test here that guards it.
 */
class SandboxXmlUnitTests {

	private static final String SECRET = "outside-secret";
	private static final String EXTERNAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
	private static final String DBF = "javax/xml/parsers/DocumentBuilderFactory";
	private static final String DB = "javax/xml/parsers/DocumentBuilder";
	private static final String TF = "javax/xml/transform/TransformerFactory";
	private static final String XIF = "javax/xml/stream/XMLInputFactory";
	private static final String INPUT_SOURCE = "org/xml/sax/InputSource";

	@TempDir
	Path folder;

	private Path root;
	private Path outside;
	private String withExternalEntity;
	private Properties savedCoreProp;

	@BeforeEach
	void makeFiles() throws Exception {
		this.savedCoreProp = DIHelper.getInstance().getCoreProp();
		DIHelper.getInstance().setCoreProp(new Properties());
		Path base = this.folder.toRealPath();
		this.root = Files.createDirectories(base.resolve("user-root"));
		this.outside = Files.writeString(base.resolve("outside.txt"), SECRET);
		this.withExternalEntity = "<!DOCTYPE d [<!ENTITY x SYSTEM \"" + this.outside.toUri() + "\">]><d>&x;</d>";
	}

	@AfterEach
	void clear() {
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		DIHelper.getInstance().setCoreProp(this.savedCoreProp);
	}

	@Test
	void theSecretIsReachableWithoutTheSandbox() throws Exception {
		String text = DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder()
				.parse(new ByteArrayInputStream(this.withExternalEntity.getBytes(StandardCharsets.UTF_8)))
				.getDocumentElement().getTextContent();

		assumeTrue(text.contains(SECRET), "this JDK does not read external entities by default");
	}

	@Test
	void aDomParserDoesNotReadAnExternalEntity() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("run", parseWithDom(this.withExternalEntity, mv -> {
				})).bytes());

		assertFalse(String.valueOf(SandboxProbe.run(probe, "run")).contains(SECRET));
	}

	@Test
	void aSettingAProjectChangesHasNoEffect() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("run", parseWithDom(this.withExternalEntity, mv -> {
					mv.visitInsn(Opcodes.DUP);
					mv.visitLdcInsn(EXTERNAL_ENTITIES);
					mv.visitInsn(Opcodes.ICONST_1);
					mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DBF, "setFeature", "(Ljava/lang/String;Z)V", false);
					mv.visitInsn(Opcodes.DUP);
					mv.visitLdcInsn(XMLConstants.ACCESS_EXTERNAL_DTD);
					mv.visitLdcInsn("all");
					mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DBF, "setAttribute",
							"(Ljava/lang/String;Ljava/lang/Object;)V", false);
					mv.visitInsn(Opcodes.DUP);
					mv.visitLdcInsn(XMLConstants.FEATURE_SECURE_PROCESSING);
					mv.visitInsn(Opcodes.ICONST_0);
					mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DBF, "setFeature", "(Ljava/lang/String;Z)V", false);
				})).bytes());

		assertFalse(String.valueOf(SandboxProbe.run(probe, "run")).contains(SECRET));
	}

	@Test
	void aLookupThatNamesAnotherParserGetsTheJdksOwn() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitLdcInsn("org.other.parsers.DocumentBuilderFactoryImpl");
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, DBF, "newInstance",
					"(Ljava/lang/String;Ljava/lang/ClassLoader;)L" + DBF + ";", false);
		}).bytes());

		Object factory = SandboxProbe.run(probe, "run");

		assertSame(DocumentBuilderFactory.class.getModule(), factory.getClass().getModule());
	}

	@Test
	void aSaxReaderStaysHardenedWhateverIsSetOnIt() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "javax/xml/parsers/SAXParserFactory", "newInstance",
					"()Ljavax/xml/parsers/SAXParserFactory;", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "javax/xml/parsers/SAXParserFactory", "newSAXParser",
					"()Ljavax/xml/parsers/SAXParser;", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "javax/xml/parsers/SAXParser", "getXMLReader",
					"()Lorg/xml/sax/XMLReader;", false);
		}).bytes());
		StringBuilder text = new StringBuilder();
		XMLReader reader = (XMLReader) SandboxProbe.run(probe, "run");
		reader.setContentHandler(new DefaultHandler() {
			@Override
			public void characters(char[] characters, int start, int length) {
				text.append(characters, start, length);
			}
		});

		reader.setFeature(EXTERNAL_ENTITIES, true);
		reader.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "all");
		reader.parse(new InputSource(new StringReader(this.withExternalEntity)));

		assertFalse(reader.getFeature(EXTERNAL_ENTITIES));
		assertFalse(text.toString().contains(SECRET));
	}

	@Test
	void theSax1ParserUnderASaxParserIsRefused() throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, "javax/xml/parsers/SAXParser", "getParser",
				"()Lorg/xml/sax/Parser;", false);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aStylesheetCannotCallJavaOrReadOtherDocuments() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("factory",
						mv -> mv.visitMethodInsn(Opcodes.INVOKESTATIC, TF, "newInstance", "()L" + TF + ";", false))
						.method("weakened", mv -> {
							mv.visitMethodInsn(Opcodes.INVOKESTATIC, TF, "newInstance", "()L" + TF + ";", false);
							mv.visitInsn(Opcodes.DUP);
							mv.visitLdcInsn(XMLConstants.FEATURE_SECURE_PROCESSING);
							mv.visitInsn(Opcodes.ICONST_0);
							mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, TF, "setFeature", "(Ljava/lang/String;Z)V",
									false);
							mv.visitInsn(Opcodes.DUP);
							mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, TF, "newTransformer",
									"()Ljavax/xml/transform/Transformer;", false);
							mv.visitInsn(Opcodes.POP);
						}).bytes());
		String prefix = "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" "
				+ "xmlns:sys=\"http://xml.apache.org/xalan/java/java.lang.System\"><xsl:template match=\"/\"><out>";
		String suffix = "</out></xsl:template></xsl:stylesheet>";
		String callsJava = prefix + "<xsl:value-of select=\"sys:getProperty('user.home')\"/>" + suffix;
		String readsAnotherDocument = prefix + "<xsl:copy-of select=\"document('" + this.outside.toUri() + "')\"/>"
				+ suffix;

		TransformerFactory factory = (TransformerFactory) SandboxProbe.run(probe, "factory");
		TransformerFactory weakened = (TransformerFactory) SandboxProbe.run(probe, "weakened");

		assertTrue(factory.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
		assertEquals("", factory.getAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET));
		assertTrue(weakened.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
		assertThrows(Exception.class, () -> transform(factory, callsJava));
		String output;
		try {
			output = transform(factory, readsAnotherDocument);
		} catch (Exception refused) {
			output = "";
		}
		assertFalse(output.contains(SECRET), output);
	}

	@Test
	void aStreamParserDoesNotReadExternalEntities() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("weakened", mv -> {
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, XIF, "newInstance", "()L" + XIF + ";", false);
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES);
			mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/Boolean", "TRUE", "Ljava/lang/Boolean;");
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, XIF, "setProperty", "(Ljava/lang/String;Ljava/lang/Object;)V",
					false);
			mv.visitInsn(Opcodes.DUP);
			reader("<a/>").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, XIF, "createXMLStreamReader",
					"(Ljava/io/Reader;)Ljavax/xml/stream/XMLStreamReader;", false);
			mv.visitInsn(Opcodes.POP);
		}).bytes());

		XMLInputFactory factory = (XMLInputFactory) SandboxProbe.run(probe, "weakened");

		assertEquals(Boolean.FALSE, factory.getProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES));
	}

	@Test
	void aSchemaIsReadFromALocalFile() throws Throwable {
		String schemaFactory = "javax/xml/validation/SchemaFactory";
		Consumer<MethodVisitor> lookup = mv -> {
			mv.visitLdcInsn(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, schemaFactory, "newInstance",
					"(Ljava/lang/String;)L" + schemaFactory + ";", false);
		};
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("factory", lookup).method("remote", mv -> {
					lookup.accept(mv);
					mv.visitTypeInsn(Opcodes.NEW, "java/net/URL");
					mv.visitInsn(Opcodes.DUP);
					mv.visitLdcInsn("https://example.invalid/schema.xsd");
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/net/URL", "<init>", "(Ljava/lang/String;)V", false);
					mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, schemaFactory, "newSchema",
							"(Ljava/net/URL;)Ljavax/xml/validation/Schema;", false);
				}).bytes());

		SchemaFactory factory = (SchemaFactory) SandboxProbe.run(probe, "factory");

		assertTrue(factory.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "remote"));
	}

	@Test
	void anXpathOverASourceUsesAHardenedParser() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "javax/xml/xpath/XPathFactory", "newInstance",
					"()Ljavax/xml/xpath/XPathFactory;", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "javax/xml/xpath/XPathFactory", "newXPath",
					"()Ljavax/xml/xpath/XPath;", false);
			mv.visitLdcInsn("/d");
			mv.visitTypeInsn(Opcodes.NEW, INPUT_SOURCE);
			mv.visitInsn(Opcodes.DUP);
			reader(this.withExternalEntity).accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, INPUT_SOURCE, "<init>", "(Ljava/io/Reader;)V", false);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "javax/xml/xpath/XPath", "evaluate",
					"(Ljava/lang/String;Lorg/xml/sax/InputSource;)Ljava/lang/String;", true);
		}).bytes());

		assertFalse(String.valueOf(SandboxProbe.run(probe, "run")).contains(SECRET));
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://example.invalid/data.xml", "http://169.254.169.254/latest/meta-data",
			"ftp://example.invalid/data.xml", "jar:file:/tmp/some.jar!/data.xml" })
	void aDocumentNamedByANetworkAddressIsNotFetched(String address) throws Exception {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("byText", mv -> {
			builder(mv);
			mv.visitLdcInsn(address);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DB, "parse", "(Ljava/lang/String;)Lorg/w3c/dom/Document;", false);
		}).method("bySource", mv -> {
			builder(mv);
			mv.visitTypeInsn(Opcodes.NEW, INPUT_SOURCE);
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(address);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, INPUT_SOURCE, "<init>", "(Ljava/lang/String;)V", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DB, "parse", "(Lorg/xml/sax/InputSource;)Lorg/w3c/dom/Document;",
					false);
		}).bytes());

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "byText"));
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "bySource"));
	}

	@Test
	void aDocumentNamedByAPathIsTheOneInsideTheUsersFolder() throws Throwable {
		Path outsideXml = Files.writeString(this.outside.resolveSibling("data.xml"), "<d>outside</d>");
		Path insideXml = this.root.resolve(outsideXml.getRoot().relativize(outsideXml).toString());
		Files.createDirectories(insideXml.getParent());
		Files.writeString(insideXml, "<d>inside</d>");
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			builder(mv);
			mv.visitLdcInsn(outsideXml.toString());
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DB, "parse", "(Ljava/lang/String;)Lorg/w3c/dom/Document;", false);
			textContent(mv);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		assertEquals("inside", SandboxProbe.run(probe, "run"));
	}

	@Test
	void aResolverCannotPointTheParserAtAnotherAddress() throws Throwable {
		String resolverName = "custom/sandbox/OwnResolver";
		SandboxProbe.write(this.folder, resolverName,
				new SandboxProbe(resolverName, "java/lang/Object", new String[] { "org/xml/sax/EntityResolver" })
						.constructor("()V", mv -> {
							mv.visitVarInsn(Opcodes.ALOAD, 0);
							mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
						}).instanceMethod("resolveEntity",
								"(Ljava/lang/String;Ljava/lang/String;)Lorg/xml/sax/InputSource;", mv -> {
									mv.visitTypeInsn(Opcodes.NEW, INPUT_SOURCE);
									mv.visitInsn(Opcodes.DUP);
									mv.visitVarInsn(Opcodes.ALOAD, 2);
									mv.visitMethodInsn(Opcodes.INVOKESPECIAL, INPUT_SOURCE, "<init>",
											"(Ljava/lang/String;)V", false);
								})
						.bytes());
		EntityResolver resolver = (EntityResolver) SandboxProbe.loader(this.folder)
				.loadClass(resolverName.replace('/', '.')).getConstructor().newInstance();
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		InputSource resolved = resolver.resolveEntity(null, this.outside.toUri().toString());

		assertTrue(Path.of(java.net.URI.create(resolved.getSystemId())).startsWith(this.root), resolved.getSystemId());
		assertThrows(SecurityException.class, () -> resolver.resolveEntity(null, "https://example.invalid/x.dtd"));
	}

	@Test
	void aSourceThatAnswersForItsOwnAddressIsRefused() throws Exception {
		String sourceName = "custom/sandbox/OwnSource";
		SandboxProbe.write(this.folder, sourceName,
				new SandboxProbe(sourceName, INPUT_SOURCE).constructor("()V", mv -> {
					mv.visitVarInsn(Opcodes.ALOAD, 0);
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, INPUT_SOURCE, "<init>", "()V", false);
				}).instanceMethod("getSystemId", "()Ljava/lang/String;",
						mv -> mv.visitLdcInsn(this.outside.toUri().toString())).bytes());
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			builder(mv);
			mv.visitTypeInsn(Opcodes.NEW, sourceName);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, sourceName, "<init>", "()V", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DB, "parse", "(Lorg/xml/sax/InputSource;)Lorg/w3c/dom/Document;",
					false);
		}).bytes());

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	private static String transform(TransformerFactory factory, String stylesheet) throws Exception {
		StringWriter output = new StringWriter();
		factory.newTransformer(new StreamSource(new StringReader(stylesheet)))
				.transform(new StreamSource(new StringReader("<a/>")), new StreamResult(output));
		return output.toString();
	}

	/**
	 * A method body that parses XML text with a DOM parser from the standard lookup
	 * and returns the text of its root element. The factory is on the stack while
	 * {@code configure} runs.
	 */
	private static Consumer<MethodVisitor> parseWithDom(String xml, Consumer<MethodVisitor> configure) {
		return mv -> {
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, DBF, "newInstance", "()L" + DBF + ";", false);
			configure.accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DBF, "newDocumentBuilder", "()L" + DB + ";", false);
			mv.visitTypeInsn(Opcodes.NEW, "java/io/ByteArrayInputStream");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(xml);
			mv.visitFieldInsn(Opcodes.GETSTATIC, "java/nio/charset/StandardCharsets", "UTF_8",
					"Ljava/nio/charset/Charset;");
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/String", "getBytes", "(Ljava/nio/charset/Charset;)[B",
					false);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/ByteArrayInputStream", "<init>", "([B)V", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DB, "parse", "(Ljava/io/InputStream;)Lorg/w3c/dom/Document;",
					false);
			textContent(mv);
		};
	}

	private static void builder(MethodVisitor mv) {
		mv.visitMethodInsn(Opcodes.INVOKESTATIC, DBF, "newInstance", "()L" + DBF + ";", false);
		mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, DBF, "newDocumentBuilder", "()L" + DB + ";", false);
	}

	private static void textContent(MethodVisitor mv) {
		mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/w3c/dom/Document", "getDocumentElement",
				"()Lorg/w3c/dom/Element;", true);
		mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/w3c/dom/Element", "getTextContent", "()Ljava/lang/String;",
				true);
	}

	private static Consumer<MethodVisitor> reader(String text) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "java/io/StringReader");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(text);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/StringReader", "<init>", "(Ljava/lang/String;)V", false);
		};
	}
}
