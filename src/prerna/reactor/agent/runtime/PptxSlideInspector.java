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
package prerna.reactor.agent.runtime;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/** Read-only, slide-local context. Inherited styles are identified, never guessed. */
final class PptxSlideInspector {

	private static final String P = "http://schemas.openxmlformats.org/presentationml/2006/main";
	private static final String A = "http://schemas.openxmlformats.org/drawingml/2006/main";
	private static final Set<String> OBJECTS = Set.of("sp", "pic", "graphicFrame", "cxnSp", "grpSp");

	static JSONObject inspect(Document document, JSONArray texts) {
		Map<Node, Integer> indexes = new IdentityHashMap<>();
		var textNodes = document.getElementsByTagNameNS(A, "t");
		for (int i = 0; i < textNodes.getLength(); i++) indexes.put(textNodes.item(i), i);
		JSONArray objects = new JSONArray();
		var elements = document.getElementsByTagNameNS(P, "*");
		for (int i = 0; i < elements.getLength(); i++) {
			Element object = (Element) elements.item(i);
			if (!OBJECTS.contains(object.getLocalName())) continue;
			Element identity = null;
			for (Node n = object.getFirstChild(); n != null; n = n.getNextSibling()) {
				if (n instanceof Element e && P.equals(e.getNamespaceURI()) && e.getLocalName().startsWith("nv")) {
					identity = child(e, P, "cNvPr");
					break;
				}
			}
			if (identity == null) continue; // Older/unsupported objects retain the legacy text inventory.
			String id = identity.getAttribute("id");
			JSONObject item = new JSONObject().put("objectId", id).put("name", identity.getAttribute("name"))
					.put("type", object.getLocalName());
			Element properties = child(object, P, "spPr");
			Element transform = child(properties, A, "xfrm");
			if (transform == null) transform = child(object, P, "xfrm");
			if (transform == null) transform = child(child(object, P, "grpSpPr"), A, "xfrm");
			JSONObject geometry = new JSONObject().put("units", "EMU").put("coordinateSpace",
					object.getParentNode() instanceof Element e && "grpSp".equals(e.getLocalName()) ? "group" : "slide");
			Element off = child(transform, A, "off"), extent = child(transform, A, "ext");
			if (off != null) geometry.put("x", off.getAttribute("x")).put("y", off.getAttribute("y"));
			if (extent != null) geometry.put("width", extent.getAttribute("cx")).put("height", extent.getAttribute("cy"));
			if (transform != null && transform.hasAttribute("rot")) geometry.put("rotation", transform.getAttribute("rot"));
			item.put("geometry", geometry).put("fill", fill(properties));
			JSONArray textIndexes = new JSONArray();
			Element body = child(object, P, "txBody");
			if (body != null) {
				var localTexts = body.getElementsByTagNameNS(A, "t");
				for (int j = 0; j < localTexts.getLength(); j++) {
					Node text = localTexts.item(j);
					Integer index = indexes.get(text);
					if (index == null) continue;
					textIndexes.put(index);
					Element run = (Element) text.getParentNode(), style = child(run, A, "rPr");
					JSONObject formatting = new JSONObject().put("fill", fill(style));
					if (style != null) {
						for (String key : new String[] { "sz", "b", "i", "u" })
							if (style.hasAttribute(key)) formatting.put(key, style.getAttribute(key));
						Element font = child(style, A, "latin");
						if (font != null) formatting.put("font", font.getAttribute("typeface"));
					}
					texts.getJSONObject(index).put("objectId", id).put("formatting", formatting);
				}
			}
			item.put("textIndexes", textIndexes).put("canSetTextColor", "sp".equals(object.getLocalName()) && !textIndexes.isEmpty());
			objects.put(item);
		}
		Element content = child(document.getDocumentElement(), P, "cSld");
		return new JSONObject().put("objects", objects).put("background", fill(child(child(content, P, "bg"), P, "bgPr")))
				.put("styleNotes", "Only explicit local styles are listed. Missing values inherit from paragraph, layout, master or theme; do not guess them. Font sz is hundredths of a point.");
	}

	private static JSONObject fill(Element parent) {
		if (parent != null) for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling()) {
			if (!(n instanceof Element e) || !A.equals(e.getNamespaceURI())) continue;
			if ("solidFill".equals(e.getLocalName())) {
				for (Node c = e.getFirstChild(); c != null; c = c.getNextSibling()) if (c instanceof Element color) {
					JSONObject result = new JSONObject().put("kind", "solid").put("colorType", color.getLocalName())
							.put("value", color.getAttribute("val"));
					if (color.hasChildNodes()) result.put("hasTransforms", true);
					return result;
				}
			}
			if (Set.of("noFill", "gradFill", "blipFill", "pattFill", "grpFill").contains(e.getLocalName()))
				return new JSONObject().put("kind", e.getLocalName());
		}
		return new JSONObject().put("kind", "inherited");
	}

	private static Element child(Element parent, String namespace, String name) {
		if (parent != null) for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling())
			if (n instanceof Element e && namespace.equals(e.getNamespaceURI()) && name.equals(e.getLocalName())) return e;
		return null;
	}
}
