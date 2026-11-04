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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.sl.usermodel.PictureData.PictureType;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFChart;
import org.apache.poi.xslf.usermodel.XSLFGraphicFrame;
import org.apache.poi.xslf.usermodel.XSLFPictureData;
import org.apache.poi.xslf.usermodel.XSLFPictureShape;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Reactor that opens a .pptx template and replaces named shapes with text,
 * images, or chart data supplied in a JSON payload.
 *
 * Pixel usage:
 * TemplatePPTX(template_file="<path>", payload="<json>", fileName="<name>")
 *
 * Payload format — keys are PowerPoint shape names (set via Selection Pane):
 * {
 * "slide_title": { "type": "text", "value": "Q3 Results" },
 * "company_logo": { "type": "image", "path": "/path/to/logo.png" },
 * "sales_chart": { "type": "chart",
 * "categories": ["Jan","Feb","Mar"],
 * "series": [{"name":"Revenue","values":[100,200,150]}] }
 * }
 */
public class TemplatePPTXReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(TemplatePPTXReactor.class);

    static final String PAYLOAD_KEY = "payload";

    public TemplatePPTXReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.TEMPLATE_FILE.getKey(),
                PAYLOAD_KEY,
                ReactorKeysEnum.FILE_NAME.getKey(),
                ReactorKeysEnum.FILE_PATH.getKey()
        };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
            AbstractReactor.throwUserNotExporterError();
        }

        String templatePath = keyValue.get(ReactorKeysEnum.TEMPLATE_FILE.getKey());
        if (templatePath == null || templatePath.isBlank()) {
            throw new IllegalArgumentException("'template_file' is required.");
        }
        templatePath = Utility.normalizePath(templatePath);

        String payloadJson = keyValue.get(PAYLOAD_KEY);
        Map<String, JsonObject> replacements = parsePayload(payloadJson);

        String downloadKey = UUID.randomUUID().toString();
        InsightFile insightFile = new InsightFile();
        insightFile.setFileKey(downloadKey);

        String prefixName = keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
        String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "pptx");
        String outputDir = keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
        String fileLocation;
        if (outputDir == null) {
            fileLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + exportName;
            insightFile.setDeleteOnInsightClose(true);
        } else {
            fileLocation = Utility.normalizePath(outputDir) + DIR_SEPARATOR + exportName;
            insightFile.setDeleteOnInsightClose(false);
        }
        insightFile.setFilePath(fileLocation);

        File templateFile = new File(templatePath);
        if (!templateFile.exists()) {
            throw new IllegalArgumentException("Template file not found: " + templatePath);
        }

        try (FileInputStream fis = new FileInputStream(templateFile);
                XMLSlideShow ppt = new XMLSlideShow(fis)) {

            applyReplacements(ppt, replacements);

            try (FileOutputStream fos = new FileOutputStream(fileLocation)) {
                ppt.write(fos);
            }
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IllegalStateException("Failed to process template PPTX: " + e.getMessage());
        }

        this.insight.addExportFile(downloadKey, insightFile);
        NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
                PixelOperationType.FILE_DOWNLOAD);
        retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the pptx file"));
        return retNoun;
    }

    // Parse Payload
    Map<String, JsonObject> parsePayload(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) {
            throw new IllegalArgumentException("'payload' is required and must not be empty.");
        }
        JsonObject root = JsonParser.parseString(payloadJson).getAsJsonObject();
        Map<String, JsonObject> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : root.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getAsJsonObject());
        }
        return map;
    }

    // Match shapes in the template with the payload keys
    void applyReplacements(XMLSlideShow ppt, Map<String, JsonObject> replacements) {
        Set<String> matched = new HashSet<>();
        for (XSLFSlide slide : ppt.getSlides()) {
            for (XSLFShape shape : slide.getShapes()) {
                String name = shape.getShapeName();
                if (name != null && replacements.containsKey(name)) {
                    JsonObject op = replacements.get(name);
                    String type = op.has("type") ? op.get("type").getAsString() : "";
                    try {
                        dispatch(slide, ppt, shape, type, op);
                        matched.add(name);
                    } catch (Exception e) {
                        classLogger.warn("Skipping shape '{}': {}", name, e.getMessage(), e);
                    }
                }
            }
        }
        // warning when payload keys are never matched to any shape
        for (String key : replacements.keySet()) {
            if (!matched.contains(key)) {
                classLogger.warn("No shape found in template for payload key: '{}'", key);
            }
        }
    }

    private void dispatch(XSLFSlide slide, XMLSlideShow ppt, XSLFShape shape,
            String type, JsonObject op) throws IOException {
        switch (type) {
            case "text":
                if (!(shape instanceof XSLFTextShape)) {
                    throw new ClassCastException("Shape '" + shape.getShapeName() + "' is not a text shape.");
                }
                replaceText((XSLFTextShape) shape, op.get("value").getAsString());
                break;
            case "image":
                if (!(shape instanceof XSLFPictureShape)) {
                    throw new ClassCastException("Shape '" + shape.getShapeName() + "' is not a picture shape.");
                }
                replaceImage(ppt, (XSLFPictureShape) shape, op.get("path").getAsString());
                break;
            case "chart":
                if (!(shape instanceof XSLFGraphicFrame)) {
                    throw new ClassCastException("Shape '" + shape.getShapeName() + "' is not a graphic frame.");
                }
                replaceChart((XSLFGraphicFrame) shape, op);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown type '" + type + "' for shape '" + shape.getShapeName() + "'.");
        }
    }

    // Text Replacement (preserves only first run's formatting)
    void replaceText(XSLFTextShape shape, String newValue) {
        // Work entirely on the underlying XML bean to avoid stale list snapshots.
        org.openxmlformats.schemas.drawingml.x2006.main.CTTextBody txBody = shape.getTextBody().getXmlObject();

        // Ensure at least one paragraph exists
        if (txBody.sizeOfPArray() == 0) {
            shape.setText(newValue);
            return;
        }

        // Remove all paragraphs except index 0
        for (int i = txBody.sizeOfPArray() - 1; i >= 1; i--) {
            txBody.removeP(i);
        }

        org.openxmlformats.schemas.drawingml.x2006.main.CTTextParagraph ctPara = txBody.getPArray(0);

        // Remove all runs except index 0; if none exist, add one
        if (ctPara.sizeOfRArray() == 0) {
            ctPara.addNewR().setT(newValue);
            return;
        }
        for (int i = ctPara.sizeOfRArray() - 1; i >= 1; i--) {
            ctPara.removeR(i);
        }

        // Set value on the surviving run (index 0), preserving its rPr formatting
        ctPara.getRArray(0).setT(newValue);
    }

    // Image replacement — swaps blip embed reference, preserves anchor
    void replaceImage(XMLSlideShow ppt, XSLFPictureShape shape, String imagePath) throws IOException {
        String safePath = Utility.normalizePath(imagePath);
        File imgFile = new File(safePath);
        if (!imgFile.exists()) {
            throw new java.io.FileNotFoundException("Image not found: " + safePath);
        }

        byte[] imgBytes = FileUtils.readFileToByteArray(imgFile);
        PictureType ptype = detectPictureType(imagePath);
        XSLFPictureData newData = ppt.addPicture(imgBytes, ptype);

        // Swap the blip reference in-place — anchor is unchanged to keep shape p
        // sition
        org.openxmlformats.schemas.presentationml.x2006.main.CTPicture ctPic = (org.openxmlformats.schemas.presentationml.x2006.main.CTPicture) shape
                .getXmlObject();
        org.openxmlformats.schemas.drawingml.x2006.main.CTBlip blip = ctPic.getBlipFill().getBlip();

        // Find the relationship ID for the new picture data on the slide that owns this
        // shape
        XSLFSlide ownerSlide = findOwnerSlide(ppt, shape);
        if (ownerSlide == null) {
            throw new IllegalStateException("Could not find owning slide for shape: " + shape.getShapeName());
        }
        String relId = ownerSlide.addRelation(null,
                org.apache.poi.xslf.usermodel.XSLFRelation.IMAGE_PNG,
                newData).getRelationship().getId();
        blip.setEmbed(relId);
    }

    private XSLFSlide findOwnerSlide(XMLSlideShow ppt, XSLFShape target) {
        for (XSLFSlide slide : ppt.getSlides()) {
            for (XSLFShape shape : slide.getShapes()) {
                if (shape == target) {
                    return slide;
                }
            }
        }
        return null;
    }

    PictureType detectPictureType(String path) {
        String lower = path.toLowerCase();
        if (lower.endsWith(".png"))
            return PictureType.PNG;
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg"))
            return PictureType.JPEG;
        if (lower.endsWith(".gif"))
            return PictureType.GIF;
        if (lower.endsWith(".emf"))
            return PictureType.EMF;
        if (lower.endsWith(".wmf"))
            return PictureType.WMF;
        if (lower.endsWith(".svg"))
            return PictureType.SVG;
        return PictureType.PNG;
    }

    // Chart Replacement (updates series data)
    void replaceChart(XSLFGraphicFrame frame, JsonObject op) {
        XSLFChart chart = frame.getChart();
        if (chart == null) {
            throw new IllegalStateException(
                    "Shape '" + frame.getShapeName() + "' is a graphic frame but contains no chart.");
        }

        JsonArray catArr = op.getAsJsonArray("categories");
        String[] categories = new String[catArr.size()];
        for (int i = 0; i < catArr.size(); i++) {
            categories[i] = catArr.get(i).getAsString();
        }
        XDDFDataSource<String> catSource = XDDFDataSourcesFactory.fromArray(categories);

        JsonArray seriesArr = op.getAsJsonArray("series");
        List<XDDFChartData> chartDataList = chart.getChartSeries();
        if (chartDataList.isEmpty()) {
            throw new IllegalStateException(
                    "Chart '" + frame.getShapeName() + "' has no data series to update.");
        }
        XDDFChartData chartData = chartDataList.get(0);

        for (int s = 0; s < seriesArr.size(); s++) {
            JsonObject seriesObj = seriesArr.get(s).getAsJsonObject();
            JsonArray valArr = seriesObj.getAsJsonArray("values");
            Double[] values = new Double[valArr.size()];
            for (int i = 0; i < valArr.size(); i++) {
                values[i] = valArr.get(i).getAsDouble();
            }
            XDDFNumericalDataSource<Double> valSource = XDDFDataSourcesFactory.fromArray(values);

            if (s < chartData.getSeriesCount()) {
                chartData.getSeries(s).replaceData(catSource, valSource);
            } else {
                chartData.addSeries(catSource, valSource);
            }
        }

        if (chartData.getSeriesCount() > seriesArr.size()) {
            classLogger.warn("Chart '{}' has more series than payload — extra series left unchanged.",
                    frame.getShapeName());
        }

        chart.plot(chartData);
    }
}
