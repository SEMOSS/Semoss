package prerna.util.pptx;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import prerna.util.DIHelper;
import prerna.util.pptx.PptxRenderService;
import prerna.util.pptx.PptxInspectionService;
import prerna.util.unoserver.Unoserver;

public class LiveInspectionCheck {
    private static String summary(JSONObject report) {
        return new JSONObject().put("status", report.get("status")).put("verdict", report.get("verdict"))
            .put("usage", report.opt("usage")).put("issues", report.get("issues")).put("errors", report.get("errors")).toString();
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Usage: LiveInspectionCheck <repo> <artifactDir> <visionEngineId>");
        Path repo = Path.of(args[0]).toAbsolutePath(), task = Path.of(args[1]).toAbsolutePath(), root = task.resolve("live");
        Files.createDirectories(root);
        for (String name : List.of("fixture-before.pptx", "fixture-fixed.pptx"))
            Files.copy(repo.resolve("test/prerna/util/pptx/fixtures").resolve(name), root.resolve(name), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        DIHelper.getInstance().loadCoreProp(repo.resolve("RDF_Map.prop").toString());
        Unoserver uno = new Unoserver();
        var renderer = new PptxRenderService(new PptxRenderService.Converter() {
            @Override public byte[] convert(Path source) { return uno.convert(source.toFile(), "pdf"); }
            @Override public String cacheKey() { return uno.getBaseUrl(); }
        });
        PptxInspectionService.VisionClient vision = (system, prompt, images, schema) -> {
            Path output = Files.createTempFile(task, "vision-response-", ".json");
            Path errors = Files.createTempFile(task, "vision-client-", ".log");
            ProcessBuilder builder = new ProcessBuilder(repo.resolve("py/install_config/.venv/bin/python").toString(),
                    repo.resolve("test/prerna/util/pptx/fixtures/live_vision_client.py").toString(), repo.toString())
                    .redirectOutput(output.toFile()).redirectError(errors.toFile());
            builder.environment().put("PPTX_TEST_VISION_ENGINE", args[2]);
            Process process = builder.start();
            try (var stdin = process.getOutputStream()) {
                stdin.write(new JSONObject().put("schema", schema).put("system", system).put("prompt", prompt)
                    .put("images", images.stream().map(Path::toString).toList()).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            if (!process.waitFor(150, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new IllegalStateException("Vision client timed out"); }
            if (process.exitValue() != 0) throw new IllegalStateException("Vision client failed; inspect " + errors.getFileName());
            JSONObject reply = new JSONObject(Files.readString(output));
            return new PptxInspectionService.VisionReply(reply.getString("text"), reply.optLong("inputTokens"), reply.optLong("outputTokens"));
        };
        var service = new PptxInspectionService(renderer, vision);
        String brief = "Check visible slide text for clipping and readability. Report only text that is cut off or unreadable; do not redesign otherwise readable slides.";
        JSONObject before = service.inspect(root, "fixture-before.pptx", null, brief, null, true, () -> {});
        Files.writeString(root.resolve("hardening-defect.json"), before.toString(2));
        System.out.println("DEFECT " + summary(before));
        if (!before.getString("status").equals("complete") || !before.getString("verdict").equals("needs_changes"))
            throw new AssertionError("Expected a complete review identifying the defect; inspect hardening-defect.json");
        boolean found = false;
        for (Object value : before.getJSONArray("issues")) {
            JSONObject issue = (JSONObject) value;
            if (issue.getInt("slide") != 3) throw new AssertionError("False positive on clean slide " + issue.getInt("slide"));
            found |= issue.getString("evidence").contains("All") && issue.getString("evidence").toLowerCase().contains("right");
        }
        if (!found) throw new AssertionError("Slide 3 defect was not identified");
        JSONObject fixed = service.inspect(root, "fixture-fixed.pptx", null, brief, null, true, () -> {});
        Files.writeString(root.resolve("hardening-clean.json"), fixed.toString(2));
        System.out.println("CLEAN " + summary(fixed));
        if (!fixed.getString("status").equals("complete") || !fixed.getString("verdict").equals("pass"))
            throw new AssertionError("Expected the corrected deck to pass");
        if (fixed.getJSONObject("usage").getInt("modelCalls") != 6) throw new AssertionError("Expected exactly 6 clean-deck calls");
    }
}
