package prerna.reactor.playwright;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor to list all available Playwright recording files in the recordings directory.
 */
public class ListPlaywrightScriptsReactor extends AbstractReactor {

    private static final String REACTOR_DESCRIPTION =
            "Return a list of all Playwright script files (.json) in the recordings folder.";

    @Override
    public NounMetadata execute() {
        // Get the recordings folder path from ReplayFromFileReactor
        Path recordingsDir = ReplayFromFileReactor.recordingsDir;
        File dir = recordingsDir.toFile();

        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("Recordings folder does not exist: " + recordingsDir);
        }

        // Collect all JSON files
        List<String> fileNames = new ArrayList<>();
        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));

        if (files != null) {
            for (File f : files) {
                fileNames.add(f.getName());
            }
        }

        // Return as a list of strings
        return new NounMetadata(fileNames, PixelDataType.VECTOR);
    }

    @Override
    public String getReactorDescription() {
        return REACTOR_DESCRIPTION;
    }
}
