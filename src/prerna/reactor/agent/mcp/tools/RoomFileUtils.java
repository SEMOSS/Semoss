package prerna.reactor.agent.mcp.tools;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared utilities for room file reactors.
 */
public final class RoomFileUtils {

    private RoomFileUtils() {
    }

    /**
     * Recursively collects all regular (non-hidden) file paths under
     * {@code rootDir}, skipping directories whose names start with '.'.
     *
     * @param rootDir the root directory to walk
     * @return list of absolute {@link Path}s for every non-hidden regular file
     * @throws IOException if the walk fails
     */
    public static List<Path> collectVisibleFiles(Path rootDir) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(rootDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (!dir.equals(rootDir) && dir.getFileName().toString().startsWith(".")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!file.getFileName().toString().startsWith(".")) {
                    files.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
    }
}
