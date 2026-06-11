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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DefaultImageGeneratorUtil {

	private static final Logger classLogger = LogManager.getLogger(DefaultImageGeneratorUtil.class);

	private static final String DEFAULT_IMAGE_THEME_KEY = "DEFAULT_IMAGE_THEME";
	private static final String LIGHT_THEME = "light";
	private static final String DARK_THEME = "dark";
	private static final String STOCK_ENGINES_DIR = "stock-engines";
	private static final String STOCK_ENGINES_LIGHT_DIR = "stock-engines-light";
	private static final String STOCK_ENGINES_DARK_DIR = "stock-engines-dark";
	private static final String GENERIC_IMAGE_NAME = "image";
	private static final String CONFIGURED_THEME = resolveConfiguredTheme();
	private static final Set<String> PATH_SEGMENT_IGNORE = Set.of("version", "app_root", "project_root", "images");

	private static final SecureRandom RANDOM = new SecureRandom();

	/**
	 * Selects a default stock image for the given output location and copies it
	 * there. Selection is deterministic: the seed key is derived from
	 * {@code fileLocation} via {@link #extractSeedKey(String)}, so the same path
	 * always resolves to the same image. If no stock image is available, the
	 * (uncopied) output file is returned and a copy failure is logged.
	 *
	 * @param fileLocation absolute path the chosen image should be copied to
	 * @return the output {@link File} at {@code fileLocation}
	 */
	public static File pickRandomImage(String fileLocation) {
		File outputFile = new File(fileLocation);
		File sourceFile = pickStockImage(extractSeedKey(fileLocation));
		if (sourceFile == null) {
			return outputFile;
		}

		ensureParentDirectory(outputFile);
		Path from = sourceFile.toPath();
		Path to = Paths.get(fileLocation);
		try {
			Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			classLogger.error("Failed to copy default engine image from '{}' to '{}'.", from, to, e);
		}
		return outputFile;
	}

	/**
	 * Selects a stock image using the provided seed key and returns its bytes.
	 * 
	 * @param seedKey key used for deterministic selection; null/blank picks random
	 * @return image bytes of the selected stock image
	 * @throws IOException if no stock image exists or read fails
	 */
	public static byte[] pickRandomImageBytes(String seedKey) throws IOException {
		File sourceFile = pickStockImage(seedKey);
		if (sourceFile == null) {
			String imageDir = getStockImageDir().getAbsolutePath();
			throw new IOException("No stock engine images are available in " + imageDir);
		}
		return Files.readAllBytes(sourceFile.toPath());
	}

	/**
	 * Picks a random stock image and returns its bytes.
	 * 
	 * @return image bytes of a randomly selected stock image
	 * @throws IOException if no stock image exists or read fails
	 */
	public static byte[] pickRandomImageBytes() throws IOException {
		return pickRandomImageBytes(null);
	}

	/**
	 * Resolves the stock image to use for the given seed key. Candidate images are
	 * read from the themed stock directory ({@link #getStockImageDir()}) and sorted
	 * by name for stable indexing. A null/blank seed selects an image at random;
	 * otherwise selection is deterministic via
	 * {@link #computeStableIndex(String, int)}.
	 *
	 * @param seedKey deterministic selection key; null/blank selects at random
	 * @return the selected stock image, or {@code null} if none are available
	 */
	private static File pickStockImage(String seedKey) {
		File stockDir = getStockImageDir();
		File[] stockImages = stockDir.listFiles(DefaultImageGeneratorUtil::isImageFile);
		if (stockImages == null || stockImages.length == 0) {
			classLogger.warn("No stock engine images are available at '{}'.", stockDir.getAbsolutePath());
			return null;
		}
		Arrays.sort(stockImages, Comparator.comparing(File::getName));
		if (seedKey == null || seedKey.isBlank()) {
			return stockImages[RANDOM.nextInt(stockImages.length)];
		}
		int index = computeStableIndex(seedKey, stockImages.length);
		return stockImages[index];
	}

	/**
	 * Derives a deterministic seed key from an output file path. Normally the file
	 * stem is used. Local engine cards, however, are stored as
	 * {@code .../app_root/version/image.png}; when the stem is the generic
	 * {@value #GENERIC_IMAGE_NAME} the nearest meaningful parent folder is used
	 * instead, skipping structural segments listed in {@link #PATH_SEGMENT_IGNORE}.
	 *
	 * @param fileName output file path (or name) to derive the seed from
	 * @return the seed key, or an empty string if {@code fileName} is null/blank
	 */
	private static String extractSeedKey(String fileName) {
		if (fileName == null || fileName.isBlank()) {
			return "";
		}
		Path fullPath = Paths.get(fileName).normalize();
		String leafName = fullPath.getFileName() == null ? fileName : fullPath.getFileName().toString();
		String stem = removeExtension(leafName);

		// Most local cards are stored as ".../app_root/version/image.png". In that
		// case, seed from the nearest meaningful parent folder instead of "image".
		if (GENERIC_IMAGE_NAME.equalsIgnoreCase(stem)) {
			for (Path parent = fullPath.getParent(); parent != null; parent = parent.getParent()) {
				Path segmentPath = parent.getFileName();
				if (segmentPath == null) {
					continue;
				}
				String segment = segmentPath.toString();
				if (segment.isBlank() || PATH_SEGMENT_IGNORE.contains(segment.toLowerCase())) {
					continue;
				}
				return segment;
			}
		}
		return stem;
	}

	/**
	 * Strips the trailing file extension from a file name.
	 *
	 * @param fileName file name to process
	 * @return {@code fileName} without its extension, or unchanged if it has none
	 */
	private static String removeExtension(String fileName) {
		int extensionIndex = fileName.lastIndexOf('.');
		if (extensionIndex > 0) {
			return fileName.substring(0, extensionIndex);
		}
		return fileName;
	}

	/**
	 * Maps a seed key to a stable index in {@code [0, size)}. The key is first
	 * reduced to its alias via {@link #normalizeSeedKey(String)}, then hashed with
	 * FNV-1a (64-bit) for good spread across similarly-named seeds.
	 *
	 * @param seedKey selection key
	 * @param size    number of available images (must be positive)
	 * @return a deterministic index in the range {@code [0, size)}
	 */
	private static int computeStableIndex(String seedKey, int size) {
		// FNV-1a 64-bit for better spread across similarly-named seeds.
		long hash = 0xcbf29ce484222325L;
		byte[] bytes = normalizeSeedKey(seedKey).getBytes(StandardCharsets.UTF_8);
		for (byte b : bytes) {
			hash ^= (b & 0xff);
			hash *= 0x100000001b3L;
		}
		return Math.floorMod(hash, size);
	}

	/**
	 * Engine and project identifiers are formatted as {@code <alias>__<uuid>}.
	 * Selection should be driven by the human alias rather than the random id
	 * suffix, otherwise two differently-named entities (e.g. "TestCSV" and
	 * "TestDB1") can share a stock image purely because their ids hash alike.
	 * Strips everything from the last {@code "__"} onward (the id), keeping the
	 * alias - aliases that themselves contain {@code "__"} are preserved.
	 *
	 * @param seedKey raw seed key; may be null
	 * @return alias-only seed key
	 */
	private static String normalizeSeedKey(String seedKey) {
		if (seedKey == null) {
			return "";
		}
		int separatorIndex = seedKey.lastIndexOf("__");
		if (separatorIndex > 0) {
			return seedKey.substring(0, separatorIndex);
		}
		return seedKey;
	}

	/**
	 * Resolves the directory stock images are read from. Prefers the themed
	 * directory for the configured theme ({@link #CONFIGURED_THEME}) when it
	 * contains images, otherwise falls back to the default
	 * {@value #STOCK_ENGINES_DIR} directory under {@code <baseFolder>/images}.
	 *
	 * @return the stock image directory to use
	 */
	private static File getStockImageDir() {
		String baseDirectory = Utility.getBaseFolder().replace("\\", "/");
		if (!baseDirectory.endsWith("/")) {
			baseDirectory = baseDirectory + "/";
		}
		String imageBasePath = baseDirectory + "images" + File.separator;
		String themedDirectoryName = getThemedDirectoryName(CONFIGURED_THEME);
		File themedDirectory = new File(imageBasePath + themedDirectoryName);
		if (hasImageFiles(themedDirectory)) {
			return themedDirectory;
		}
		return new File(imageBasePath + STOCK_ENGINES_DIR);
	}

	/**
	 * Determines the configured image theme. Reads
	 * {@value #DEFAULT_IMAGE_THEME_KEY} from the DIHelper properties, falling back
	 * to the environment variable of the same name. Unset or unrecognized values
	 * default to {@value #LIGHT_THEME} (a warning is logged for unrecognized ones).
	 *
	 * @return the resolved theme: {@value #LIGHT_THEME} or {@value #DARK_THEME}
	 */
	private static String resolveConfiguredTheme() {
		String configuredValue = Utility.getDIHelperProperty(DEFAULT_IMAGE_THEME_KEY);
		if (configuredValue == null || configuredValue.isBlank()) {
			configuredValue = System.getenv(DEFAULT_IMAGE_THEME_KEY);
		}
		if (configuredValue == null || configuredValue.isBlank()) {
			return LIGHT_THEME;
		}
		String normalizedTheme = configuredValue.trim().toLowerCase();
		if (LIGHT_THEME.equals(normalizedTheme) || DARK_THEME.equals(normalizedTheme)) {
			return normalizedTheme;
		}
		classLogger.warn("Unsupported '{}' value '{}'. Defaulting to '{}'.", DEFAULT_IMAGE_THEME_KEY, configuredValue,
				LIGHT_THEME);
		return LIGHT_THEME;
	}

	/**
	 * Maps a theme name to its stock image directory name.
	 *
	 * @param theme the theme (e.g. {@value #LIGHT_THEME} or {@value #DARK_THEME})
	 * @return {@value #STOCK_ENGINES_DARK_DIR} for the dark theme, otherwise
	 *         {@value #STOCK_ENGINES_LIGHT_DIR}
	 */
	private static String getThemedDirectoryName(String theme) {
		if (DARK_THEME.equals(theme)) {
			return STOCK_ENGINES_DARK_DIR;
		}
		return STOCK_ENGINES_LIGHT_DIR;
	}

	/**
	 * Checks whether the given directory exists and contains at least one image
	 * file (per {@link #isImageFile(File)}).
	 *
	 * @param directory directory to inspect; may be null
	 * @return {@code true} if it is a directory holding one or more image files
	 */
	private static boolean hasImageFiles(File directory) {
		if (directory == null || !directory.exists() || !directory.isDirectory()) {
			return false;
		}
		File[] stockImages = directory.listFiles(DefaultImageGeneratorUtil::isImageFile);
		return stockImages != null && stockImages.length > 0;
	}

	/**
	 * Ensures the parent directory of the given output file exists, creating it
	 * (and any missing ancestors) if necessary.
	 *
	 * @param outputFile file whose parent directory should exist
	 */
	private static void ensureParentDirectory(File outputFile) {
		File parentDir = outputFile.getParentFile();
		if (parentDir != null && (!parentDir.exists() || !parentDir.isDirectory())) {
			parentDir.mkdirs();
		}
	}

	/**
	 * Tests whether a file is a supported image by its extension ({@code .png},
	 * {@code .jpg}, {@code .jpeg}, {@code .webp}).
	 *
	 * @param candidate file to test; may be null
	 * @return {@code true} if {@code candidate} is a regular file with a supported
	 *         image extension
	 */
	private static boolean isImageFile(File candidate) {
		if (candidate == null || !candidate.isFile()) {
			return false;
		}
		String fileName = candidate.getName().toLowerCase();
		return fileName.endsWith(".png") || fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")
				|| fileName.endsWith(".webp");
	}

}
