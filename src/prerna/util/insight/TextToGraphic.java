/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util.insight;

import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import javax.imageio.ImageIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.util.Constants;

@Deprecated
public class TextToGraphic {

	private static final Logger classLogger = LogManager.getLogger(TextToGraphic.class);

	private static Random rand = new Random();

	public static void makeImage(String name, String imageLocation) {
		BufferedImage img = buildBufferedImage(name);
		try {
			ImageIO.write(img, "png", new File(imageLocation));
		} catch (IOException ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}
	}

	public static BufferedImage buildBufferedImage(String name) {
		String[] split = TextHelper.formatCompountText(name).split(" ");
		StringBuilder textBuilder = new StringBuilder();
		if (split.length == 1) {
			textBuilder.append(split[0].toUpperCase().charAt(0));
		} else {
			textBuilder.append(split[0].toUpperCase().charAt(0));
			textBuilder.append(split[split.length - 1].toUpperCase().charAt(0));
		}
		String text = textBuilder.toString().trim();

		int width = 125;
		int height = 105;
		BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g2d = img.createGraphics();

		try {
			Font font = new Font(Font.SANS_SERIF, Font.PLAIN, 24);
			g2d.setFont(font);

			// Set rendering hints before getting font metrics
			g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
			g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

			FontMetrics fm = g2d.getFontMetrics();

			// Fill background
			g2d.setColor(getRandomColor());
			g2d.fillRect(0, 0, width, height);
			g2d.setColor(Color.WHITE);

			// Calculate centered position
			int strWidth = (width - fm.stringWidth(text)) / 2;
			// Use ascent to position text properly relative to baseline
			int strHeight = (height - fm.getHeight()) / 2 + fm.getAscent();

			g2d.drawString(text, strWidth, strHeight);
		} finally {
			g2d.dispose();
		}

		return img;
	}

	public static Color getRandomColor() {
		String[] colors = new String[]{"#48BFA8", "#E0BF39", "#E67E22", "#4FA4DE", "#52CF87", "#EB6456", "#bdc3c7",
				"#9b59b6", "#34495e", "#F28E8E"};
		int index = rand.nextInt(colors.length);
		Color c = Color.decode(colors[index]);
		return c;
	}
}
