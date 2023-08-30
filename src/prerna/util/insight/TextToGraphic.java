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

import prerna.algorithm.nlp.TextHelper;

@Deprecated
public class TextToGraphic {

	private static Random rand = new Random();
	
    public static void makeImage(String name, String imageLocation) {
        BufferedImage img = buildBufferedImage(name);
        try {
            ImageIO.write(img, "png", new File(imageLocation));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
    public static BufferedImage buildBufferedImage(String name) {
    	String[] split = TextHelper.formatCompountText(name).split(" ");
    	StringBuilder textBuilder = new StringBuilder();
    	if(split.length == 1) {
    		textBuilder.append(split[0].toUpperCase().charAt(0));
    	} else {
    		textBuilder.append(split[0].toUpperCase().charAt(0));
    		textBuilder.append(split[split.length-1].toUpperCase().charAt(0));
    	}
    	String text = textBuilder.toString().trim();
        /*
           Because font metrics is based on a graphics context, we need to create
           a small, temporary image so we can ascertain the width and height
           of the final image
         */
        BufferedImage img = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = img.createGraphics();
        Font font = new Font("SansSerif", Font.PLAIN, 48);
        g2d.setFont(font);
        FontMetrics fm = g2d.getFontMetrics();
        int width = 125; //fm.stringWidth(text)+20;
        int height = 105; //fm.getHeight();
        g2d.dispose();

        img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        g2d = img.createGraphics();
        g2d.setColor(getRandomColor());
        g2d.fillRect(0, 0, width, height);
        g2d.setRenderingHint(RenderingHints.KEY_ALPHA_INTERPOLATION, RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY);
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2d.setRenderingHint(RenderingHints.KEY_COLOR_RENDERING, RenderingHints.VALUE_COLOR_RENDER_QUALITY);
        g2d.setRenderingHint(RenderingHints.KEY_DITHERING, RenderingHints.VALUE_DITHER_ENABLE);
        g2d.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);
        g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
        g2d.setRenderingHint(RenderingHints.KEY_STROKE_CONTROL, RenderingHints.VALUE_STROKE_PURE);
        g2d.setFont(font);
        fm = g2d.getFontMetrics();
        g2d.setColor(Color.WHITE);
        font = new Font("SansSerif", Font.PLAIN, 24);
        fm = g2d.getFontMetrics();
        int strWidth = (int) (width - fm.stringWidth(text))/2;
        int strHeight = (int) (height - fm.getHeight())/2 + height/2;
        g2d.drawString(text, strWidth, strHeight);
        g2d.dispose();
        return img;
    }
    
    public static Color getRandomColor() {
    	String[] colors = new String[]{"#48BFA8", "#E0BF39", "#E67E22", "#4FA4DE", "#52CF87", "#EB6456", "#bdc3c7", "#9b59b6", "#34495e", "#F28E8E"};
    	int index = rand.nextInt(colors.length);
    	Color c = Color.decode(colors[index]);
    	return c;
    }
    
    /*
    public static void main(String[] args) {
    	TextToGraphic.makeImage("MovieDatabase", "C:\\workspace\\Semoss_Dev\\image.png");
    }
    */

}