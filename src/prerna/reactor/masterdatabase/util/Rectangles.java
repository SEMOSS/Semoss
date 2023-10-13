package prerna.reactor.masterdatabase.util;

import java.awt.Point;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Rectangles {

    /**The map of rectangles holding x and y coordinates*/
    private Map<String, Rectangle2D> rectanglesToDraw;

    /**
     * The main algorithm that attempts to declutter the rectangles.
     * 
     * @param rectanglesToFix the map of rectangles to declutter
     */
    public Map<String, Rectangle2D> fix(Map<String, Rectangle2D> rectanglesToFix) {
        rectanglesToDraw = new HashMap<>();

        //make copies to keep original list unaffected
        rectanglesToFix.forEach((key,rectangle) -> {
        	Rectangle2D copyRect = new Rectangle2D.Double();
            copyRect.setRect(rectangle);
            rectanglesToDraw.put(key,copyRect);
        });

        // Find the center C of the bounding box of your rectangles.
        Rectangle2D surroundRect = surroundingRect(rectanglesToDraw);
        Point center = new Point((int) surroundRect.getCenterX(), (int) surroundRect.getCenterY());
        int numIterations = 0;
        int movementFactor = 10; //ideally would be 1
        boolean hasIntersections = true;

        //keep going until there are no intersections present    
        while (hasIntersections) {
            //initialize to false within the loop.  
            hasIntersections = false;

            for(Entry<String, Rectangle2D> rectangles: rectanglesToDraw.entrySet()) {
            	Rectangle2D rectangle = rectangles.getValue();
            	// Find all the rectangles R' that overlap R.
                List<Rectangle2D> intersectingRects = findIntersections(rectangle, rectanglesToDraw);

                if (intersectingRects.size() > 0) {
                    // Define a movement vector v.
                    Point movementVector = new Point(0, 0);
                    Point centerR = new Point((int) rectangle.getCenterX(), (int) rectangle.getCenterY());

                    // For each rectangle R that overlaps another.
                    for (Rectangle2D rPrime : intersectingRects) {
                        Point centerRPrime = new Point((int) rPrime.getCenterX(), (int) rPrime.getCenterY());
                        int xTrans = (int) (centerR.getX() - centerRPrime.getX());
                        int yTrans = (int) (centerR.getY() - centerRPrime.getY());

                        // Add a vector to v proportional to the vector between the center of R and R'.
                        movementVector.translate(xTrans < 0 ? -movementFactor : movementFactor,
                                yTrans < 0 ? -movementFactor : movementFactor);
                    }

                    int xTrans = (int) (centerR.getX() - center.getX());
                    int yTrans = (int) (centerR.getY() - center.getY());

                    // Add a vector to v proportional to the vector between C and the center of R.
                    movementVector.translate(xTrans < 0 ? -movementFactor : movementFactor,
                            yTrans < 0 ? -movementFactor : movementFactor);

                    // Move R by v.
                    rectangle.setRect(rectangle.getX() + movementVector.getX(), rectangle.getY() + movementVector.getY(),
                    		rectangle.getWidth(), rectangle.getHeight());

                    // Repeat until nothing overlaps.
                    hasIntersections = true;
                }
            }

            numIterations++;
        }
        System.out.println("That took " + numIterations+ " iterations.");

        return rectanglesToDraw;
    }
    
    /**
     * Given a rectangle, finds the rectangles from the rectMap that intersect with it.
     * 
     * @param rect the rectangle to find intersections on
     * @param rectMap a map of all the rectangles in question
     */
    private List<Rectangle2D> findIntersections(Rectangle2D rect, Map<String, Rectangle2D> rectMap) {
        ArrayList<Rectangle2D> intersections = new ArrayList<Rectangle2D>();

        rectMap.forEach((key, intersectingRect) -> {
        	 if (!rect.equals(intersectingRect) && intersectingRect.intersects(rect)) {
                 intersections.add(intersectingRect);
             }
        });

        return intersections;
    }

    /**
     * Find the bounding rectangle of the list of rectangles by iterating over all
     * rectangles and finding the top left and bottom right corners
     * 
     * @param rectangles map of rectangle names and x and y coordinates
     */
    private Rectangle2D surroundingRect(Map<String, Rectangle2D> rectangles) {
        Point topLeft = new Point(Integer.MAX_VALUE, Integer.MAX_VALUE);
        Point bottomRight = new Point(Integer.MIN_VALUE, Integer.MIN_VALUE);

        rectangles.forEach((key, rectangle) -> {
        	topLeft.x = Math.min(topLeft.x, (int) rectangle.getMinX());
            topLeft.y = Math.min(topLeft.y, (int) rectangle.getMinY());
            bottomRight.x = Math.max(bottomRight.x, (int) rectangle.getMaxX());
            bottomRight.y = Math.max(bottomRight.y, (int) rectangle.getMaxY());
        });

        return new Rectangle2D.Double(topLeft.getX(), 
                                      topLeft.getY(), 
                                      bottomRight.getX() - topLeft.getX(),
                                      bottomRight.getY() - topLeft.getY());
    }
}
