package prerna.om;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class PixelList implements Iterable<Pixel> {

	private AtomicInteger counter = new AtomicInteger(0);
	private List<Pixel> pixelList = new Vector<>();
	
	public PixelList() {
		
	}
	
	/**
	 * Add a pixel step to the recipe
	 * @param pixelString
	 * @return
	 */
	public Pixel addPixel(String pixelString) {
		List<String> pixelRecipe = new Vector<>();
		pixelRecipe.add(pixelString);
		List<Pixel> pList = addPixel(pixelRecipe);
		return pList.get(0);
	}
	
	/**
	 * Add the pixel at a specific location in the recipe
	 * @param index
	 * @param pixelString
	 * @return
	 */
	public Pixel addPixel(int index, String pixelString) {
		List<String> pixelRecipe = new Vector<>();
		pixelRecipe.add(pixelString);
		List<Pixel> pList = addPixel(pixelRecipe);
		return pList.get(0);
	}

	/**
	 * Add a list of pixel steps to the recipe
	 * @param pixelRecipe
	 * @return
	 */
	public List<Pixel> addPixel(List<String> pixelRecipe) {
		synchronized(this) {
			List<Pixel> subset = new Vector<>();
			for(int i = 0; i < pixelRecipe.size(); i++) {
				int intVal = counter.getAndIncrement();
				String uid = intVal + "__" + UUID.randomUUID().toString();
				Pixel pixel = new Pixel(uid, pixelRecipe.get(i));
				this.pixelList.add(pixel);
				// return the pixel object that was added
				subset.add(pixel);
			}
			return subset;
		}
	}
	
	/**
	 * Add the pixel at a specific location in the recipe
	 * @param index
	 * @param pixelRecipe
	 * @return
	 */
	public List<Pixel> addPixel(int index, List<String> pixelRecipe) {
		synchronized(this) {
			List<Pixel> subset = new Vector<>();
			for(int i = 0; i < pixelRecipe.size(); i++) {
				int intVal = counter.getAndIncrement();
				String uid = intVal + "__" + UUID.randomUUID().toString();
				Pixel pixel = new Pixel(uid, pixelRecipe.get(i));
				this.pixelList.add(index++, pixel);
				// return the pixel object that was added
				subset.add(pixel);
			}
			return subset;
		}
	}
	
	public Pixel get(int index) {
		return pixelList.get(index);
	}
	
	public List<String> getPixelRecipe() {
		List<String> pixelRecipe = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			pixelRecipe.add(p.getPixelString());
		}
		return pixelRecipe;
	}
	
	public List<String> getPixelIds() {
		List<String> pixelIds = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			pixelIds.add(p.getUid());
		}
		return pixelIds;
	}
	
	/**
	 * Set the pixel id for each step in the recipe
	 * @param pixelIds
	 */
	public void updateAllPixelIds(List<String> pixelIds) {
		if(pixelIds.size() != this.pixelList.size()) {
			throw new IllegalArgumentException("Array size must match current recipe size");
		}
		for(int i = 0; i < pixelIds.size(); i++) {
			pixelList.get(i).setUid(pixelIds.get(i));
		}
	}

	/**
	 * Get the Pixel object if found in the list
	 * @param pixelId
	 * @return
	 */
	public Pixel getPixel(String pixelId) {
		for(int i = 0; i < pixelList.size(); i++) {
			Pixel p = pixelList.get(i);
			if(p.getUid().equals(pixelId)) {
				return p;
			}
		}
		return null;
	}
	
	/**
	 * Find the index for the pixel id
	 * @param pixelId
	 * @return
	 */
	public int findIndex(String pixelId) {
		for(int i = 0; i < pixelList.size(); i++) {
			if(pixelList.get(i).getUid().equals(pixelId)) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Wrapper method for the List<Pixel> contained in the object
	 * @return
	 */
	public boolean isEmpty() {
		return this.pixelList.isEmpty();
	}
	
	/**
	 * Wrapper method for the List<Pixel> contained in the object
	 * @return
	 */
	public void clear() {
		this.pixelList.clear();
	}
	
	/**
	 * Wrapper method for the List<Pixel> contained in the object
	 * @return
	 */
	public int size() {
		return this.pixelList.size();
	}

	@Override
	public Iterator<Pixel> iterator() {
		return this.pixelList.iterator();
	}

}
