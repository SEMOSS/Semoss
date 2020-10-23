package prerna.om;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PixelList implements Iterable<Pixel> {

	private static final Logger logger = LogManager.getLogger(PixelList.class);

	private AtomicInteger counter = new AtomicInteger(0);
	private List<Pixel> pixelList = new Vector<>();
	private Map<String, Integer> idToIndexHash = new ConcurrentHashMap<>();
	private Map<String, LinkedList<Pixel>> frameDependency = new ConcurrentHashMap<>();
	
	public PixelList() {
		
	}
	
	/**
	 * Add a pixel directly to the list
	 * @param p
	 */
	public void addPixel(Pixel p) {
		pixelList.add(p);
		idToIndexHash.put(p.getId(), this.pixelList.size()-1);
		syncLastPixel();
	}
	
	public void syncLastPixel() {
		// now store the frame dependency
		Pixel p = pixelList.get(pixelList.size()-1);
		Set<String> frameOutputs = p.getFrameOutputs();
		for(String frameName : frameOutputs) {
			if(frameDependency.containsKey(frameName)) {
				frameDependency.get(frameName).addLast(p);
			} else {
				LinkedList<Pixel> dll = new LinkedList<>();
				dll.addFirst(p);
				frameDependency.put(frameName, dll);
			}
		}
		// now we also want to keep track of querying / filtering
		Set<String> frameInputs = p.getFrameInputs();
		for(String frameName : frameInputs) {
			// do not double count..
			if(!frameOutputs.contains(frameName)) {
				if(frameDependency.containsKey(frameName)) {
					frameDependency.get(frameName).addLast(p);
				} else {
					logger.info("Super weird... this is a frame input for pixel but doesn't exist as a previous frame output");
					LinkedList<Pixel> dll = new LinkedList<>();
					dll.addFirst(p);
					frameDependency.put(frameName, dll);
				}
			}
		}
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
				idToIndexHash.put(uid, this.pixelList.size()-1);
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
			// modifying the order here
			// need to reset the index
			recalculateIdToIndexHash();
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
			pixelIds.add(p.getId());
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
			pixelList.get(i).setId(pixelIds.get(i));
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
			if(p.getId().equals(pixelId)) {
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
			if(pixelList.get(i).getId().equals(pixelId)) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Delete the pixel ids and propagate the deletion of frame dependencies
	 * @param pixelIds
	 */
	public List<Integer> removeIds(List<String> pixelIds, boolean propogate) {
		synchronized(this.pixelList) {
			Map<String, Integer> idToIndex = getIdToIndexHash();
			List<Integer> indices = new Vector<Integer>(pixelIds.size());
			for(String pId : pixelIds) {
				Integer index = idToIndex.get(pId);
				if(index == null) {
					// error
					// recalculate the hash
					recalculateIdToIndexHash();
					throw new IllegalArgumentException("Cannot find pixel step with id = " + pId);
				}
				indices.add(index);
			}
			
			// sort the index list from largest to smallest
			Collections.sort(indices, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
				    return o2.intValue()-o1.intValue();
				}
			});
			
			// now find all the downstream indices
			// that are affected by this deletion
			// from a frame perspective
			
			Set<Integer> downstreamIndex = new HashSet<>();
			if(propogate) {
				for(int i = 0; i < indices.size(); i++) {
					Pixel p = this.pixelList.get(indices.get(i).intValue());
					for(String frameName : p.getFrameOutputs()) {
						LinkedList<Pixel> dll = frameDependency.get(frameName);
						Pixel lastP = dll.removeLast();
						while( lastP != null ) {
							// grab the index of last P
							// we will have to delete this guy
							downstreamIndex.add(idToIndex.get(lastP.getId()));
							
							// condition on when to stop
							// either we hit the pixel p
							// or p is the creator of this frame
							if(lastP.equals(p)
									|| dll.isEmpty()) {
								lastP = null;
							} else {
								lastP = dll.removeLast();
							}
						}
						
						// remove the frame index entirely
						if(dll.isEmpty()) {
							frameDependency.remove(frameName);
						}
					}
				}
			}
			
			// now merge the lists into the set
			// so it is all distinct values
			downstreamIndex.addAll(indices);
			// and now remove all 
			// but need to remove from largest index
			// to smallest index
			List<Integer> finalList = new Vector<>();
			finalList.addAll(downstreamIndex);
			Collections.sort(finalList, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
				    return o2.intValue()-o1.intValue();
				}
			});
			
			for(Integer index : finalList) {
				Pixel p = this.pixelList.remove(index.intValue());
				logger.info("Dropping from recipe " + p);
			}
			
			// recalculate the hash
			recalculateIdToIndexHash();
			return indices;
		}
	}
	
	/**
	 * Get the id to index hash
	 * @return
	 */
	private Map<String, Integer> getIdToIndexHash() {
		if(idToIndexHash == null || idToIndexHash.isEmpty()) {
			recalculateIdToIndexHash();
		}
		return idToIndexHash;
	}
	
	/**
	 * Recalculate the index hash
	 */
	private void recalculateIdToIndexHash() {
		if(idToIndexHash == null) {
			idToIndexHash = new ConcurrentHashMap<>();
		} else {
			idToIndexHash.clear();
		}
		for(int i = 0; i < pixelList.size(); i++) {
			idToIndexHash.put(pixelList.get(i).getId(), i);
		}
	}
	
	////////////////////////////////////////////////////////////
	
	/*
	 * General methods
	 */
	
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
