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
package prerna.collaboration;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

// One avatar option for a Digital Twin, made by the image model an admin names in RDF_Map (COLLAB_AVATAR_ENGINE_ID):
// a cartoon avatar in one of several looks, drawn with the owner's own Microsoft profile photo as a rough reference,
// or from a description when they have none. Nobody picks the model and anyone in Collaboration may use it, so there
// is no catalog permission check; the model's usage limits still apply. The call's room is never saved, so neither
// the photo nor the picture reaches a chat folder or cloud storage. The browser asks once per option, each time for
// a different look, and uploads the one the owner picks.
public final class WorkTwinAvatar {

	private static final Logger classLogger = LogManager.getLogger(WorkTwinAvatar.class);

	static final int DESCRIPTION_CHARS = 500;
	// an avatar shows at 96 px at most; this keeps the answer small on every model
	static final int OPTION_SIDE = 512;
	// as the catalog image upload: a picture larger than this is not decoded
	private static final long MAX_PIXELS = 25_000_000L;
	// the largest size Graph holds
	static final String PHOTO = "/me/photo/$value";

	// the photo goes to the model as a small copy on purpose: enough to read hair, skin tone and glasses from, too
	// coarse to copy a face from. Given the full photo, the model hands it back with a filter on it
	static final int REFERENCE_SIDE = 128;

	// it opens with the feature's name because the usage log names the call after the prompt's first words
	static final String OPENING = "Digital Twin avatar. Draw a brand-new cartoon avatar character in this style: ";
	// one per option of a run. Each names its own face, pose and background, because the same words three times
	// come back as the same picture three times, and a face made of dots cannot be traced from a photo
	static final String[] LOOKS = {
			"A chunky 3D avatar from a casual virtual-world game: an oversized round head on a small simple body, "
					+ "two dot eyes, a small curved smile and no nose detail, hair as one solid rounded shape, flat "
					+ "untextured colours. Three-quarter view with one hand waving, on a warm yellow background.",
			"A blocky toy-figure game character: the head and body are simple rounded blocks, the face is printed "
					+ "on as two dots and a smile, and the hair is a single moulded piece. Front view, on a teal "
					+ "background.",
			"A flat 2D vector cartoon: solid colour shapes with clean bold outlines, no gradients and no realistic "
					+ "shading, simple oval eyes and a line for the smile. Front view with a slight head tilt, on a "
					+ "coral background.",
			"A cel-shaded anime-style game avatar: large stylised eyes, hair in a few sharp simplified shapes, flat "
					+ "colours with hard-edged shadows. Three-quarter view, on a violet background.",
			"A collectible vinyl toy figure: a very large smooth head on a tiny body, glossy plastic surfaces and a "
					+ "minimal face with small bead eyes. Seen slightly from above, on a mint green background.",
			"A low-poly 3D game avatar: the head, hair and body are built from large flat facets with simple flat "
					+ "shading, and the face is a few simple shapes. Front view, on a sky blue background." };
	static final String FRAMING = " Show the head and shoulders only, centred, with a little empty space around the "
			+ "head so the picture can be cropped to a circle. Square picture, no text, no logo, no border. It must "
			+ "read as a cartoon at a glance, never as a photograph or a photo with a filter.";
	static final String FROM_PHOTO = " The attached picture is only a small, rough reference of the person this "
			+ "avatar stands in for. Take from it just the hair colour and general hair style, the skin tone, "
			+ "glasses if they wear them, and a beard or moustache if they have one. Everything else is new: the "
			+ "simple face of the style above instead of the person's real features, different clothes, and the "
			+ "pose and background described above. Do not retouch, trace or restyle the reference picture.";
	static final String FROM_DESCRIPTION = " The person: ";

	private WorkTwinAvatar() {
	}

	/**
	 * Whether avatars can be made on this server: an admin turned the feature on
	 * and named its image model. The page hides the feature when this is false.
	 */
	public static boolean available() {
		return enabled() && engineId() != null;
	}

	// off unless an admin turns it on, like the platform's other optional features
	private static boolean enabled() {
		return Boolean.parseBoolean(setting(Constants.COLLAB_AVATAR_GENERATION_ENABLED));
	}

	// the image model an admin set for avatars, or null when none is
	private static String engineId() {
		return setting(Constants.COLLAB_AVATAR_ENGINE_ID);
	}

	// RDF_Map first, then an environment variable of the same name: a container gets its RDF_Map from its image, so
	// the environment is where one instance is told apart from the next
	static String setting(String name) {
		String value = Utility.getDIHelperProperty(name);
		if (value == null || value.isBlank()) {
			value = System.getenv(name);
		}
		return value == null || value.isBlank() ? null : value.trim();
	}

	/**
	 * One avatar option for this person.
	 *
	 * @param description how the avatar should look; blank to use their Microsoft
	 *                    profile photo
	 * @param variant     which look to draw, counted round the looks there are, so
	 *                    three numbers in a row are three different looks; null
	 *                    for any
	 * @return {@code image}, a base64 PNG, or {@code needsDescription} true when
	 *         they have no photo and sent no description
	 */
	public static Map<String, Object> generate(User user, String description, Integer variant) {
		// refused here too: hiding the button does not stop a call
		if (!enabled()) {
			throw new IllegalArgumentException("Avatar generation is turned off on this server; an admin sets "
					+ Constants.COLLAB_AVATAR_GENERATION_ENABLED + " to true in RDF_Map.prop or the environment");
		}
		String id = engineId();
		if (id == null) {
			throw new IllegalArgumentException("Avatar generation has no image model; an admin sets "
					+ Constants.COLLAB_AVATAR_ENGINE_ID + " in RDF_Map.prop or the environment");
		}
		IModelEngine model = Utility.getModel(id);
		if (model == null) {
			throw new IllegalArgumentException("The avatar model could not be loaded; an admin checks "
					+ Constants.COLLAB_AVATAR_ENGINE_ID + " in RDF_Map.prop or the environment");
		}
		return generate(user, description, variant, BrainMailHeaderSource.current(), model);
	}

	// the words for one option: its look, how to frame it, then who it is of
	static String prompt(Integer variant, String look) {
		int at = variant == null ? ThreadLocalRandom.current().nextInt(LOOKS.length)
				: Math.floorMod(variant, LOOKS.length);
		return OPENING + LOOKS[at] + FRAMING + (look.isEmpty() ? FROM_PHOTO : FROM_DESCRIPTION + look);
	}

	static Map<String, Object> generate(User user, String description, Integer variant, BrainMailHeaderSource source,
			IModelEngine model) {
		String look = description == null ? "" : description.trim();
		if (look.length() > DESCRIPTION_CHARS) {
			throw new IllegalArgumentException(
					"Keep the description to " + DESCRIPTION_CHARS + " characters or fewer");
		}
		byte[] photo = look.isEmpty() ? photo(user, source) : null;
		if (look.isEmpty() && photo == null) {
			return Map.of("needsDescription", true);
		}

		Insight insight = new Insight();
		insight.setUser(user);
		// never saved: Room.ask would keep the photo and the picture in a chat folder and in cloud storage
		Room room = new Room();
		// the usage log files the call under the insight's id
		room.setId(insight.getInsightId());
		room.setInsight(insight);
		InputMessage.Builder message = InputMessage.builder(room).withModelType(model.getModelType())
				.withText(prompt(variant, look));
		if (photo != null) {
			message.withMediaInputs(List.of(MessageInputMedia.fromBase64("photo.png", "image/png",
					Base64.getEncoder().encodeToString(photo))));
		}
		// askRoom writes into the map it is given; a model that does not know a key ignores it
		Map<String, Object> params = new HashMap<>();
		params.put("size", "1024x1024");
		params.put("quality", "medium");
		params.put("output_format", "jpeg");
		params.put("stream", false);
		AskModelEngineResponse<?> answer = model.askRoom(message.build(), room, params);
		for (MessagePart part : answer.getParts()) {
			if (part instanceof MediaMessagePart media && media.getMediaInfo() != null) {
				byte[] option = png(decode(media.getMediaInfo().getBase64Data()), OPTION_SIDE);
				if (option != null) {
					return Map.of("image", Base64.getEncoder().encodeToString(option));
				}
			}
		}
		throw new IllegalArgumentException("The image model returned no picture. Try again, or upload your own.");
	}

	// the owner's own photo as a small PNG, the format the image clients name every upload; no Microsoft login, no
	// photo set and a failed read all count as having none
	private static byte[] photo(User user, BrainMailHeaderSource source) {
		try {
			return png(source.photo(user, PHOTO), REFERENCE_SIDE);
		} catch (Exception e) {
			classLogger.debug("No Microsoft photo to make an avatar from: {}", e.getMessage());
			return null;
		}
	}

	private static byte[] decode(String base64) {
		try {
			return base64 == null ? null : Base64.getDecoder().decode(base64);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * The picture as a PNG no larger than {@code maxSide} on its longer side, or
	 * null when the bytes are not a picture or it is too large to decode.
	 */
	static byte[] png(byte[] image, int maxSide) {
		if (image == null || image.length == 0) {
			return null;
		}
		// memory streams throughout: ImageIO's own defaults cache to temp files
		try (ImageInputStream input = new MemoryCacheImageInputStream(new ByteArrayInputStream(image))) {
			Iterator<ImageReader> readers = ImageIO.getImageReaders(input);
			if (!readers.hasNext()) {
				return null;
			}
			ImageReader reader = readers.next();
			try {
				reader.setInput(input, true, true);
				int width = reader.getWidth(0);
				int height = reader.getHeight(0);
				if (width <= 0 || height <= 0 || (long) width * height > MAX_PIXELS) {
					return null;
				}
				BufferedImage read = reader.read(0);
				double scale = Math.min(1.0, (double) maxSide / Math.max(width, height));
				BufferedImage scaled = new BufferedImage(Math.max(1, (int) Math.round(width * scale)),
						Math.max(1, (int) Math.round(height * scale)), BufferedImage.TYPE_INT_ARGB);
				Graphics2D graphics = scaled.createGraphics();
				try {
					graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
							RenderingHints.VALUE_INTERPOLATION_BICUBIC);
					graphics.drawImage(read, 0, 0, scaled.getWidth(), scaled.getHeight(), null);
				} finally {
					graphics.dispose();
				}
				ByteArrayOutputStream bytes = new ByteArrayOutputStream();
				try (ImageOutputStream output = new MemoryCacheImageOutputStream(bytes)) {
					if (!ImageIO.write(scaled, "png", output)) {
						return null;
					}
				}
				return bytes.toByteArray();
			} finally {
				reader.dispose();
			}
		} catch (IOException | RuntimeException e) {
			// a reader throws either on bytes that only look like a picture
			return null;
		}
	}
}
