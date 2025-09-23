from gaas_server_proxy import ServerProxy
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputMap(BaseModel):
    input: str = Field(..., description="The input message to be processed")
    type: str = Field(..., description="The type of input (e.g., 'text')")
    embeddings: List[float] = Field(
        ..., description="The embeddings of the input message"
    )


def calculate_semantic_similarity(
    input_embedding: List[float],
    topic_embedding: List[float],
    similarity_method: str = "cosine",
) -> float:
    """
    Calculate semantic similarity between input and topic embeddings.
    """
    try:
        if not input_embedding or not topic_embedding:
            return 0.0

        # Convert to numpy arrays
        input_vec = np.array(input_embedding).reshape(1, -1)
        topic_vec = np.array(topic_embedding).reshape(1, -1)

        # Check dimension compatibility
        if input_vec.shape[1] != topic_vec.shape[1]:
            logger.warning(
                f"Dimension mismatch: input={input_vec.shape[1]}, topic={topic_vec.shape[1]}"
            )
            return 0.0

        if similarity_method == "cosine":
            # Cosine similarity
            similarity = cosine_similarity(input_vec, topic_vec)[0][0]
        elif similarity_method == "euclidean":
            # Euclidean distance (converted to similarity)
            distance = np.linalg.norm(input_vec - topic_vec)
            similarity = 1 / (1 + distance)  # Convert distance to similarity
        elif similarity_method == "dot_product":
            # Dot product similarity (normalized)
            dot_product = np.dot(input_vec.flatten(), topic_vec.flatten())
            norm_input = np.linalg.norm(input_vec)
            norm_topic = np.linalg.norm(topic_vec)
            similarity = (
                dot_product / (norm_input * norm_topic)
                if (norm_input * norm_topic) > 0
                else 0.0
            )
        else:
            # Default to cosine
            similarity = cosine_similarity(input_vec, topic_vec)[0][0]

        return float(similarity)

    except Exception as e:
        logger.error(f"Error calculating similarity: {e}")
        return 0.0


def semantic_router(
    input_maps: List[Dict[str, Any]],
    reference_topics: List[str],
    reference_topic_embeddings: Dict[str, List[float]] = None,
    similarity_threshold: float = 0.3,
    similarity_method: str = "cosine",
) -> List[Dict[str, Any]]:
    """
    Enhanced semantic router using actual model embeddings for both inputs and reference topics.

    Args:
        input_maps: List of input dictionaries with embeddings
        reference_topics: List of reference topic strings
        reference_topic_embeddings: Dictionary mapping topic names to their embeddings
        similarity_threshold: Minimum similarity score for a positive match
        similarity_method: Method for calculating similarity ('cosine', 'euclidean', 'dot_product')

    Returns:
        List of dictionaries with message, scores, and decision
    """
    logger.info(
        f"Processing {len(input_maps)} input maps against {len(reference_topics)} reference topics"
    )

    if not reference_topics:
        logger.warning("No reference topics provided")
        return []

    if not reference_topic_embeddings:
        logger.warning(
            "No reference topic embeddings provided - this will result in poor performance"
        )
        reference_topic_embeddings = {}

    results = []

    for i, input_map in enumerate(input_maps):
        try:
            logger.info(f"Processing input map {i+1}/{len(input_maps)}")

            scores = {}
            input_embedding = input_map.get("embeddings", [])
            input_message = input_map.get("input", "")

            logger.info(
                f"Input message: '{input_message[:50]}{'...' if len(input_message) > 50 else ''}'"
            )
            logger.info(
                f"Input embedding dimensions: {len(input_embedding) if input_embedding else 0}"
            )

            if not input_embedding:
                logger.warning(
                    f"No embeddings found for input: {input_message[:50]}..."
                )
                # Initialize with zero scores
                for topic in reference_topics:
                    scores[topic] = 0.0
            else:
                # Calculate similarity with each reference topic
                for topic in reference_topics:
                    topic_embedding = reference_topic_embeddings.get(topic, [])

                    if topic_embedding:
                        similarity = calculate_semantic_similarity(
                            input_embedding, topic_embedding, similarity_method
                        )
                        scores[topic] = similarity
                        logger.info(
                            f"Similarity between input and '{topic}': {similarity:.4f}"
                        )
                    else:
                        scores[topic] = 0.0
                        logger.warning(f"No embedding found for topic: {topic}")

            # Determine the best matching topic
            if scores and max(scores.values()) > 0:
                best_topic = max(scores.keys(), key=lambda k: scores[k])
                best_score = scores[best_topic]

                logger.info(f"Best topic: '{best_topic}' with score: {best_score:.4f}")

                # Apply threshold check
                if best_score >= similarity_threshold:
                    decision = best_topic
                    logger.info(
                        f"Score {best_score:.4f} above threshold {similarity_threshold}, assigning to '{best_topic}'"
                    )
                else:
                    decision = best_topic  # Still assign best match, but could be "unclassified"
                    logger.info(
                        f"Score {best_score:.4f} below threshold {similarity_threshold}, but assigning best match '{best_topic}'"
                    )
            else:
                decision = reference_topics[0] if reference_topics else "unknown"
                logger.info(f"No valid scores found, defaulting to '{decision}'")

            payload = {
                "message": input_message,
                "scores": scores,
                "decision": decision,
            }
            results.append(payload)

        except Exception as e:
            logger.error(f"Error processing input map {i}: {e}")
            # Create fallback response
            scores = {topic: 0.0 for topic in reference_topics}
            payload = {
                "message": input_map.get("input", ""),
                "scores": scores,
                "decision": reference_topics[0] if reference_topics else "error",
            }
            results.append(payload)

    logger.info(f"Completed processing. Generated {len(results)} results")
    return results


# Backward compatibility function (for when reference_topic_embeddings is not provided)
def semantic_router_fallback(
    input_maps: List[Dict[str, Any]], reference_topics: List[str]
) -> List[Dict[str, Any]]:
    """
    Fallback function for backward compatibility when no topic embeddings are provided.
    """
    logger.warning(
        "Using fallback semantic router - consider providing reference_topic_embeddings for better accuracy"
    )
    return semantic_router(
        input_maps, reference_topics, reference_topic_embeddings=None
    )
