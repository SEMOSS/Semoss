from typing import Dict, List, Any
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


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

    if not reference_topics:
        return []

    if not reference_topic_embeddings:
        reference_topic_embeddings = {}

    results = []

    for i, input_map in enumerate(input_maps):
        try:

            scores = {}
            input_embedding = input_map.get("embeddings", [])
            input_message = input_map.get("input", "")

            if not input_embedding:
                for topic in reference_topics:
                    scores[topic] = 0.0
            else:
                for topic in reference_topics:
                    topic_embedding = reference_topic_embeddings.get(topic, [])

                    if topic_embedding:
                        similarity = calculate_semantic_similarity(
                            input_embedding, topic_embedding, similarity_method
                        )
                        scores[topic] = similarity
                    else:
                        scores[topic] = 0.0

            if scores and max(scores.values()) > 0:
                best_topic = max(scores.keys(), key=lambda k: scores[k])
                best_score = scores[best_topic]

                if best_score >= similarity_threshold:
                    decision = best_topic
                else:
                    decision = best_topic
            else:
                decision = reference_topics[0] if reference_topics else "unknown"

            payload = {
                "message": input_message,
                "scores": scores,
                "decision": decision,
            }
            results.append(payload)

        except Exception as e:
            scores = {topic: 0.0 for topic in reference_topics}
            payload = {
                "message": input_map.get("input", ""),
                "scores": scores,
                "decision": reference_topics[0] if reference_topics else "error",
            }
            results.append(payload)

    return results


def semantic_router_fallback(
    input_maps: List[Dict[str, Any]], reference_topics: List[str]
) -> List[Dict[str, Any]]:
    """
    Fallback function for backward compatibility when no topic embeddings are provided.
    """
    return semantic_router(
        input_maps, reference_topics, reference_topic_embeddings=None
    )
