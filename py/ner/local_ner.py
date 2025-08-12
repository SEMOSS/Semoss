import os
import random
import string
import json
from typing import Dict, List, Any, Optional, Tuple
from gliner import GLiNER
import torch


class LocalNER:
    def __init__(self, model_id: str):
        self.model_id = model_id
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        try:
            self.model = GLiNER.from_pretrained(self.model_id)
            print("model loaded successfully")
        except Exception as e:
            raise RuntimeError(
                f"Failed to load model '{self.model_id}'. Ensure the model is available and compatible."
            ) from e

    def _generate_mask(self, length: int = 6) -> str:
        """Generate a random mask string."""
        random_str = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=length)
        )
        return f"m_{random_str}"

    def _mask_entities(
        self, text: str, entities: List[Dict[str, Any]], mask_entities: List[str]
    ) -> Dict[str, Any]:
        """
        Mask entities in the text based on the mask_entities list.

        Args:
            text (str): Original text
            entities (List[Dict[str, Any]]): Detected entities
            mask_entities (List[str]): List of entity types to mask

        Returns:
            Dict[str, Any]: Dictionary containing masked text and mapping
        """
        entities = sorted(entities, key=lambda x: x["start"], reverse=True)

        mask_values = {}
        new_text = text

        for entity in entities:
            if entity["label"] in mask_entities:
                orig_text = entity["text"]
                start = entity["start"]
                end = entity["end"]

                if orig_text in mask_values:
                    mask_text = mask_values[orig_text]
                else:
                    mask_text = self._generate_mask()
                    mask_values[orig_text] = mask_text
                    mask_values[mask_text] = orig_text

                new_text = new_text[:start] + mask_text + new_text[end:]

        return {"masked_text": new_text, "mask_values": mask_values}

    def predict(
        self,
        text: str,
        entities: List[str],
        mask_entities: Optional[List[str]] = None,
        param_dict: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Perform named entity recognition on the input text.

        Args:
            text (str): Input text to process
            entities (List[str]): List of entity types to detect
            mask_entities (Optional[List[str]]): List of entity types to mask in the output
            param_dict (Optional[Dict[str, Any]]): Additional parameters (for future use)

        Returns:
            Dict[str, Any]: Dictionary containing:
                - output: masked text (if masking enabled) or original text
                - raw_output: list of detected entities
                - mask_values: mapping between original and masked values
                - input: original input text
                - entities: entity types searched for
        """
        if mask_entities is None:
            mask_entities = []

        # Predict entities using GLiNER
        detected_entities = self.model.predict_entities(text, entities)

        # Apply masking if requested
        masked_output = (
            self._mask_entities(text, detected_entities, mask_entities)
            if mask_entities
            else {"masked_text": text, "mask_values": {}}
        )

        return {
            "output": masked_output["masked_text"],
            "raw_output": detected_entities,
            "mask_values": masked_output["mask_values"],
            "input": text,
            "entities": entities,
        }

    def predict_entities_only(
        self, text: str, entities: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Simplified method that only returns detected entities.

        Args:
            text (str): Input text to process
            entities (List[str]): List of entity types to detect

        Returns:
            List[Dict[str, Any]]: List of detected entities
        """
        return self.model.predict_entities(text, entities)

    def unmask_text(self, masked_text: str, mask_values: Dict[str, str]) -> str:
        """
        Restore original text from masked text using mask values.

        Args:
            masked_text (str): Text with masked entities
            mask_values (Dict[str, str]): Mapping between masks and original values

        Returns:
            str: Original text with entities restored
        """
        restored_text = masked_text
        for mask, original in mask_values.items():
            if mask.startswith("m_"):  # Only replace mask tokens, not original values
                restored_text = restored_text.replace(mask, original)
        return restored_text
