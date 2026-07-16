from typing import Union, List, Dict


class LocalWordCountTokenizer:
    def encode(self, input_data: Union[str, List[Dict], Dict], **kwargs) -> List[str]:
        if isinstance(input_data, list):
            input_data = " ".join(
                str(msg["content"])
                for msg in input_data
                if isinstance(msg, dict) and "content" in msg
            )
        elif isinstance(input_data, dict) and "content" in input_data:
            input_data = input_data["content"]
        if not isinstance(input_data, str):
            raise Exception("The text input cannot be transformed into a string")
        return input_data.split()

    def count_tokens(self, input_data: Union[str, List[Dict], Dict]) -> int:
        return len(self.encode(input_data))
