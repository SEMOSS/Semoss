from typing import List, Dict
from .semoss_models import (
    SEMOSSToolFunction,
    SEMOSSToolCall,
    SEMOSSToolResponse,
    SEMOSSImageContent,
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageType,
    SEMOSSToolType,
)


class SemossMessageBuilder:

    def build_messages(self, input_messages: List[Dict] = None):
        if input_messages is None:
            return []

        semoss_messages = []
        for message in input_messages:
            message_type = message.get("type", None)
            content = message.get("inputPrompt", None)
            param_map = message.get("paramMap", {})

            semoss_message = SEMOSSMessage(
                type=message_type, content=content, param_map=param_map
            )

            # Handle Image Content
            if "imageInfos" in message:
                image_content = self._parse_image_content(message["imageInfos"])
                semoss_message.image_content = image_content

            semoss_messages.append(semoss_message)
        return semoss_messages

    def _parse_image_content(
        self, image_infos: List[Dict[str, str]]
    ) -> List[SEMOSSImageContent]:
        semoss_image_contents = []
        for image_info in image_infos:
            folder_path = image_info.get("folderPath", None)
            file_name = image_info.get("fileName", None)
            mime_type = image_info.get("mimeType", None)
            format = image_info.get("format", None)
            file_name = image_info.get("fileName", None)
            url = image_info.get("url", None)
            data = image_info.get("data", None)

            if folder_path is not None:
                type = SEMOSSImageType.FILE_PATH
            elif url is not None:
                type = SEMOSSImageType.URL
            else:
                type = SEMOSSImageType.BASE64

            image_content = SEMOSSImageContent(
                type=type,
                data=data,
                format=format,
                mime_type=mime_type,
                file_name=file_name,
                file_path=folder_path,
            )

            semoss_image_contents.append(image_content)

        return semoss_image_contents
