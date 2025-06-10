from gaas_server_proxy import ServerProxy


class User(ServerProxy):
    def __init__(self, insight_id=None):
        super().__init__()
        self.insight_id = insight_id
        self.user_info = self._get_user_info()

    def _get_user_info(self) -> dict:
        epoc = super().get_next_epoc()

        pixel = f"GetCurrentUser();"
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=self.insight_id,
        )

        if pixelReturn is None or len(pixelReturn) == 0:
            raise Exception("Failed to get user information.")

        output = pixelReturn[0]["pixelReturn"][0]["output"]
        if "SAML" in output:
            provider = "SAML"
            user = output["SAML"]
        elif "NATIVE" in output:
            provider = "NATIVE"
            user = output["NATIVE"]
        else:
            provider = next(iter(output))
            user = output[provider]

        user_info = {
            "provider": provider,
            "email": user["email"] if "email" in user else "N/A",
            "id": user["id"] if "id" in user else "N/A",
            "name": user["name"] if "name" in user else "N/A",
            "username": user["username"] if "username" in user else "N/A",
            "userEpoch": user["userEpoch"] if "userEpoch" in user else "N/A",
            "lastLogin": user["lastLogin"] if "lastLogin" in user else "N/A",
            "isAdmin": output["isAdmin"] if "isAdmin" in output else "false",
        }

        return user_info
