import logging

from parsons.etl.table import Table
from parsons.utilities import check_env
from parsons.utilities.api_connector import APIConnector

logger = logging.getLogger(__name__)


class Qomon:
    def __init__(self, api_key: str | None = None) -> None:
        token: str = check_env.check("QOMON_API_KEY", api_key)
        headers: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(Qomon.get_auth_headers(token))
        self.client = APIConnector(
            uri="https://incoming.qomon.app/", headers=headers, data_key="data"
        )

    @staticmethod
    def get_auth_headers(api_key: str | None) -> dict[str, str]:
        if api_key is None:
            raise ValueError("access_token can't None")

        if not isinstance(api_key, str):
            raise ValueError("access_token must be an str")

        if len(api_key.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"Authorization": f"Bearer {api_key}"}

    def parser_resp(self, url: str, key: str, to_table: bool = False) -> Table:
        json_resp: dict = self.client.get_request(url=url)
        data: dict = self.client.data_parse(json_resp)[key]
        if to_table:
            return self.client.convert_to_table(data)
        return data

    def get_contact(self, id: str | int) -> Table:
        return self.parser_resp(url=f"contacts/{id}", key="contact")

    def get_form(self, id: str | int) -> Table:
        return self.parser_resp(url=f"forms/{id}", key="form")

    def get_forms_by_type(self, type: str, to_table: bool = True) -> Table:
        return self.parser_resp(url=f"forms/{type}", key="forms", to_table=to_table)

    def search(self):
        payload = {
            "data": {
                "advanced_search": {
                    "per_page": 1000,
                    "query": {
                        "$all": [],
                    },
                },
            },
        }
