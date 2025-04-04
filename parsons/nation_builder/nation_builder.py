import json
import logging
import time
from typing import Any, Dict, Optional, Tuple, cast
from urllib.parse import parse_qs, urlparse

from parsons import Table
from parsons.utilities import check_env

from .nb_connector import NBConnector
from .signups import Signups

logger = logging.getLogger(__name__)


class NationBuilder(
    Signups,
):
    """
    Instantiate the NationBuilder class

    `Args:`
        slug: str
            The Nation Builder slug Not required if ``NB_SLUG`` env variable set. The slug is the
            nation slug of the nation from which your application is requesting approval to retrieve
            data via the NationBuilder API. For example, your application's user could provide this
            slug via a text field in your application.
        access_token: str
            The Nation Builder access_token Not required if ``NB_ACCESS_TOKEN`` env variable set.
    """

    def __init__(
        self,
        slug: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        version: int = 1,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ) -> None:
        slug = check_env.check("NB_SLUG", slug)
        access_token = check_env.check("NB_ACCESS_TOKEN", access_token)
        refresh_token = check_env.check("NB_REFRESH_TOKEN", refresh_token)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(NationBuilder.get_auth_headers(access_token))

        if 0 < version < 3:
            raise ValueError("invalid version number")
        self.version = int(version)

        self.client = NBConnector(NationBuilder.get_uri(slug, version), headers=headers)

    @classmethod
    def get_uri(cls, slug: Optional[str], version: int) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v{version}"

    @classmethod
    def get_auth_headers(cls, access_token: Optional[str]) -> Dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"authorization": f"Bearer {access_token}"}

    # @classmethod
    # def parse_next_params(cls, next_value: str) -> Tuple[str, str]:
    #     next_params = parse_qs(urlparse(next_value).query)

    #     if "__nonce" not in next_params:
    #         raise ValueError("__nonce param not found")

    #     if "__token" not in next_params:
    #         raise ValueError("__token param not found")

    #     nonce = next_params["__nonce"][0]
    #     token = next_params["__token"][0]

    #     return nonce, token

    # @classmethod
    # def make_next_url(cls, original_url: str, nonce: str, token: str) -> str:
    #     return f"{original_url}?limit=100&__nonce={nonce}&__token={token}"

    # @classmethod
    # def normalize_params(cls, data):
    #     normal_list = lambda x: ",".join(x) if len(x) > 0 else None

    #     temp = {}
    #     for i in data:
    #         if type(data[i]) is dict:

    #             for key, val in data[i].items():
    #                 if type(val) is list or type(val) is tuple or type(val) is set:
    #                     temp[f"{i}[{key}]"] = normal_list(val)
    #                 else:
    #                     temp[f"{i}[{key}]"] = val

    #         elif type(data[i]) is list:
    #             temp[i] = normal_list(data[i])

    #         else:
    #             temp[i] = data[i]

    #     # return temp
    #     return {key: val for key, val in temp.items() if val is not None}

    @classmethod
    def build_filter_params(cls, filters: dict) -> dict:
        """Convert filter dictionary into NationBuilder's filter format."""
        if not filters:
            return {}

        formatted_filters = {}
        for key, value in filters.items():
            if isinstance(value, dict):  # Handling complex filters
                for operator, val in value.items():
                    formatted_filters[f"filter[{key}][{operator}]"] = val
            else:  # Simple filter
                formatted_filters[f"filter[{key}]"] = value
        return formatted_filters

    @classmethod
    def _to_table(cls, resp):
        return Table(
            [
                {"id": i["id"], "type": i["type"], **i["attributes"]}
                for i in resp.json()["data"]
            ],
        )

    def _get_next(self, resp):
        try:
            q = urlparse(resp.json()["links"]["next"])
            resp = self.client.get_request(q.path, params=q.query)
            return resp
        except:
            return None

    def _get_all(self, resp: int, limit: int) -> Table:
        data = NationBuilder._to_table(resp)
        while limit != 0 and len(data) < limit:
            resp = self.get_next(resp)
            if resp is None:
                break
            data.stack(NationBuilder._to_table(resp))
        return data

    def get_resource(
        self,
        resource: str,
        filters: dict = None,
        page_size: int = 100,
        all_pages: bool = False,
        limit: int = 0,
        include: list = None,
    ) -> Table:
        """
        Generic function to fetch data from any NationBuilder v2 API endpoint.
        :param resource: API resource (e.g., "people", "donations", "events").
        :param filters: Dictionary of filters.
        :param per_page: Number of records per page.
        :param all_pages: Whether to fetch all pages.
        """
        if not include:
            include = [
                "id",
                "type",
                "civicrm_id",
                "county_file_id",
                "datatrust_id",
                "do_not_call",
                "do_not_contact",
                "dw_id",
                "email",
                "email_opt_in",
                "external_id",
                "first_name",
                "last_name",
                "mobile_number",
                "mobile_opt_in",
                "ngp_id",
                "party",
                "pf_strat_id",
                "phone_number",
                "precinct_id",
                "salesforce_id",
                "sex",
                "state_file_id",
                "van_id",
                "import_id",
                "is_donor",
                "is_mobile_bad",
                "language",
                "custom_values",
                "updated_at",
                "created_at",
            ]
        params = NationBuilder.build_filter_params(filters)
        params["page_size"] = max(min(int(page_size), 100), 1)

        resp = self.client.get_request(resource, params)

        if all_pages:
            return self._get_all(resp, limit)
        else:
            return NationBuilder._to_table(resp)
