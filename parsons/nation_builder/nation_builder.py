import json
import logging
import time
from typing import Any, Dict, Optional, Tuple, cast
from urllib.parse import parse_qs, urlparse

from parsons import Table
from parsons.utilities import check_env
from parsons.utilities.api_connector import APIConnector
from parsons.utilities.api_connector import OAuth2APIConnector

logger = logging.getLogger(__name__)


class NationBuilderV1:
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

    def __init__(self, slug: Optional[str] = None, access_token: Optional[str] = None) -> None:
        slug = check_env.check("NB_SLUG", slug)
        token = check_env.check("NB_ACCESS_TOKEN", access_token)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(NationBuilderV1.get_auth_headers(token))

        self.client = APIConnector(NationBuilderV1.get_uri(slug), headers=headers)

    @classmethod
    def get_uri(cls, slug: Optional[str]) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v1"

    @classmethod
    def get_auth_headers(cls, access_token: Optional[str]) -> Dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"authorization": f"Bearer {access_token}"}

    @classmethod
    def parse_next_params(cls, next_value: str) -> Tuple[str, str]:
        next_params = parse_qs(urlparse(next_value).query)

        if "__nonce" not in next_params:
            raise ValueError("__nonce param not found")

        if "__token" not in next_params:
            raise ValueError("__token param not found")

        nonce = next_params["__nonce"][0]
        token = next_params["__token"][0]

        return nonce, token

    @classmethod
    def make_next_url(cls, original_url: str, nonce: str, token: str) -> str:
        return f"{original_url}?limit=100&__nonce={nonce}&__token={token}"

    def get_people(self) -> Table:
        """
        `Returns:`
            A Table of all people stored in Nation Builder.
        """
        data = []
        original_url = "people"

        url = f"{original_url}"

        while True:
            try:
                logging.debug("sending request %s" % url)
                response = self.client.get_request(url)

                res = response.get("results", None)

                if res is None:
                    break

                logging.debug("response got %s records" % len(res))

                data.extend(res)

                if response.get("next", None):
                    nonce, token = NationBuilderV1.parse_next_params(response["next"])
                    url = NationBuilderV1.make_next_url(original_url, nonce, token)
                else:
                    break
            except Exception as error:
                logging.error("error requesting data from Nation Builder: %s" % error)

                wait_time = 30
                logging.info("waiting %d seconds before retrying" % wait_time)
                time.sleep(wait_time)

        return Table(data)

    def update_person(self, person_id: str, person: Dict[str, Any]) -> Dict[str, Any]:
        """
        This method updates a person with the provided id to have the provided data. It returns a
        full representation of the updated person.

        `Args:`
            person_id: str
                Nation Builder person id.
            data: dict
                Nation builder person object.
                For example {"email": "user@example.com", "tags": ["foo", "bar"]}
                Docs: https://nationbuilder.com/people_api
        `Returns:`
            A person object with the updated data.
        """
        if person_id is None:
            raise ValueError("person_id can't None")

        if not isinstance(person_id, str):
            raise ValueError("person_id must be a str")

        if len(person_id.strip()) == 0:
            raise ValueError("person_id can't be an empty str")

        if not isinstance(person, dict):
            raise ValueError("person must be a dict")

        url = f"people/{person_id}"
        response = self.client.put_request(url, data=json.dumps({"person": person}))
        response = cast("Dict[str, Any]", response)

        return response

    def upsert_person(self, person: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Updates a matched person or creates a new one if the person doesn't exist.

        This method attempts to match the input person resource to a person already in the
        nation. If a match is found, the matched person is updated. If a match is not found, a new
        person is created. Matches are found by including one of the following IDs in the request:

            - civicrm_id
            - county_file_id
            - dw_id
            - external_id
            - email
            - facebook_username
            - ngp_id
            - salesforce_id
            - twitter_login
            - van_id

        `Args:`
            data: dict
                Nation builder person object.
                For example {"email": "user@example.com", "tags": ["foo", "bar"]}
                Docs: https://nationbuilder.com/people_api
        `Returns:`
            A tuple of `created` and `person` object with the updated data. If the request fails
            the method will return a tuple of `False` and `None`.
        """

        _required_keys = [
            "civicrm_id",
            "county_file_id",
            "dw_id",
            "external_id",
            "email",
            "facebook_username",
            "ngp_id",
            "salesforce_id",
            "twitter_login",
            "van_id",
        ]

        if not isinstance(person, dict):
            raise ValueError("person must be a dict")

        has_required_key = any(x in person for x in _required_keys)

        if not has_required_key:
            _keys = ", ".join(_required_keys)
            raise ValueError(f"person dict must contain at least one key of {_keys}")

        url = "people/push"
        response = self.client.request(url, "PUT", data=json.dumps({"person": person}))

        self.client.validate_response(response)

        if response.status_code == 200:
            if self.client.json_check(response):
                return (False, response.json())

        if response.status_code == 201:
            if self.client.json_check(response):
                return (True, response.json())

        return (False, None)


class NationBuilderV2:
    def __init__(
        self,
        slug: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ) -> None:
        slug = check_env.check("NB_SLUG", slug)
        token = check_env.check("NB_ACCESS_TOKEN", access_token)
        refresh_token = check_env.check("NB_REFRESH_TOKEN", refresh_token)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(NationBuilderV2.get_auth_headers(token))

        self.client = OAuth2APIConnector(
            uri=NationBuilderV2.get_uri(slug),
            client_id=
        )

    @classmethod
    def get_uri(cls, slug: Optional[str]) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com"

    @classmethod
    def get_auth_headers(cls, access_token: Optional[str]) -> Dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"authorization": f"Bearer {access_token}"}


    @classmethod
    def _to_table(cls, resp) -> Table:
        return Table(
            [
                {"id": i["id"], "type": i["type"]}.update(i["attributes"])
                for i in resp.json()["data"]
            ],
        )

    def _get_next(self, resp):
        if "next" in resp.json()["links"]:
            q = urlparse(resp.json()["links"]["next"])
            resp = self.client.get_request(q.path, params=q.query)
            return resp

    @classmethod
    def _get_all(self, resp: int, limit: int) -> Table:
        data = NationBuilderV2._to_table(resp)
        while limit != 0 and len(data) < limit:
            resp = self.get_next(resp)
            if resp is None:
                break
            data.stack(NationBuilderV2._to_table(resp))
        return data

    @classmethod
    def _param_builder(cls, param_name: str, param_dict: dict) -> dict:
        """Convert param dictionary into NationBuilder's param format."""
        if not param_dict:
            return {}

        params = {}
        for key, value in param_dict.items():
            if isinstance(value, dict):  # Handling complex cases
                params.update(
                    {f"{param_name}[{key}][{operator}]": val for operator, val in value.items()}
                )
            else:  # Simple case
                params[f"{param_name}[{key}]"] = value
        return params

    @classmethod
    def _field_params(cls, resource: str, fields: str | list) -> dict:
        if not fields:
            return {}
        elif isinstance(fields, str):
            return {f"fields[{resource}]": fields}
        elif isinstance(fields, list):
            return {f"fields[{resource}]": ",".join(fields)}
        else:
            raise TypeError("fields should be str or list")

    def resource(
        self,
        data: dict = None,
        id: str | int = None,
        client = None,
        req_type: str = None,
        url: str = None,
        resource: str = None,
        filters: dict = None,
        fields: list = None,
        sort_by: dict = None,
        result_count: bool = False,
        page_size: int = 100,
        all_results: bool = False,
        results_limit: int = 0,
    ) -> Table:
        """
        Generic function to fetch data from any NationBuilder v2 API endpoint.
        :param resource: API resource (e.g., "people", "donations", "events").
        :param filters: Dictionary of filters.
        :param per_page: Number of records per page.
        :param all_pages: Whether to fetch all pages.
        """

        params = self._param_builder("filter", filters)
        params.update(self._field_params(resource=resource, fields=fields))
        if sort_by:
            params["sort"] = sort_by
        if result_count or all_results:
            params["stats[total]"] = "count"
        params["page_size"] = min(100, max(1, page_size))

        valid_req_params = {
            "GET": ("url", "params", "return_format"),
            "POST": ("url", "params", "data", "json", "success_codes"),
            "DELETE": ("url", "params", "success_codes"),
            "PUT": ("url", "params", "data", "json", "success_codes"),
            "PATCH": ("url", "params", "data", "json", "success_codes"),
        }

        req_params = {
            "url": url,
            "params": params,
            "data": data,
            "json": json_data,
            "return_format": return_format,
            "success_codes": succss_code,
        }

        resp = self.client(
            **{
                k: v
                for k, v in req_params.items()
                if v is not None and k in valid_req_params[req_type]
            }
        )

        if all_results and isinstance(client, self.client.get_request):
            return self._get_all(resp, results_limit)
        return NationBuilder._to_table(resp)

        def refresh_token_for_nation(nation_slug: str, refresh_token: str) -> dict:
            """
            Contacts the NationBuilder API to exchange a refresh token for a new set of tokens.

            Args:
                nation_slug: The slug of the NationBuilder nation.
                refresh_token: The refresh token to be exchanged.

            Returns:
                A dictionary containing the new token data from the API.

            Raises:
                Exception: If the token refresh request fails.
            """
            token_url = f"https://{nation_slug}.nationbuilder.com/oauth/token"
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }
            headers = {"Content-Type": "application/json"}

            print(f"Attempting to refresh token for nation: {nation_slug}")
            response = requests.post(token_url, json=payload, headers=headers)

            if response.status_code == 200:
                new_token_data = response.json()
                print("Successfully refreshed token.")
                return new_token_data
            else:
                print(f"Failed to refresh token. Status: {response.status_code}, Body: {response.text}")
                raise Exception("NationBuilder token refresh failed")


class NationBuilder:
    def __new__(
        cls,
        slug: Optional[str] = None,
        access_token: Optional[str] = None,
        client_id: str,
        client_secret: str,
        token_url: str,
        refresh_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        parsons_version: str = "v1",
    ):
        parsons_version = check_env.check("NB_PARSONS_VERSION", parsons_version)
        if parsons_version == "v1":
            logger.info("Consider upgrading to version 2 of the NationBuilder connector!")
            logger.info(
                "See docs for more information: https://move-coop.github.io/parsons/html/latest/nation_builder.html"
            )
            return NationBuilderV1(slug=slug, access_token=access_token)
        if parsons_version == "v2":
            return NationBuilderV2(
                slug=check_env("NB_SLUG", slug),
                access_token=check_env("NB_ACCESS_TOKEN", access_token),
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
            )
        raise ValueError(f"{parsons_version} not supported")
