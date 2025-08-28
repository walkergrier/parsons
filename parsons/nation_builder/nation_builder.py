import json
import logging
import time
from typing import Any, Dict, Optional, Tuple, cast
from urllib.parse import parse_qs, urlparse, quote_plus

from requests import Response

from parsons import Table
from parsons.utilities import check_env
from parsons.utilities.api_connector import APIConnector

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
        # slug = check_env.check("NB_SLUG", slug)
        # token = check_env.check("NB_ACCESS_TOKEN", access_token)
        # refresh_token = check_env.check("NB_REFRESH_TOKEN", refresh_token)

        self.client = APIConnector(
            NationBuilderV2.get_uri(slug),
            headers=NationBuilderV2.get_auth_headers(access_token=access_token),
            data_key="data",
        )

    @classmethod
    def get_uri(cls, slug: Optional[str]) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v2/"

    @classmethod
    def get_auth_headers(cls, access_token: Optional[str]) -> Dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "authorization": f"Bearer {access_token}",
        }
        return headers

    @classmethod
    def _to_table(cls, data) -> Table:
        result = [{"id": i["id"], "type": i["type"]} | i["attributes"] for i in data]
        return Table(result)

    @classmethod
    def _param_builder(cls, param_name: str, param_dict: dict) -> dict:
        """Convert param dictionary into NationBuilder's param format."""
        if not param_dict:
            return {}

        params = []
        for key, value in param_dict.items():
            if isinstance(value, dict):  # Handling complex cases
                params.append(
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

    def _urlparse(cls, url, params_as_dict=False) -> tuple[str, list[tuple] | dict]:
        if url.startswith("/api/v2/"):
            url = url[len("/api/v2/") :]
        url = urlparse(url)
        if params_as_dict:
            try:
                params = {i.split("=")[0]: i.split("=")[1] for i in url.query.split("&")}
            except Exception as e:
                logger.error(e)
                params = [tuple(i.split("=")) for i in url.query.split("&")]
        else:
            params = [tuple(i.split("=")) for i in url.query.split("&")]
        return url.path, params

    def _get_next(self, resp, **kwargs) -> Response | None:
        if "links" in resp and "next" in resp["links"]:
            url, params = self._urlparse(resp["links"]["next"])
            resp = self.client.get_request(url, params=params)
            return resp

    def _get_all(self, resp: int, limit: int = 0, **kwargs) -> Table:
        data = resp["data"]
        while limit <= 0 or len(data) < limit:
            resp = self._get_next(resp)
            if resp is None:
                break
            data += resp["data"]
        return self._to_table(data=data)

    #*
    #* Resource Methods

    def list_resource(
        self,
        resource,
        params: dict = None,
        page_size: int = 100,
        all_results: bool = False,
        url: str | None = None,
        limit: int = 0,
        raw_resp: bool = False,
        raw_json: bool = False,
        count: bool = False,
        **kwargs,
    ) -> Table:
        if not url:
            url = resource
        if not params:
            params = {}
        if isinstance(params, dict):
            params["page[size]"] = min(100, max(1, page_size))
            if count:
                params["stats[total]"] = "count"

        if raw_resp:
            return self.client.request(url, req_type="GET", params=params)

        resp = self.client.get_request(url, params=params)

        if all_results:
            return self._get_all(resp=resp, limit=limit)
        return self._to_table(resp["data"])

    def show_resource(
        self, resource: str, id: int | str, params: dict = None, url=None, sideload: list = None
    ) -> dict:
        id = int(id)
        if not url:
            url = f"{resource}/{id}"
        resp = self.client.get_request(url, params=params)

        if not sideload:
            return resp

        sideloaded_resources = {
            r: self.sideload_rescource(resp, r)
            for r in resp["data"]["relationships"]
            if r in sideload or sideload == "all"
        }
        resp["data"]["relationships"] = {k: v for k, v in sideloaded_resources.items() if v}
        return resp

    def sideload_rescource(self, resp, resource):
        link = resp["data"]["relationships"][resource]["links"]["related"]
        if not link:
            return None
        url, params = self._urlparse(link)
        return self.list_resource(resource, params, url, all_results=True)

    def post_resource(self, resource, params: dict, payload: dict, url=None):
        if not url:
            url = resource
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.post_request(url, params=params, json=payload)

    def delete_resource(self, resource, id: int | str, params: dict = None, url=None):
        id = int(id)
        if not url:
            url = f"{resource}/{id}"
        return self.client.delete_request(url, params=params)

    def upsert_resource(self, resource, payload, params, url=None):
        if not url:
            url = f"{resource}/push"
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    def patch_resource(self, resource, id, params, payload, url=None):
        id = int(id)
        if not url:
            url = f"{resource}/{id}"
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        payload = {"data": {"id": id, "type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    def list_relationship(
        self,
    ): ...

    #*
    #* Membership Endpoints

    def get_memberships(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("memberships", params, page_size, all_results, **kwargs)

    def show_membership(self, id: int | str, params: dict = None, **kwargs):
        return self.show_resource("membrships", id, params, **kwargs)

    def post_membership(self, payload: dict = None, params: dict = None):
        return self.post_resource("memberships", params, payload)

    def delete_membership(self, id: int | str, params: dict = None):
        return self.delete_resource("path_membership", id, params)

    def patch_membrship(self, id: int | str, payload: dict = None, params: dict = None):
        return self.patch_resource("path_membership", id, params, payload)

    #*
    #* Path Histories Endpoints

    def get_path_histories(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("path_histories", params, page_size, all_results, **kwargs)

    def show_path_history(self, id: int | str, params: dict = None, **kwargs):
        return self.show_resource("path_histories", id, params, **kwargs)

    #*
    #* Path Journey Status Changes Endpoints

    def get_path_journey_status_changes(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource(
            "path_journey_status_changes", params, page_size, all_results, **kwargs
        )

    def post_path_journey_status_change(self, payload: dict = None, params: dict = None):
        return self.post_resource("path_journey_status_changes", params, payload)

    def show_path_journey_status_change(self, id: int | str, params: dict = None, **kwargs):
        return self.show_resource("path_journey_status_changes", id, params, **kwargs)

    def delete_path_journey_status_change(self, id: int | str, params: dict = None):
        return self.delete_resource("path_journey_status_changes", id, params)

    def patch_path_journey_status_change(
        self, id: int | str, payload: dict = None, params: dict = None
    ):
        return self.patch_resource("path_journey_status_changes", id, params, payload)

    #*
    #* Path Journeys Endpoints

    def get_path_journeys(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("path_journeys", params, page_size, all_results, **kwargs)

    def post_path_journey(self, payload: dict = None, params: dict = None):
        return self.post_resource("path_journeys", params, payload)

    def show_path_journey(self, id: int | str, params: dict = None, **kwargs):
        return self.show_resource("path_journey", id, params, **kwargs)

    def patch_path_journey(self, id: int | str, payload: dict, params: dict = None):
        return self.patch_resource("path_journey", id, params, payload)

    def abandon_path_journey(
        self, id: int | str, path_journey_status_change_id: int | str, params: dict = None
    ):
        if not params:
            params = {}
        if not path_journey_status_change_id:
            params["path_journey_status_change_id"] = int(path_journey_status_change_id)
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/abandon", params=params)

    def complete_path_journey(
        self, id: int | str, path_journey_status_change_id: int | str, params: dict = None
    ):
        if not params:
            params = {}
        if not path_journey_status_change_id:
            params["path_journey_status_change_id"] = int(path_journey_status_change_id)
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/complete", params=params)

    def reactivate_path_journey(self, id: int | str, params: dict = None):
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/reactivate", params=params)

    def void_path_journey(self, id: int | str, params: dict = None):
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/void", params=params)

    #* Path Step Endpoints

    def get_path_steps(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("path_steps", params, page_size, all_results, **kwargs)

    def post_path_steps(self, payload: dict = None, params: dict = None):
        return self.post_resource("path_steps", params, payload)

    def show_path_step(self, id: int | str, params: dict = None, **kwargs):
        return self.show_resource("path_steps", id, params, **kwargs)

    def delete_path_step(self, id: int | str, params: dict = None):
        return self.delete_resource("path_steps", id, params)

    def patch_path_step(self, id: int | str, payload: dict, params: dict = None):
        return self.patch_resource("path_steps", id, params, payload)

    #* Path Endpoints

    def get_paths(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("paths", params, page_size, all_results, **kwargs)

    def post_path(self, payload: dict = None, params: dict = None):
        return self.post_resource("paths", params, payload)

    def show_path(self, id: int | str, params: dict = None, **kwargs):
        return self.show_resource("paths", id, params, **kwargs)

    def delete_path(self, id: int | str, params: dict = None):
        return self.delete_resource("paths", id, params)

    def patch_path(self, id: int | str, payload: dict, params: dict = None):
        return self.patch_resource("paths", id, params, payload)

    #* Signup Taggings Endpoints

    def get_signups_taggings(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("signups_taggings", params, page_size, all_results, **kwargs)

    def post_signup_tagging(self, signup_id: str | int, tag_id: str | int, params: dict) -> dict:
        """
        Creates a signup tagging from given data

        `Args:`
            signup_id: str | int
                The signup that was tagged.
            tag_id: str | int
                The signup that was tagged.
            params: dict
                a dict of query string arguments to be passed

        `Returns:`
            dict
        """
        payload = {"signup_id": signup_id, "tag_id": tag_id}
        return self.post_resource("signups", params, payload)

    #* Signup Tags

    #* Signup Endpoints

    def get_signups(
        self, params: dict = None, page_size: int = 100, all_results: bool = False, **kwargs
    ):
        return self.list_resource("signups", params, page_size, all_results, **kwargs)

    def post_signup(self, payload: dict, params: dict) -> dict:
        return self.post_resource("signups", params, payload)

    def patch_signup(self, payload: dict, params: dict) -> dict:
        required_keys = [
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
        has_required_key = any(x in payload for x in required_keys)
        if not has_required_key:
            keys = ", ".join(required_keys)
            raise ValueError(f"payload dict must contain at least one key of {keys}")
        return self.upsert_resource("signups", payload, params)


class NationBuilder:
    def __new__(
        cls,
        slug: Optional[str] = None,
        access_token: Optional[str] = None,
        parsons_version: str = "v1",
        # refresh_token: Optional[str] = None,
        # client_id: Optional[str] = None,
        # client_secret: Optional[str] = None,
        # redirect_uri: Optional[str] = None,
    ):
        if parsons_version == "v1":
            parsons_version = check_env.check("NB_PARSONS_VERSION", None, True)
        if parsons_version == "v1":
            logger.info("Consider upgrading to version 2 of the NationBuilder connector!")
            logger.info(
                "See docs for more information: https://move-coop.github.io/parsons/html/latest/nation_builder.html"
            )
            return NationBuilderV1(slug=slug, access_token=access_token)
        if parsons_version == "v2":
            return NationBuilderV2(
                slug=check_env.check("NB_SLUG", slug),
                access_token=check_env.check("NB_ACCESS_TOKEN", access_token),
                # refresh_token=refresh_token,
                # client_id=client_id,
                # client_secret=client_secret,
                # redirect_uri=redirect_uri,
            )
        raise ValueError(f"{parsons_version} not supported")
