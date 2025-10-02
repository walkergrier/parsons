import json
import logging
import time
from typing import Any, Dict, cast
from urllib.parse import ParseResult, parse_qs, urlparse

from requests import Response

from parsons import Table  # pyright: ignore[reportMissingImports]
from parsons.utilities import check_env  # pyright: ignore[reportMissingImports]
from parsons.utilities.api_connector import APIConnector  # pyright: ignore[reportMissingImports]

logger: logging.Logger = logging.getLogger(name=__name__)


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

    def __init__(self, slug: str | None = None, access_token: str | None = None) -> None:
        slug = check_env.check("NB_SLUG", slug)
        token = check_env.check("NB_ACCESS_TOKEN", access_token)

        headers: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(NationBuilderV1.get_auth_headers(access_token=token))

        self.client = APIConnector(NationBuilderV1.get_uri(slug=slug), headers=headers)

    @classmethod
    def get_uri(cls, slug: str | None) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v1"

    @classmethod
    def get_auth_headers(cls, access_token: str | None) -> dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"authorization": f"Bearer {access_token}"}

    @classmethod
    def parse_next_params(cls, next_value: str) -> tuple[str, str]:
        next_params = parse_qs(urlparse(next_value).query)

        if "__nonce" not in next_params:
            raise ValueError("__nonce param not found")

        if "__token" not in next_params:
            raise ValueError("__token param not found")

        nonce: str = next_params["__nonce"][0]
        token: str = next_params["__token"][0]

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

    def update_person(self, person_id: str, person: dict[str, Any]) -> dict[str, Any]:
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

        url: str = f"people/{person_id}"
        response = self.client.put_request(url, data=json.dumps({"person": person}))
        response = cast("Dict[str, Any]", response)

        return response

    def upsert_person(self, person: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
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
        slug: str | None = None,
        access_token: str | None = None,
        refresh_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        redirect_uri: str | None = None,
    ) -> None:
        # slug = check_env.check("NB_SLUG", slug)
        # token = check_env.check("NB_ACCESS_TOKEN", access_token)
        # refresh_token = check_env.check("NB_REFRESH_TOKEN", refresh_token)

        self.client = APIConnector(
            NationBuilderV2.get_uri(slug=slug),
            headers=NationBuilderV2.get_auth_headers(access_token=access_token),
            data_key="data",
        )

    @classmethod
    def get_uri(cls, slug: str | None) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v2/"

    @classmethod
    def get_auth_headers(cls, access_token: str | None) -> dict[str, str]:
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
        result: list[dict] = [
            {"id": i["id"], "type": i["type"]} | i["attributes"]
            if "attributes" in i.keys()
            else {"id": i["id"], "type": i["type"]}
            for i in data
        ]
        return Table(result)

    @classmethod
    def _param_builder(cls, param_name: str, param_dict: dict[str, Any] | Any) -> list[tuple]:
        """Convert param dictionary into NationBuilder's param format."""
        params: list = []
        if not param_dict:
            return params
        for key, value in param_dict.items():
            if isinstance(value, dict):  # Handling complex cases
                params += [
                    (f"{param_name}[{key}][{operator}]", val) for operator, val in value.items()
                ]
            else:  # Simple case
                params.append((f"{param_name}[{key}]", value))
        return params

    @staticmethod
    def _params_formatter(p: str, resource: str, fields: str | list) -> dict:
        if not fields:
            return {}
        return {f"{p}[{resource}]": fields}

    @staticmethod
    def _urlparse(url: str, params_as_dict: bool = False) -> tuple[str, list[tuple] | dict]:
        if url.startswith("/api/v2/"):
            url = url[len("/api/v2/") :]
        parsed_url: ParseResult = urlparse(url=url)
        if params_as_dict:
            try:
                params_dict: dict = {
                    i.split("=")[0]: i.split("=")[1] for i in parsed_url.query.split("&")
                }
                return parsed_url.path, params_dict
            except Exception as e:
                logger.error(e)
        params_list: list = [tuple(i.split("=")) for i in parsed_url.query.split("&")]
        return parsed_url.path, params_list

    def _get_next(self, resp, **kwargs) -> Response | dict | None:
        if "links" in resp and "next" in resp["links"]:
            url, params = self._urlparse(url=resp["links"]["next"])
            resp = self.client.get_request(url, params=params)
            return resp

    def _get_all(self, resp: dict, limit: int = 0, **kwargs) -> Table:
        data = resp["data"]
        while limit <= 0 or len(data) < limit:
            resp = self._get_next(resp=resp)  # type: ignore
            if resp is None:
                break
            data += resp["data"]
        return self._to_table(data=data)

    # * ####################################################################################### * #

    # * Resource Methods

    # * ####################################################################################### * #

    def list_resource(
        self,
        resource: str,
        filters: dict | None = None,
        params: dict | list[tuple] | None = None,
        page_size: int = 100,
        all_results: bool = False,
        url: str | None = None,
        limit: int = 0,
        raw_resp: bool = False,
        # raw_json: bool = False,
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
            params = list(params.items())

        if filters:
            params += self._param_builder(param_name="filter", param_dict=filters)

        if raw_resp:
            return self.client.request(url, req_type="GET", params=params)

        resp = self.client.get_request(url, params=params)

        if all_results:
            return self._get_all(resp=resp, limit=limit)
        return self._to_table(data=resp["data"])

    def show_resource(
        self,
        resource: str,
        id: int | str,
        params: dict | None = None,
        url: str = "",
        sideload: list[str] | str | bool = False,
        sideload_params: dict | None = None,
    ) -> dict:
        id = int(id)
        if not url:
            url = f"{resource}/{id}"
        resp = self.client.get_request(url, params=params)["data"]

        if sideload is False:
            return resp
        if isinstance(sideload, str):
            sideload = [sideload]

        if not sideload_params:
            sideload_params = {}

        sideloaded_resources: dict = {
            r: self.sideload_rescource(resp=resp, resource=r)
            for r in resp["relationships"]
            if sideload is True or r in sideload
            for r in resp["relationships"]
        }
        resp["relationships"] = {k: v for k, v in sideloaded_resources.items() if v}
        return resp

    def sideload_rescource(self, resp, resource: str) -> Table:
        link: str | None = resp["relationships"][resource]["links"]["related"]
        if not link:
            return None
        url, params = self._urlparse(url=link, params_as_dict=False)
        return self.list_resource(resource=resource, params=params, url=url, all_results=True)

    def post_resource(self, resource, params: dict | None, payload: dict | None, url: str = ""):
        if not url:
            url = resource
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.post_request(url, params=params, json=payload)

    def delete_resource(
        self,
        resource,
        id: int | str,
        params: dict | None = None,
        url: str = "",
    ):
        id = int(id)
        if not url:
            url = f"{resource}/{id}"
        return self.client.delete_request(url, params=params)

    def upsert_resource(
        self,
        resource: str,
        payload: dict,
        params: dict | list[tuple] | None,
        url: str = "",
    ):
        if not url:
            url = f"{resource}/push"
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    def patch_resource(
        self,
        resource: str,
        id: int | str,
        params: dict | list[tuple] | None,
        payload: dict | None,
        url: str = "",
    ):
        id = int(id)
        if not url:
            url = f"{resource}/{id}"
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"id": id, "type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    # * ####################################################################################### * #

    # * Automation Enrollments

    # * ####################################################################################### * #

    def get_automation_enrollments(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="automation_enrollments",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def show_automation_enrollment(
        self, id: int | str, params: dict | None = None, **kwargs
    ) -> dict:
        return self.show_resource(resource="automation_enrollments", id=id, params=params, **kwargs)

    def post_automation_enrollment(self, payload: dict | None = None, params: dict | None = None):
        return self.post_resource(resource="automation_enrollments", params=params, payload=payload)

    def delete_automation_enrollments(self, id: int | str, params: dict | None = None):
        return self.delete_resource(resource="automation_enrollments", id=id, params=params)

    # * ####################################################################################### * #

    # * Automations

    # * ####################################################################################### * #

    def get_automations(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="automations",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def show_automationt(self, id: int | str, params: dict | None = None, **kwargs) -> dict:
        return self.show_resource(resource="automations", id=id, params=params, **kwargs)

    # * ####################################################################################### * #

    # * Membership Endpoints

    # * ####################################################################################### * #

    def get_memberships(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="memberships",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def show_membership(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self.show_resource(
            resource="membrships", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_membership(self, id: int | str, params: dict | None = None):
        return self.delete_resource(resource="membership", id=id, params=params)

    def patch_membership(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self.patch_resource(resource="membership", id=id, params=params, payload=payload)

    def patch_membrship(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self.patch_resource(
            resource="path_membership", id=id, params=params, payload=payload
        )

    # * ####################################################################################### * #

    # * Path Histories Endpoints

    # * ####################################################################################### * #

    def get_path_histories(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="path_histories",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def show_path_history(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self.show_resource(
            resource="path_histories", id=id, params=params, sideload=sideload, **kwargs
        )

    # * ####################################################################################### * #

    # * Path Journey Status Changes Endpoints

    # * ####################################################################################### * #

    def get_path_journey_status_changes(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="path_journey_status_changes",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def post_path_journey_status_change(
        self, payload: dict | None = None, params: dict | None = None
    ):
        return self.post_resource(
            resource="path_journey_status_changes", params=params, payload=payload
        )

    def show_path_journey_status_change(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self.show_resource(
            resource="path_journey_status_changes",
            id=id,
            params=params,
            sideload=sideload,
            **kwargs,
        )

    def delete_path_journey_status_change(self, id: int | str, params: dict | None = None):
        return self.delete_resource(resource="path_journey_status_changes", id=id, params=params)

    def patch_path_journey_status_change(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self.patch_resource(
            resource="path_journey_status_changes", id=id, params=params, payload=payload
        )

    # * ####################################################################################### * #

    # * Path Journeys Endpoints

    # * ####################################################################################### * #

    def get_path_journeys(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="path_journeys",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def post_path_journey(self, payload: dict | None = None, params: dict | None = None):
        return self.post_resource(resource="path_journeys", params=params, payload=payload)

    def show_path_journey(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self.show_resource(
            resource="path_journey", id=id, params=params, sideload=sideload, **kwargs
        )

    def patch_path_journey(self, id: int | str, payload: dict, params: dict | None = None):
        return self.patch_resource(resource="path_journey", id=id, params=params, payload=payload)

    def abandon_path_journey(
        self, id: int | str, path_journey_status_change_id: int | str, params: dict | None = None
    ):
        if not params:
            params = {}
        if not path_journey_status_change_id:
            params["path_journey_status_change_id"] = int(path_journey_status_change_id)
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/abandon", params=params)

    def complete_path_journey(
        self, id: int | str, path_journey_status_change_id: int | str, params: dict | None = None
    ):
        if not params:
            params = {}
        if not path_journey_status_change_id:
            params["path_journey_status_change_id"] = int(path_journey_status_change_id)
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/complete", params=params)

    def reactivate_path_journey(self, id: int | str, params: dict | None = None):
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/reactivate", params=params)

    def void_path_journey(self, id: int | str, params: dict | None = None):
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/void", params=params)

    # * ####################################################################################### * #

    # * Path Step Endpoints

    # * ####################################################################################### * #

    def get_path_steps(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="path_steps",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def post_path_step(self, payload: dict | None = None, params: dict | None = None):
        return self.post_resource(resource="path_steps", params=params, payload=payload)

    def show_path_step(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self.show_resource(
            resource="path_steps", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_path_step(self, id: int | str, params: dict | None = None):
        return self.delete_resource(resource="path_steps", id=id, params=params)

    def patch_path_step(self, id: int | str, payload: dict, params: dict | None = None):
        return self.patch_resource(resource="path_steps", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Path Endpoints

    # * ####################################################################################### * #

    def get_paths(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="paths", params=params, page_size=page_size, all_results=all_results, **kwargs
        )

    def post_path(self, payload: dict | None = None, params: dict | None = None):
        return self.post_resource(resource="paths", params=params, payload=payload)

    def show_path(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self.show_resource(
            resource="paths", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_path(self, id: int | str, params: dict | None = None):
        return self.delete_resource(resource="paths", id=id, params=params)

    def patch_path(self, id: int | str, payload: dict, params: dict | None = None):
        return self.patch_resource(resource="paths", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Signup Taggings Endpoints

    # * ####################################################################################### * #

    def get_signups_taggings(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="signup_taggings",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def post_signup_tagging(
        self, signup_id: str | int, tag_id: str | int, params: dict | None = None
    ) -> dict:
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
        return self.post_resource(resource="signup_taggings", params=params, payload=payload)

    def delete_signup_tagging(self, id: int | str, params: dict | None = None):
        return self.delete_resource(resource="signup_taggings", id=id, params=params)

    # * ####################################################################################### * #

    # * Signup Tags

    # * ####################################################################################### * #

    def get_signup_tags(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="signup_tags",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def show_signup_tag(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self.show_resource(
            resource="signup_tags", id=id, params=params, sideload=sideload, **kwargs
        )

    # * ####################################################################################### * #

    # * Signup Endpoints

    # * ####################################################################################### * #

    def get_signups(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="signups",
            params=params,
            page_size=page_size,
            all_results=all_results,
            **kwargs,
        )

    def post_signup(self, payload: dict, params: dict) -> dict:
        return self.post_resource(resource="signups", params=params, payload=payload)

    def patch_signup(self, payload: dict, params: dict) -> dict:
        required_keys: list[str] = [
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
        has_required_key: bool = any(x in payload for x in required_keys)
        if not has_required_key:
            keys: str = ", ".join(required_keys)
            raise ValueError(f"payload dict must contain at least one key of {keys}")
        return self.upsert_resource(resource="signups", payload=payload, params=params)

    def show_signup(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self.show_resource(
            resource="signups", id=id, params=params, sideload=sideload, **kwargs
        )

    # * ####################################################################################### * #

    # * List Endpoints

    # * ####################################################################################### * #

    def get_lists(
        self, params: dict | None = None, page_size: int = 100, all_results: bool = False, **kwargs
    ) -> Table:
        return self.list_resource(
            resource="lists", params=params, page_size=page_size, all_results=all_results, **kwargs
        )

    def show_list(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self.show_resource(
            resource="lists", id=id, params=params, sideload=sideload, **kwargs
        )

    def add_signups_to_list(
        self,
        list_id: int | str,
        signup_ids: list[str | int] | str | int,
        params: dict | None = None,
        **kwargs,
    ):
        if not isinstance(signup_ids, list):
            signup_ids = [signup_ids]
        payload: dict[str, dict] = {
            "data": {"id": list_id, "type": "lists", "signup_ids": signup_ids}
        }
        return self.patch_resource(
            resource="lists",
            id=list_id,
            params=params,
            payload=payload,
            url=f"lists/{id}/add_signups",
            **kwargs,
        )

    def remove_signups_signups_list(
        self,
        list_id: int | str,
        signup_ids: list[str | int] | str | int,
        params: dict | None = None,
        **kwargs,
    ):
        if not isinstance(signup_ids, list):
            signup_ids = [signup_ids]
        payload: dict[str, dict] = {
            "data": {"id": list_id, "type": "lists", "signup_ids": signup_ids}
        }
        return self.patch_resource(
            resource="lists",
            id=list_id,
            params=params,
            payload=payload,
            url=f"lists/{id}/remove_signups",
            **kwargs,
        )

    def list_signups_on_list(
        self,
        id,
        params: dict | None = None,
        page_size: int = 100,
        all_results: bool = True,
        **kwargs,
    ) -> Table:
        return self.list_resource(
            resource="lists",
            params=params,
            page_size=page_size,
            all_results=all_results,
            url=f"lists/{id}/signups",
            **kwargs,
        )


class NationBuilder:
    def __new__(
        cls,
        slug: str | None = None,
        access_token: str | None = None,
        parsons_version: str = "v1",
        # refresh_token: Optional[str] = None,
        # client_id: Optional[str] = None,
        # client_secret: Optional[str] = None,
        # redirect_uri: Optional[str] = None,
    ) -> NationBuilderV1 | NationBuilderV2:
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