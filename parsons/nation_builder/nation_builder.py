import json
import logging
import time
from typing import Any, cast
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

        self.client: APIConnector = APIConnector(
            NationBuilderV1.get_uri(slug=slug), headers=headers
        )

    @classmethod
    def get_uri(cls, slug: str | None) -> str:
        if slug is None:
            raise ValueError("slug can't be None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v1"

    @classmethod
    def get_auth_headers(cls, access_token: str | None) -> dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't be None")

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
                logging.debug(f"sending request {url}")
                response = self.client.get_request(url)

                res = response.get("results", None)

                if res is None:
                    break

                logging.debug(f"response got {len(res)} records")

                data.extend(res)

                if response.get("next", None):
                    nonce, token = NationBuilderV1.parse_next_params(response["next"])
                    url = NationBuilderV1.make_next_url(original_url, nonce, token)
                else:
                    break
            except Exception as error:
                logging.error(f"error requesting data from Nation Builder: {error}")

                wait_time = 30
                logging.info("waiting %s seconds before retrying", wait_time)
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
            raise ValueError("person_id can't be None")

        if not isinstance(person_id, str):
            raise ValueError("person_id must be a str")

        if len(person_id.strip()) == 0:
            raise ValueError("person_id can't be an empty str")

        if not isinstance(person, dict):
            raise ValueError("person must be a dict")

        url: str = f"people/{person_id}"
        response = self.client.put_request(url, data=json.dumps({"person": person}))
        response = cast("dict[str, Any]", response)

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

        if response.status_code == 200 and self.client.json_check(response):
            return (False, response.json())

        if response.status_code == 201 and self.client.json_check(response):
            return (True, response.json())

        return (False, None)


class NationBuilderV2:
    def __init__(self, slug: str, access_token: str) -> None:
        self.client: APIConnector = APIConnector(
            NationBuilderV2.get_uri(slug=slug),
            headers=NationBuilderV2.get_auth_headers(access_token=access_token),
            data_key="data",
        )

    @staticmethod
    def get_uri(slug: str) -> str:
        if slug is None:
            raise ValueError("slug can't None")
        if not isinstance(slug, str):
            raise ValueError("slug must be an str")
        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")
        return f"https://{slug}.nationbuilder.com/api/v2/"

    @staticmethod
    def get_auth_headers(access_token: str) -> dict[str, str]:
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

    @staticmethod
    def _to_table(data: dict) -> Table:
        """
        Converts a list of dictionary API resources into a Table object.
        Flattens the 'attributes' key into the main dictionary.

        Args:
            data (list[dict]): A list of resource dictionaries from the API.

        Returns:
            Table: A Table object containing the formatted data.
        """
        result: list[dict] = [
            {"id": i["id"], "type": i["type"]} | i.get("attributes", {}) for i in data
        ]
        return Table(result)

    @staticmethod
    def _param_builder(
        values: dict[str, Any] | Any,
        param_name: str,
        resource: str,
    ) -> list[tuple]:
        """
        Converts a parameter dictionary into NationBuilder's specific param format.
        Handles simple and nested structures.

        Args:
            param_name (str): The base name for the parameter (e.g., 'filter').
            param_dict (dict[str, Any]): The dictionary of parameters to format.

        Returns:
            list[tuple]: A list of (key, value) tuples formatted for the API request.
        """

        if param_name == "include":
            return [(param_name, ",".join(values) if isinstance(values, list) else values)]

        if param_name == "filter":
            params: list = []
            for key, value in values.items():  # Handling complex cases
                if isinstance(value, dict):
                    params.extend(
                        [
                            (f"{param_name}[{key}][{operator}]", val)
                            for operator, val in value.items()
                        ]
                    )
                else:  # Simple case
                    params.append((f"{param_name}[{key}]", value))
            return params

        else:
            return [
                (
                    f"{param_name}[{resource}]",
                    [",".join(map(str, values)) if isinstance(values, list) else values],
                )
            ]

    @staticmethod
    def _urlparse(url: str) -> tuple[str, list[tuple]]:
        """
        Parses a URL string to separate the path from the query parameters.

        Args:
            url (str): The full or partial URL to parse.

        Returns:
            tuple[str, list[tuple] | dict]: A tuple containing the URL path and the parsed parameters.
        """
        url = url[len("/api/v2/") :] if url.startswith("/api/v2/") else url
        parsed_url: ParseResult = urlparse(url=url)
        query: str = parsed_url.query
        if not query:
            return parsed_url.path, []
        return parsed_url.path, [tuple(p.split(sep="=", maxsplit=1)) for p in query.split(sep="&")]

    def get_next(self, resp: dict | Response) -> tuple | None:
        """
        Fetches the next page of results from a paginated API response.

        Args:
            resp (dict): The current API response dictionary which may contain a 'next' link.

        Returns:
            dict | None: The API response for the next page, or None if there is no next page.
        """
        if isinstance(resp, Response):
            resp = resp.json()
        if "links" in resp and "next" in resp["links"]:
            url, params = self._urlparse(url=resp["links"]["next"])
            return url, params
        else:
            return None, None

    def _get_all(self, resp: dict, limit: int = 0) -> Table:
        """
        Fetches all pages of results from a paginated API response, up to a specified limit.

        Args:
            resp (dict): The initial API response.
            limit (int): The maximum number of records to retrieve. If 0, all records are fetched.

        Returns:
            Table: A Table object containing all the aggregated data.
        """
        data = resp["data"]
        while limit <= 0 or len(data) < limit:
            url, params = self.get_next(resp=resp)
            if url is None:
                break
            resp = self.client.get_request(url, params=params)
            data.extend(resp["data"])
        return self._to_table(data=data)

    # * ####################################################################################### * #

    # * Resource Methods

    # * ####################################################################################### * #

    def count_resource(
        self,
        resource: str,
        filters: dict | None = None,
        url: str = "",
        params: dict | None = None,
    ) -> int:
        url = url if url else resource
        params = list(params.items()) if params else []
        if filters:
            params.extend(
                self._param_builder(values=filters, param_name="filter", resource=resource)
            )
        params.extend(
            [
                (f"fields{resource}", "id"),
                ("stats[total]", "count"),
                ("page[size]", 1),
            ]
        )
        resp = self.client.get_request(url, params=params)
        return resp["meta"]["stats"]["total"]["count"]

    def _list_resource(
        self,
        resource: str,
        filters: dict | None = None,
        fields: list | str | None = None,
        extra_fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | list[tuple] | None = None,
        all_results: bool = False,
        url: str = "",
        page_size: int = 100,
        limit: int = 0,
        raw_resp: bool = False,
        debug_params: bool = False,
        **kwargs,
    ) -> Table:
        """
        Lists records for a given resource, with options for filtering and pagination.

        Args:
            resource (str): The name of the resource to list (e.g., 'people', 'lists').
            filters (dict | None): A dictionary of filters to apply to the query.
            params (dict | list[tuple] | None): Additional query parameters.
            all_results (bool): If True, fetches all pages of results.
            url (str): A specific URL to use instead of the resource name.
            page_size (int): The number of results to return per page (max 100).
            limit (int): The maximum number of results to return when all_results is True.
            raw_resp (bool): If True, returns the raw API response dictionary.
            count (bool): If True, requests the total count of matching records.

        Returns:
            Table | dict: A Table of results, or a raw dictionary if raw_resp is True.
        """
        url = url if url else resource

        if not params:
            params = []
        if isinstance(params, dict):
            params = list(params.items())
        params.append(("page[size]", min(100, max(1, page_size))))

        param_matrix: dict = {
            "filter": filters,
            "fields": fields,
            "extra_fields": extra_fields,
            "include": include,
        }

        for param_name, values in param_matrix.items():
            if values is not None:
                params.extend(
                    self._param_builder(values=values, param_name=param_name, resource=resource)
                )

        if debug_params:
            return params

        if raw_resp:
            return self.client.request(url, req_type="GET", params=params)

        resp = self.client.get_request(url, params=params)
        if all_results:
            return self._get_all(resp=resp, limit=limit)
        return self._to_table(data=resp["data"])

    def _show_resource(
        self,
        resource: str,
        id: int | str,
        fields: list | str | None = None,
        extra_fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | list[tuple] | None = None,
        url: str = "",
        sideload: list[str] | str | bool = False,
        sideload_params: dict | None = None,
    ) -> dict:
        """
        Retrieves a single resource record by its ID.

        Args:
            resource (str): The name of the resource.
            id (int | str): The unique ID of the record.
            params (dict | None): Additional query parameters.
            url (str): A specific URL to use instead of the resource name and ID.
            sideload (list[str] | str | bool): Sideload related resources.
                                                True for all, or a list of specific relations.

        Returns:
            dict: A dictionary representing the resource.
        """
        id = int(id)
        url = url if url else f"{resource}/{id}"
        if not params:
            params = []
        if isinstance(params, dict):
            params = list(params.items())

        param_matrix: dict = {
            "fields": fields,
            "extra_fields": extra_fields,
            "include": include,
        }

        for param_name, values in param_matrix.items():
            if values is not None:
                params.extend(
                    self._param_builder(values=values, param_name=param_name, resource=resource)
                )

        resp = self.client.get_request(url, params=params)["data"]
        resp |= resp.pop("attributes")
        relationships = resp.pop("relationships")
        resp["relationships"] = relationships

        if sideload is False:
            return resp

        sideload = [sideload] if isinstance(sideload, str) else sideload
        sideload_params = sideload_params if sideload_params else {}

        sideloaded_resources: dict = {
            r: self._sideload_resource(resp=resp, resource=r)
            for r in resp["relationships"]
            if sideload is True or r in sideload
            for r in resp["relationships"]
        }
        resp["relationships"] = {k: v for k, v in sideloaded_resources.items() if v}
        return resp

    def _sideload_resource(self, resp: dict, resource: str) -> Table:
        """
        Fetches and sideloads a related resource from a relationship link.

        Args:
            resp (dict): The primary resource's response dictionary.
            resource (str): The name of the relationship to sideload.

        Returns:
            Table | None: A Table of the related resources, or None if the link is not present.
        """
        link: str | None = resp["relationships"][resource]["links"]["related"]
        if not link:
            return None
        url, params = self._urlparse(url=link)
        return self._list_resource(resource=resource, params=params, url=url, all_results=True)

    def _post_resource(
        self,
        resource: str,
        params: dict | None,
        payload: dict,
        url: str = "",
    ):
        """
        Creates a new resource record.

        Args:
            resource (str): The name of the resource to create.
            payload (dict): The attributes for the new record.
            params (dict | None): Additional query parameters.
            url (str): A specific URL to use for the POST request.

        Returns:
            dict: The API response for the creation request.
        """
        url = url if url else resource
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.post_request(url, params=params, json=payload)

    def _delete_resource(self, resource, id: int | str, params: dict | None = None, url: str = ""):
        """
        Deletes a resource record by its ID.

        Args:
            resource (str): The name of the resource.
            id (int | str): The ID of the record to delete.
            params (dict | None): Additional query parameters.
            url (str): A specific URL to use for the DELETE request.

        Returns:
            dict: The API response.
        """
        id = int(id)
        url = url if url else f"{resource}/{id}"
        return self.client.delete_request(url, params=params)

    def _upsert_resource(
        self, resource: str, payload: dict, params: dict | list[tuple] | None = None, url: str = ""
    ):
        """
        Creates or updates a resource record using the '/push' endpoint.

        Args:
            resource (str): The name of the resource.
            payload (dict): The attributes for the record to upsert.
            params (dict | list[tuple] | None): Additional query parameters.
            url (str): A specific URL to use for the request.

        Returns:
            dict: The API response.
        """
        url = url if url else f"{resource}/push"
        params = params if params else {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    def _patch_resource(
        self,
        resource: str,
        id: int | str,
        params: dict | list[tuple] | None,
        payload: dict | None,
        url: str = "",
    ):
        """
        Updates an existing resource record.

        Args:
            resource (str): The name of the resource.
            id (int | str): The ID of the record to update.
            payload (dict): The attributes to update on the record.
            params (dict | list[tuple] | None): Additional query parameters.
            url (str): A specific URL to use for the PATCH request.

        Returns:
            dict: The API response.
        """
        id = int(id)
        url = url if url else f"{resource}/{id}"
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"id": id, "type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    # * ####################################################################################### * #

    # * Automation Enrollments

    # * ####################################################################################### * #

    def get_automation_enrollments(
        self,
        fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        """
        Lists all automation enrollments
        """
        return self._list_resource(
            resource="automation_enrollments",
            fields=fields,
            include=include,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_automation_enrollment(self, payload: dict, params: dict | None = None):
        """
        Creates an automation enrollment from given data
        """
        return self._post_resource(
            resource="automation_enrollments", params=params, payload=payload
        )

    def show_automation_enrollment(
        self, id: int | str, params: dict | None = None, **kwargs
    ) -> dict:
        """
        Show automation enrollments with provided ID
        """
        return self._show_resource(
            resource="automation_enrollments", id=id, params=params, **kwargs
        )

    def delete_automation_enrollments(self, id: int | str, params: dict | None = None):
        """
        Delete automation enrollments with provided ID
        """
        return self._delete_resource(resource="automation_enrollments", id=id, params=params)

    # * ####################################################################################### * #

    # * Automations

    # * ####################################################################################### * #

    def get_automations(
        self,
        fields: list | str | None = None,
        extra_fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        """
        Lists all automations
        """
        return self._list_resource(
            resource="automations",
            fields=fields,
            extra_fields=extra_fields,
            include=include,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def show_automationt(self, id: int | str, params: dict | None = None, **kwargs) -> dict:
        """
        Show automation with provided ID
        """
        return self._show_resource(resource="automations", id=id, params=params, **kwargs)

    # * ####################################################################################### * #

    # * Contacts

    # * ####################################################################################### * #

    def get_contacts(
        self,
        filters: dict | None = None,
        fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        """
        Lists all contacts
        """
        return self._list_resource(
            resource="contacts",
            filters=filters,
            fields=fields,
            include=include,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_contact(self, payload: dict, params: dict | None = None):
        """
        Creates a contact from given data
        """
        return self._post_resource(resource="contacts", params=params, payload=payload)

    def show_contact(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        """
        Show contact with provided ID
        """
        return self._show_resource(
            resource="contacts", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_contact(self, id: int | str, params: dict | None = None):
        """
        Show contact with provided ID
        """
        return self._delete_resource(resource="contacts", id=id, params=params)

    def update_contact(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        """
        Updates an existing contact
        """
        return self._patch_resource(resource="contacts", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Donation Tracking Codes

    # * ####################################################################################### * #

    def get_donation_tracking_codes(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        """
        Lists all donation tracking codes
        """
        return self._list_resource(
            resource="donation_tracking_codes",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_donation_tracking_code(self, payload: dict, params: dict | None = None):
        """
        Creates a donation tracking code from given data
        """
        return self._post_resource(
            resource="donation_tracking_codes", params=params, payload=payload
        )

    def show_donation_tracking_code(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        """
        Show donation tracking code with provided ID
        """
        return self._show_resource(
            resource="donation_tracking_codes", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_donation_tracking_code(self, id: int | str, params: dict | None = None):
        """
        Delete donation tracking code with provided ID
        """
        return self._delete_resource(resource="donation_tracking_codes", id=id, params=params)

    def update_donation_tracking_code(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        """
        Updates an existing donation tracking code
        """
        return self._patch_resource(
            resource="donation_tracking_codes", id=id, params=params, payload=payload
        )

    # * ####################################################################################### * #

    # * Donations

    # * ####################################################################################### * #

    def get_donations(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        """
        Lists all donations
        """
        return self._list_resource(
            resource="donations",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_donation(self, payload: dict, params: dict | None = None):
        """
        Creates a donation from given data
        """
        return self._post_resource(resource="donations", params=params, payload=payload)

    def show_donation(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        """
        Show donation with provided ID
        """
        return self._show_resource(
            resource="donations", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_donation(self, id: int | str, params: dict | None = None):
        """
        Delete donation with provided ID
        """
        return self._delete_resource(resource="donations", id=id, params=params)

    def update_donation(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        """
        Updates an existing donation
        """
        return self._patch_resource(resource="donations", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Event RSVPs

    # * ####################################################################################### * #

    def get_event_rsvps(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="event_rsvps",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_event_rsvp(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="event_rsvps", params=params, payload=payload)

    def show_event_rsvp(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self._show_resource(
            resource="event_rsvps", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_event_rsvp(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="event_rsvps", id=id, params=params)

    def update_event_rsvps(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self._patch_resource(resource="event_rsvps", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Events

    # * ####################################################################################### * #

    def get_events(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="events",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_event(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="events", params=params, payload=payload)

    def show_event(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self._show_resource(
            resource="events", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_event(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="events", id=id, params=params)

    def update_event(self, id: int | str, payload: dict | None = None, params: dict | None = None):
        return self._patch_resource(resource="events", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Lists

    # * ####################################################################################### * #

    def get_lists(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="lists", filters=filters, params=params, all_results=all_results, **kwargs
        )

    def show_list(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="lists", id=id, params=params, sideload=sideload, **kwargs
        )

    def add_signups_to_list(
        self,
        list_id: int | str,
        signup_ids: list[str | int] | str | int,
        params: dict | None = None,
        **kwargs,
    ):
        signup_ids = signup_ids if isinstance(signup_ids, list) else [signup_ids]
        payload: dict[str, dict] = {
            "data": {"id": list_id, "type": "lists", "signup_ids": signup_ids}
        }
        return self.client.patch_request(
            url=f"lists/{id}/add_signups",
            params=params,
            json=payload,
        )

    def remove_signups_from_list(
        self,
        list_id: int | str,
        signup_ids: list[str | int] | str | int,
        params: dict | None = None,
        **kwargs,
    ):
        signup_ids = signup_ids if isinstance(signup_ids, list) else [signup_ids]
        payload: dict[str, dict] = {
            "data": {"id": list_id, "type": "lists", "signup_ids": signup_ids}
        }
        return self.client.patch_request(
            url=f"lists/{id}/remove_signups",
            params=params,
            json=payload,
        )

    def get_signups_on_list(
        self, id: int | str, params: dict | None = None, all_results: bool = True, **kwargs
    ) -> Table:
        return self._list_resource(
            resource="lists",
            params=params,
            all_results=all_results,
            url=f"lists/{id}/signups",
            **kwargs,
        )

    # * ####################################################################################### * #

    # * Membership Types

    # * ####################################################################################### * #

    def get_membership_types(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="membership_types",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_membership_types(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="membership_types", params=params, payload=payload)

    def show_membership_type(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self._show_resource(
            resource="membership_types", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_membership_type(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="membership_types", id=id, params=params)

    def update_membership_type(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self._patch_resource(
            resource="membership_types", id=id, params=params, payload=payload
        )

    # * ####################################################################################### * #

    # * Memberships

    # * ####################################################################################### * #

    def get_memberships(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="memberships",
            filters=filters,
            params=params,
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
        return self._show_resource(
            resource="memberships", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_membership(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="membership", id=id, params=params)

    def update_membership(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self._patch_resource(resource="membership", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Event

    # * ####################################################################################### * #

    def get_pages(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="pages",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_page(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="pages", params=params, payload=payload)

    def show_page(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self._show_resource(
            resource="pages", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_page(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="pages", id=id, params=params)

    def update_page(self, id: int | str, payload: dict | None = None, params: dict | None = None):
        return self._patch_resource(resource="pages", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Path Histories

    # * ####################################################################################### * #

    def get_path_histories(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="path_histories",
            filters=filters,
            params=params,
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
        return self._show_resource(
            resource="path_histories", id=id, params=params, sideload=sideload, **kwargs
        )

    # * ####################################################################################### * #

    # * Path Journey Status Changes

    # * ####################################################################################### * #

    def get_path_journey_status_changes(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="path_journey_status_changes",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_path_journey_status_change(self, payload: dict, params: dict | None = None):
        return self._post_resource(
            resource="path_journey_status_changes", params=params, payload=payload
        )

    def show_path_journey_status_change(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self._show_resource(
            resource="path_journey_status_changes",
            id=id,
            params=params,
            sideload=sideload,
            **kwargs,
        )

    def delete_path_journey_status_change(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="path_journey_status_changes", id=id, params=params)

    def update_path_journey_status_change(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self._patch_resource(
            resource="path_journey_status_changes", id=id, params=params, payload=payload
        )

    # * ####################################################################################### * #

    # * Path Journeys

    # * ####################################################################################### * #

    def get_path_journeys(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="path_journeys",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_path_journey(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="path_journeys", params=params, payload=payload)

    def show_path_journey(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="path_journeys", id=id, params=params, sideload=sideload, **kwargs
        )

    def update_path_journey(self, id: int | str, payload: dict, params: dict | None = None):
        return self._patch_resource(resource="path_journeys", id=id, params=params, payload=payload)

    def abandon_path_journey(
        self, id: int | str, path_journey_status_change_id: int | str, params: dict | None = None
    ):
        id = int(id)
        params = params if params else {}
        params["path_journey_status_change_id"] = int(path_journey_status_change_id)
        return self.client.patch_request(f"path_journeys/{id}/abandon", params=params)

    def complete_path_journey(
        self, id: int | str, path_journey_status_change_id: int | str, params: dict | None = None
    ):
        id = int(id)
        params = params if params else {}
        params["path_journey_status_change_id"] = int(path_journey_status_change_id)
        return self.client.patch_request(f"path_journeys/{id}/complete", params=params)

    def reactivate_path_journey(self, id: int | str, params: dict | None = None):
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/reactivate", params=params)

    def void_path_journey(self, id: int | str, params: dict | None = None):
        id = int(id)
        return self.client.patch_request(f"path_journeys/{id}/void", params=params)

    # * ####################################################################################### * #

    # * Path Steps

    # * ####################################################################################### * #

    def get_path_steps(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="path_steps",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_path_step(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="path_steps", params=params, payload=payload)

    def show_path_step(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="path_steps", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_path_step(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="path_steps", id=id, params=params)

    def update_path_step(self, id: int | str, payload: dict, params: dict | None = None):
        return self._patch_resource(resource="path_steps", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Paths

    # * ####################################################################################### * #

    def get_paths(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="paths", filters=filters, params=params, all_results=all_results, **kwargs
        )

    def post_path(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="paths", params=params, payload=payload)

    def show_path(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="paths", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_path(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="paths", id=id, params=params)

    def update_path(self, id: int | str, payload: dict, params: dict | None = None):
        return self._patch_resource(resource="paths", id=id, params=params, payload=payload)

    # * ####################################################################################### * #

    # * Signup Taggings

    # * ####################################################################################### * #

    def get_signups_taggings(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="signup_taggings",
            filters=filters,
            params=params,
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
        return self._post_resource(resource="signup_taggings", params=params, payload=payload)

    def delete_signup_tagging(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="signup_taggings", id=id, params=params)

    # * ####################################################################################### * #

    # * Signup Tags

    # * ####################################################################################### * #

    def get_signup_tags(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="signup_tags",
            filters=filters,
            params=params,
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
        return self._show_resource(
            resource="signup_tags", id=id, params=params, sideload=sideload, **kwargs
        )

    # * ####################################################################################### * #

    # * Signups

    # * ####################################################################################### * #

    def get_signups(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table:
        return self._list_resource(
            resource="signups",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_signup(self, payload: dict, params: dict) -> dict:
        return self._post_resource(resource="signups", params=params, payload=payload)

    def push_signup(self, payload: dict, params: dict | None = None) -> dict:
        params = params if params else {}
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
        return self._upsert_resource(resource="signups", payload=payload, params=params)

    def upsert_signup(self, **kwargs):
        return self.push_signup(**kwargs)

    def show_signup(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        extra_fields: list[str] | str | bool = True,
        **kwargs,
    ) -> dict:
        if extra_fields is True:
            extra_fields = [
                "billing_address",
                "mailing_address",
                "home_address",
                "primary_address",
                "registered_address",
                "user_submitted_address",
                "work_address",
                "profile_image_url",
            ]
        elif extra_fields is False:
            extra_fields = []
        elif isinstance(extra_fields, str):
            extra_fields = [extra_fields]

        return self._show_resource(
            resource="signups", id=id, params=params, sideload=sideload, **kwargs
        )

    def show_me(self, params: dict | None = None):
        # id does nothing
        return self._show_resource(id=0, resource="signups", params=params, url="signups/me")


class NationBuilder:
    def __new__(
        cls,
        slug: str | None = None,
        access_token: str | None = None,
        parsons_version: str = "v1",
    ) -> NationBuilderV1 | NationBuilderV2:
        if checked_version := check_env.check("NB_PARSONS_VERSION", None, True):
            parsons_version = checked_version
        if parsons_version == "v1":
            logger.info(msg="Consider upgrading to version 2 of the NationBuilder connector!")
            logger.info(
                msg="See docs for more information: https://move-coop.github.io/parsons/html/latest/nation_builder.html"
            )
            return NationBuilderV1(slug=slug, access_token=access_token)
        if parsons_version == "v2":
            return NationBuilderV2(
                slug=check_env.check("NB_SLUG", slug),
                access_token=check_env.check("NB_ACCESS_TOKEN", access_token),
            )
        raise ValueError(f"{parsons_version} not supported")
