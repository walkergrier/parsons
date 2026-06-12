import logging
from collections.abc import Generator, Iterable
from typing import Any, Literal, overload, get_args
from urllib.parse import ParseResult, urlparse

from requests import Response

from parsons import Table
from parsons.utilities.api_connector import APIConnector

from .const import NationBuilderResource, NOT_IMPLEMENTED
from .simple_resources import SimpleNbResources
from .events import EventsNB
from .lists import ListsNB
from .path_journeys import PathJourneysNB
from .signups import SignupsNB

logger: logging.Logger = logging.getLogger(name=__name__)

class NationBuilderV2(
    SimpleNbResources,
    EventsNB,
    ListsNB,
    PathJourneysNB,
    SignupsNB,
):
    def __init__(self, slug: str, access_token: str) -> None:
        self.client: APIConnector = APIConnector(
            NationBuilderV2.get_uri(slug=slug),
            headers=NationBuilderV2.get_auth_headers(access_token=access_token),
            data_key="data",
        )

    __resources_not_implemented: set[str] = NOT_IMPLEMENTED

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
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "authorization": f"Bearer {access_token}",
        }
        return headers

    @staticmethod
    def _to_table(data: list[dict]) -> Table:
        """
        Converts a list of dictionary API resources into a Table object.
        Flattens the 'attributes' key into the main dictionary.

        Args:
            data:
                A list of resource dictionaries from the API.

        Returns:
            A Table object containing the formatted data.
        """
        result: list[dict] = [
            {"id": i["id"], "type": i["type"]} | i.get("attributes", {}) for i in data
        ]
        return Table(result)

    # TODO: implment this
    @staticmethod
    def fmt_list(values) -> str:
        if isinstance(values, (str, int)) or not isinstance(values, Iterable):
            return str(values)
        return ",".join(map(str, values))

    @staticmethod
    def _param_builder(
        values: dict | str | list,
        param_name: str,
        resource: NationBuilderResource,
    ) -> list[tuple]:
        """
        Converts a parameter dictionary into NationBuilder's specific param format.
        Handles simple and nested structures.

        Args:
            param_name:
                The base name for the parameter (e.g., 'filter').
            param_dict:
                The dictionary of parameters to format.

        Returns:
            A list of (key, value) tuples formatted for the API request.
        """

        if param_name == "include":
            return [(param_name, NationBuilderV2.fmt_list(values))]

        # if param_name in ("fields", "extra_fields"):
        #     if isinstance(values, str):
        #         final_values: str = f"[{values}]"
        #     if isinstance(values, (list,tuple,set)):
        #         final_values = f"[{}]"

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
            return [(f"{param_name}[{resource}]", NationBuilderV2.fmt_list(values))]

    @staticmethod
    def _urlparse(url: str) -> tuple[str, list[tuple]]:
        """
        Parses a URL string to separate the path from the query parameters.

        Args:
            url:
                The full or partial URL to parse.

        Returns:
            A tuple containing the URL path and the parsed parameters.
        """
        url = url[len("/api/v2/") :] if url.startswith("/api/v2/") else url
        parsed_url: ParseResult = urlparse(url=url)
        query: str = parsed_url.query
        if not query:
            return parsed_url.path, []
        return parsed_url.path, [tuple(p.split(sep="=", maxsplit=1)) for p in query.split(sep="&")]

    def fetch_next(self, resp: dict | Response) -> tuple[str, list[tuple]] | tuple[None, None]:
        """
        Fetches the next page of results from a paginated API response.

        Args:
            resp:
                The current API response dictionary which may contain a 'next' link.

        Returns:
            The API response for the next page, or None if there is no next page.
        """
        if isinstance(resp, Response):
            resp_dict: dict = resp.json()
        else:
            resp_dict: dict = resp
        if "links" in resp_dict and "next" in resp_dict["links"]:
            url, params = self._urlparse(url=resp_dict["links"]["next"])
            return url, params
        else:
            return None, None

    def _fetch_all(self, resp: dict, limit: int = 0) -> Table:
        """
        Fetches all pages of results from a paginated API response, up to a specified limit.

        Args:
            resp:
                The initial API response.
            limit:
                The maximum number of records to retrieve. If 0, all records are fetched.

        Returns:
            A Table object containing all the aggregated data.
        """
        data: list[dict] = resp["data"]
        while limit <= 0 or len(data) < limit:
            url, params = self.fetch_next(resp=resp)
            if url is None:
                break
            resp = self.client.get_request(url, params=params)
            data.extend(resp["data"])
        return self._to_table(data=data)

    def _fetch_all_generator(self, resp: dict, limit: int = 0) -> Generator[Any, None, None]:
        """
        A generator that yields Table objects page by page.
        """
        yield self._to_table(data=resp["data"])
        count = len(resp["data"])

        while limit <= 0 or count < limit:
            url, params = self.fetch_next(resp=resp)
            if url is None:
                break

            resp = self.client.get_request(url, params=params)

            if not resp.get("data"):
                break

            page_table = self._to_table(data=resp["data"])
            yield page_table

            count += len(resp["data"])

    # * ####################################################################################### * #

    # * Resource Methods

    # * ####################################################################################### * #

    @overload
    def _list_resource(
        self,
        resource: NationBuilderResource,
        *args: Any,
        all_results: Literal["gen"] = "gen",
        **kwargs: Any,
    ) -> Generator: ...

    @overload
    def _list_resource(
        self, resource: NationBuilderResource, *args: Any, all_results: bool = False, **kwargs: Any
    ) -> Table: ...

    def _list_resource(
        self,
        resource: NationBuilderResource,
        filters: dict | None = None,
        fields: list | str | None = None,
        extra_fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | list[tuple] | None = None,
        all_results: bool | str = False,
        url: str = "",
        page_size: int = 100,
        limit: int = 0,
        raw_resp: bool = False,
        debug_params: bool = False,
        **kwargs,
    ) -> Table | Generator:
        """
        Lists records for a given resource, with options for filtering and pagination.

        Args:
            resource:
                The name of the resource to list (e.g., 'people', 'lists').
            filters:
                A dictionary of filters to apply to the query.
            params:
                Additional query parameters.
            all_results:
                If True, fetches all pages of results.
            url:
                A specific URL to use instead of the resource name.
            page_size:
                The number of results to return per page (max 100).
            limit:
                The maximum number of results to return when all_results is True.
            raw_resp:
                If True, returns the raw API response dictionary.
            count:
                If True, requests the total count of matching records.

        Returns:
            A Table of results, or a raw dictionary if raw_resp is True.
        """
        url = url if url else resource

        if not params:
            params = []
        if isinstance(params, dict):
            params = list(params.items())
        params.append(
            ("page[size]", min(100, max(1, page_size))),
        )

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

        resp: dict = self.client.get_request(url, params=params)

        if all_results is True:
            return self._fetch_all(resp=resp, limit=limit)
        elif isinstance(all_results, str) and all_results.lower() in ("gen", "generator"):
            return self._fetch_all_generator(resp=resp, limit=limit)
        if all_results not in (False, True, "gen", "generator"):
            logger.warning("all_results should be type bool or 'generator'")

        return self._to_table(data=resp["data"])

    def _show_resource(
        self,
        resource: NationBuilderResource,
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
            resource:
                The name of the resource.
            id:
                The unique ID of the record.
            params:
                Additional query parameters.
            url:
                A specific URL to use instead of the resource name and ID.
            sideload:
                Sideload related resources.
                True for all, or a list of specific relations.

        Returns:
            A dictionary representing the resource.
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

        resp: dict = self.client.get_request(url, params=params)["data"]
        resp |= resp.pop("attributes", {})
        resp["relationships"] = resp.pop("relationships", {})

        if not sideload:
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

    def _sideload_resource(self, resp: dict, resource: NationBuilderResource) -> Table | None:
        """
        Fetches and sideloads a related resource from a relationship link.

        Args:
            resp:
                The primary resource's response dictionary.
            resource:
                The name of the relationship to sideload.

        Returns:
            A Table of the related resources, or None if the link is not present.
        """
        link: str | None = resp["relationships"][resource]["links"]["related"]
        if not link:
            return None
        url, params = self._urlparse(url=link)
        return self._list_resource(resource=resource, params=params, url=url, all_results=True)

    def _post_resource(
        self,
        resource: NationBuilderResource,
        params: dict | None,
        payload: dict,
        payload_override: dict | None = None,
        url: str = "",
        sideposting=False,
    ):
        """
        Creates a new resource record.

        Args:
            resource:
                The name of the resource to create.
            payload:
                The attributes for the new record.
            params:
                Additional query parameters.
            url:
                A specific URL to use for the POST request.

        Returns:
            The API response for the creation request.
        """
        url = url if url else resource
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = payload_override or {"data": {"type": resource, "attributes": payload}}
        return self.client.post_request(url, params=params, json=payload)

    def _delete_resource(self, resource: NationBuilderResource, id: int | str, params: dict | None = None, url: str = ""):
        """
        Deletes a resource record by its ID.

        Args:
            resource:
                The name of the resource.
            id:
                The ID of the record to delete.
            params:
                Additional query parameters.
            url:
                A specific URL to use for the DELETE request.

        Returns:
            dict: The API response.
        """
        id = int(id)
        url = url if url else f"{resource}/{id}"
        return self.client.delete_request(url, params=params)

    def _upsert_resource(
        self, resource: NationBuilderResource, payload: dict, params: dict | list[tuple] | None = None, url: str = ""
    ):
        """
        Creates or updates a resource record using the '/push' endpoint.

        Args:
            resource:
                The name of the resource.
            payload:
                The attributes for the record to upsert.
            params:
                Additional query parameters.
            url:
                A specific URL to use for the request.

        Returns:
            The API response.
        """
        url = url if url else f"{resource}/push"
        params = params if params else {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    def _patch_resource(
        self,
        resource: NationBuilderResource,
        id: int | str,
        params: dict | list[tuple] | None,
        payload: dict | None,
        url: str = "",
    ):
        """
        Updates an existing resource record.

        Args:
            resource:
                The name of the resource.
            id:
                The ID of the record to update.
            payload:
                The attributes to update on the record.
            params:
                Additional query parameters.
            url:
                A specific URL to use for the PATCH request.

        Returns:
            The API response.
        """
        id = int(id)
        url = url if url else f"{resource}/{id}"
        if not isinstance(payload, dict):
            raise ValueError("payload must be dict")
        payload = {"data": {"id": id, "type": resource, "attributes": payload}}
        return self.client.patch_request(url, params=params, json=payload)

    # *
    # * Genral Fetch Function ---------------------------------------------------------------------

    def count_resource(
        self,
        resource: NationBuilderResource,
        filters: dict | None = None,
        url: str = "",
        params: dict | None = None,
    ) -> int:
        url = url if url else resource
        params_list: list[tuple] = list(params.items()) if params else []
        if filters:
            params_list.extend(
                self._param_builder(values=filters, param_name="filter", resource=resource)
            )
        params_list.extend(
            [
                (f"fields{resource}", "id"),
                ("stats[total]", "count"),
                ("page[size]", 1),
            ]
        )
        resp: dict = self.client.get_request(url, params=params_list)
        return resp["meta"]["stats"]["total"]["count"]

    def fetch_resource(self, resource: NationBuilderResource, **kwargs) -> Table:
        if resource not in get_args(NationBuilderResource):
            raise ValueError(f"'{resource}' is not a vaild resource")
        if hasattr(self, f"fetch_{resource}"):
            return getattr(self,f"fetch_{resource}")(**kwargs)
        if resource in self.__resources_not_implemented:
            raise NotImplementedError(
                f"'{resource}' is a vaild resource, but is not currently accessible"
            )
        return self._list_resource(resource=resource, **kwargs)

    def post_resource(self, resource: NationBuilderResource, payload: dict | None = None, **kwargs) -> Table:
        if resource not in get_args(NationBuilderResource):
            raise ValueError(f"'{resource}' is not a vaild resource")
        
        kwargs = {"payload":payload} | kwargs
        
        if hasattr(self, f"post_{resource}"):
            return getattr(self,f"fetch_{resource}")(**kwargs)
        if resource in self.__resources_not_implemented:
            raise NotImplementedError(
                f"'{resource}' is a vaild resource, but is not currently accessible"
            )
        return self._list_resource(resource=resource, **kwargs)