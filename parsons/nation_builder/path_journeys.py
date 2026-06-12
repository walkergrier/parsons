from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Callable
    from parsons import Table
    from parsons.utilities.api_connector import APIConnector

class PathJourneysNB:
    client: APIConnector
    _list_resource: Callable
    _post_resource: Callable
    _show_resource: Callable
    _patch_resource: Callable
    _delete_resource: Callable

    def fetch_path_journeys(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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
        fields: list | str | None = None,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="path_journeys",
            id=id,
            fields=fields,
            params=params,
            sideload=sideload,
            **kwargs,
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