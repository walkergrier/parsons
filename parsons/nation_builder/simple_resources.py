from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Callable
    from parsons import Table
    from parsons.utilities.api_connector import APIConnector


class SimpleNbResources:
    _list_resource: Callable
    _post_resource: Callable
    _show_resource: Callable
    _patch_resource: Callable
    _delete_resource: Callable

    # *
    # * Automation Enrollments --------------------------------------------------------------------

    def fetch_automation_enrollments(
        self,
        fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Automations -------------------------------------------------------------------------------

    def fetch_automations(
        self,
        fields: list | str | None = None,
        extra_fields: list | str | None = None,
        include: list | str | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Contacts ----------------------------------------------------------------------------------

    def fetch_contacts(
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

    # *
    # * Donation Tracking Codes -------------------------------------------------------------------

    def fetch_donation_tracking_codes(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Donations ---------------------------------------------------------------------------------

    def fetch_donations(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Event RSVPs -------------------------------------------------------------------------------

    def fetch_event_rsvps(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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


    # *
    # * Membership Types --------------------------------------------------------------------------

    def fetch_membership_types(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Memberships -------------------------------------------------------------------------------

    def fetch_memberships(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
        return self._list_resource(
            resource="memberships",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_membership(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="memberships", params=params, payload=payload)

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
        return self._delete_resource(resource="memberships", id=id, params=params)

    def update_membership(
        self, id: int | str, payload: dict | None = None, params: dict | None = None
    ):
        return self._patch_resource(resource="memberships", id=id, params=params, payload=payload)

    # *
    # * Pages -------------------------------------------------------------------------------------

    def fetch_pages(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Path Histories ----------------------------------------------------------------------------

    def fetch_path_histories(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

    # *
    # * Path Journey Status Changes ---------------------------------------------------------------

    def fetch_path_journey_status_changes(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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


    # *
    # * Path Steps --------------------------------------------------------------------------------

    def fetch_path_steps(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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
        fields: list | str | None = None,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="path_steps", id=id, fields=fields, params=params, sideload=sideload, **kwargs
        )

    def delete_path_step(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="path_steps", id=id, params=params)

    def update_path_step(self, id: int | str, payload: dict, params: dict | None = None):
        return self._patch_resource(resource="path_steps", id=id, params=params, payload=payload)

    # *
    # * Paths -------------------------------------------------------------------------------------

    def fetch_paths(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
        return self._list_resource(
            resource="paths", filters=filters, params=params, all_results=all_results, **kwargs
        )

    def post_path(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="paths", params=params, payload=payload)

    def show_path(
        self,
        id: int | str,
        fields: list | str | None = None,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="paths", id=id, fields=fields, params=params, sideload=sideload, **kwargs
        )

    def delete_path(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="paths", id=id, params=params)

    def update_path(self, id: int | str, payload: dict, params: dict | None = None):
        return self._patch_resource(resource="paths", id=id, params=params, payload=payload)

    # *
    # * Signup Taggings ---------------------------------------------------------------------------

    def fetch_signup_taggings(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
        return self._list_resource(
            resource="signup_taggings",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def show_signup_tagging(
        self,
        id: int | str,
        fields: list | str | None = None,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="signup_taggings",
            id=id,
            fields=fields,
            params=params,
            sideload=sideload,
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
        payload: dict[str, str | int] = {"signup_id": signup_id, "tag_id": tag_id}
        return self._post_resource(resource="signup_taggings", params=params, payload=payload)

    def delete_signup_tagging(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="signup_taggings", id=id, params=params)

    # *
    # * Signup Tags -------------------------------------------------------------------------------

    def fetch_signup_tags(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
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

