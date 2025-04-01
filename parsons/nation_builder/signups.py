from parsons import Table
from parsons.utilities import check_env

from typing import Any, Dict, Optional, Tuple, List, Set, Sequence, cast


class Signups:

    def all_signups(
        self,
        nearby: List[Dict] | Dict | None = None,
        page_id: int | str | None = None,
        tag_id: int | str | None = None,
        page_number: int | str | None = None,
        page_size: int | str | None = 100,
        include: List | Set | Tuple | None = None,
        fields: List | Set | Tuple | None = None,
        extra_fields: List | Set | Tuple | None = None,
    ) -> Table:
        """
        `Args:`
            nearby :  List[Dict] | Dict | None = None
                Filter signups by location (lat, long) and distance from the location in miles.
                Format for the parameter value is {"location": "90.0,-180.0", "distance": 15}.
                If a distance is not provided, signups within 1 mile from the location
                will be returned.
            page_id: int | str | None = None
                Filters Signups by page_id, the id of the page they signed up from.
            tag_id: int | str | None = None
            page_number: int | str | None = None
                Page number to list (starting at 1)
            page_size: int | str | None = 100
                Number of results to display per page (default: 100, max: 100, min: 1)
            include: List | Set | Tuple | None = None
                Comma-delimited list of sideloaded resources to include as part of the response.
            fields: List | Set | Tuple | None = None
                Comma-delimited list of attributes to only return in the response
            extra_fields: List | Set | Tuple | None = None
                Comma-delimited list of extra attributes, which are only returned in the response if requested.

        `Returns:`
            A Table of all signups stored in Nation Builder.
        """

        valid_includes = {
            "author",
            "last_contacted_by",
            "page",
            "parent",
            "precinct",
            "recruiter",
            "signup_profile",
            "signup_tags"
            "voter",
        }

        valid_extra_fields = {
            "billing_address",
            "mailing_address",
            "home_address",
            "primary_address",
            "registered_address",
            "user_submitted_address",
            "work_address",
            "profile_image_url",
        }

        params = {
            "filter": {
                "nearby": nearby,
                "page_id": page_id,
                "with_email_address": None,
                "with_bouncing_email": None,
                "tag_id": tag_id,
            },
            "page": {
                "number": page_number,
                "size": page_size,
            },
            "include": include,
            "fields": {"signups": fields},
            "extra_fields": {"signups": extra_fields},
        }
