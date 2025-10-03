from .quart import session
from components.models.tables import TableSearch


def table_search_helper(
    body, session_key_identifier, default_sort_attr, default_sort_reverse: bool = False
):
    search_model = TableSearch(**body or {})
    post_fields = body.keys()

    if "page" in post_fields:
        page = search_model.page
    else:
        page = session.get(f"{session_key_identifier}_page", search_model.page)

    if "page_size" in post_fields:
        page_size = search_model.page_size
    else:
        page_size = session.get(
            f"{session_key_identifier}_page_size", search_model.page_size
        )

    if "filters" in post_fields:
        filters = search_model.filters
    else:
        filters = session.get(f"{session_key_identifier}_filters", search_model.filters)

    if "sorting" in post_fields:
        sorting = search_model.sorting
    else:
        sorting = session.get(
            f"{session_key_identifier}_sorting",
            (default_sort_attr, default_sort_reverse),
        )

    sort_attr, sort_reverse = sorting

    session.update(
        {
            f"{session_key_identifier}_page": page,
            f"{session_key_identifier}_page_size": page_size,
            f"{session_key_identifier}_sorting": sorting,
            f"{session_key_identifier}_filters": filters,
        }
    )

    return search_model.q, page, page_size, sort_attr, sort_reverse, filters
