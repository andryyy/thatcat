from .quart import session
from components.models.tables import TableSearch


def table_search_helper(
    body, session_key_identifier, default_sort_attr, default_sort_reverse: bool = False
):
    search_model = TableSearch.parse_obj(body or {})
    search_model_post = search_model.dict(exclude_unset=True)

    # Post wins over session wins over default
    page = search_model_post.get(
        "page", session.get(f"{session_key_identifier}_page", search_model.page)
    )
    page_size = search_model_post.get(
        "page_size",
        session.get(f"{session_key_identifier}_page_size", search_model.page_size),
    )
    filters = search_model_post.get(
        "filters",
        session.get(f"{session_key_identifier}_filters", search_model.filters),
    )
    sorting = search_model_post.get(
        "sorting",
        session.get(
            f"{session_key_identifier}_sorting",
            (default_sort_attr, default_sort_reverse),
        ),
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
