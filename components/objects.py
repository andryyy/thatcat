import asyncio
import re
from components.database import *
from components.logs import logger
from components.models.objects import *
from components.utils import ensure_list, merge_models, batch
from components.utils.osm import coords_to_display_name
from components.web.utils.quart import current_app, session

BACKGROUND_TASKS = set()


def session_context(fn):
    async def inner(*args, **kwargs):
        if current_app and session:
            if not "session_context" in kwargs:
                kwargs["session_context"] = session["id"], session["acl"]
            else:
                logger.warning(
                    f"{fn.__name__}: session_context changed to {kwargs.get('session_context')}"
                )
        return await fn(*args, **kwargs)

    return inner


@validate_call
async def _populate_form_cache(object_type: Literal[*model_classes["types"]]):
    IN_MEMORY_DB["CACHE"]["FORMS"][object_type] = await form_options(object_type)


@session_context
@validate_call
async def get(
    object_type: Literal[*model_classes["types"]],
    object_id: UUID,
    session_context: tuple = (),
):
    query = Q.id == str(object_id)

    if session_context:
        user_id, user_acl = session_context
        if not "system" in user_acl:
            query &= Q.details.assigned_users.any([user_id])

    async with TinyDB(**dbparams()) as db:
        object_data = db.table(object_type).get(query)

        if object_data:
            o = model_classes["base"][object_type].model_validate(object_data)
            if session_context and not "system" in user_acl:
                for f in model_classes["system_fields"][object_type]:
                    delattr(o.details, f)
            return o

    return object_data


@session_context
@validate_call
async def delete(
    object_type: Literal[*model_classes["types"]],
    object_id: UUID | list[UUID],
    session_context: tuple = (),
):
    object_ids = [str(o) for o in ensure_list(object_id)]
    query = Q.id.one_of(object_ids)

    if session_context:
        user_id, user_acl = session_context
        if not "system" in user_acl:
            query &= Q.details.assigned_users.any([user_id])

    if object_type == "projects":
        results, pagination = await search(
            object_type="cars",
            q="",
            filters={"assigned_project": object_ids},
            session_context=("", ["system"]),
        )
        if results:
            non_empty_projects = []
            for car in results:
                project = await get(
                    object_type="projects", object_id=car.details.assigned_project
                )
                non_empty_projects.append(project.name)

            raise ValueError(
                "name",
                f"Den Projekten {', '.join(non_empty_projects)} sind Autos zugeordnet"
                if len(non_empty_projects) > 1
                else f"Dem Projekt {non_empty_projects[0]} sind Autos zugeordnet",
            )

    async with TinyDB(**dbparams()) as db:
        db.table(object_type).remove(query)

    t = asyncio.create_task(_populate_form_cache(object_type))
    BACKGROUND_TASKS.add(t)
    t.add_done_callback(BACKGROUND_TASKS.discard)

    return object_ids


@session_context
@validate_call
async def patch(
    object_type: Literal[*model_classes["types"]],
    object_id: UUID | list[UUID],
    data: dict,
    session_context: tuple = (),
):
    to_patch_objects = [
        await get(object_type, object_id, session_context=("", ["system"]))
        for object_id in ensure_list(object_id)
    ]
    patched_objects = []

    if session_context:
        user_id, user_acl = session_context

    for to_patch in to_patch_objects:
        if session_context and not "system" in user_acl:
            if not UUID(user_id) in to_patch.details.assigned_users:
                continue
            if not "details" in data:
                data["details"] = dict()
            for f in model_classes["system_fields"][object_type]:
                if f in data["details"]:
                    if hasattr(to_patch.details, f):
                        data["details"][f] = getattr(to_patch.details, f)
                    else:
                        del data["details"][f]

        patch_data = model_classes["patch"][object_type].model_validate(data)

        patched_object = merge_models(
            to_patch,
            patch_data,
            exclude_strategies=["exclude_override_none"],
        )

        results, _ = await search(
            object_type=object_type,
            q="",
            filters={
                f: str(getattr(patched_object.details, f))
                for f in model_classes["unique_fields"][object_type]
            },
            session_context=("", ["system"]),
        )

        for result in results:
            if result.id != patched_object.id:
                raise ValueError(
                    f"name",
                    "Ein solches Objekt existiert bereits",
                )

        if (
            hasattr(patched_object.details, "location")
            and isinstance(patched_object.details.location, Location)
            and patched_object.details.location._is_valid
            and to_patch.details.location != patched_object.details.location
            and patched_object.details.location.display_name
            == patched_object.details.location.coords
        ):
            try:
                patched_object.details.location.display_name = (
                    await coords_to_display_name(patched_object.details.location.coords)
                )
            except (ValueError, ValidationError) as e:
                pass

        if object_type == "cars":
            if (
                patched_object.details.assigned_project
                != to_patch.details.assigned_project
            ):
                if session_context and not "system" in user_acl:
                    for project in [
                        patched_object.details.assigned_project,
                        to_patch.details.assigned_project,
                    ]:
                        if not await get("projects", project):
                            raise ValueError(
                                "details.assigned_project",
                                f"Das Projekt kann nicht geändert werden",
                            )
                else:
                    if not await get(
                        "projects", patched_object.details.assigned_project
                    ):
                        raise ValueError(
                            "details.assigned_project",
                            f"Das Projekt ist nicht bekannt",
                        )

        async with TinyDB(**dbparams()) as db:
            db.table(object_type).update(
                patched_object.model_dump(
                    mode="json", exclude={"name", "id", "created"}
                ),
                Q.id == to_patch.id,
            )
            patched_objects.append(patched_object.id)

    t = asyncio.create_task(_populate_form_cache(object_type))
    BACKGROUND_TASKS.add(t)
    t.add_done_callback(BACKGROUND_TASKS.discard)

    return patched_objects


@session_context
@validate_call
async def create(
    object_type: Literal[*model_classes["types"]],
    data: dict,
    session_context: tuple = (),
):
    if not "details" in data:
        data["details"] = dict()

    if session_context:
        user_id, user_acl = session_context

    if session_context:
        data["details"]["assigned_users"] = user_id

    create_object = model_classes["add"][object_type].model_validate(data)

    conflicts, _ = await search(
        object_type=object_type,
        q="",
        filters={
            f: str(getattr(create_object.details, f))
            for f in model_classes["unique_fields"][object_type]
        },
        session_context=("", ["system"]),
    )

    for result in conflicts:
        raise ValueError(
            f"name",
            "Ein solches Objekt existiert bereits",
        )

    if object_type == "cars":
        if create_object.details.assigned_project and not await get(
            "projects", create_object.details.assigned_project
        ):
            raise ValueError("name", "Das ausgewählte Projekt ist nicht verfügbar")

    if object_type == "projects":
        if session_context and not "system" in user_acl:
            raise ValueError("name", "Nur Administratoren können Projekte anlegen")

    async with TinyDB(**dbparams()) as db:
        insert_data = create_object.model_dump(mode="json")
        db.table(object_type).insert(insert_data)

    t = asyncio.create_task(_populate_form_cache(object_type))
    BACKGROUND_TASKS.add(t)
    t.add_done_callback(BACKGROUND_TASKS.discard)

    return create_object.id


@validate_call(config=dict(arbitrary_types_allowed=True))
@session_context
async def search(
    object_type: Literal[*model_classes["types"]],
    q: str = "",
    filters: dict = {},
    pagination: dict = {},
    session_context: tuple = (),
):
    if pagination:
        pagination = ObjectPagination.model_validate(pagination)

    async with TinyDB(**dbparams()) as db:
        and_parts = []
        or_parts = []

        if session_context:
            user_id, user_acl = session_context

        if session_context and not "system" in user_acl:
            and_parts = [Q.details.assigned_users.any([user_id])]

        for s in model_classes["searchables"][object_type]:
            or_parts.append(Q["details"][s].matches(q, flags=re.IGNORECASE))

        for filter_key, filter_value in filters.items():
            if filter_key in model_classes["filterables"][object_type]["list"]:
                and_parts.append(
                    Q["details"][filter_key].any(
                        [str(v) for v in ensure_list(filter_value)]
                    )
                )
            elif filter_key in model_classes["filterables"][object_type]["str"]:
                and_parts.append(
                    Q["details"][filter_key].one_of(
                        [str(v) for v in ensure_list(filter_value)]
                    )
                )
            else:
                logger.warning(f"Ignoring invalid filter key: {filter_key}")

        try:
            and_query = and_parts[0]
            for q in and_parts[1:]:
                and_query = and_query & q
        except IndexError:
            and_query = None

        try:
            or_query = or_parts[0]
            for q in or_parts[1:]:
                or_query = or_query | q
        except IndexError:
            or_query = None

        if and_query and or_query:
            final_query = and_query & or_query
        elif and_query:
            final_query = and_query
        elif or_query:
            final_query = or_query
        else:
            final_query = None

        if final_query:
            matched_objects = db.table(object_type).search(final_query)
        else:
            matched_objects = db.table(object_type).all()

    if pagination:
        if pagination.sort_attr == "name":
            matched_objects = sorted(
                matched_objects,
                key=lambda d: d.get("details", {}).get(
                    model_classes["base"][object_type]
                    .model_computed_fields["name"]
                    .title,
                    "",
                ),
                reverse=pagination.sort_reverse,
            )
        else:
            matched_objects = sorted(
                matched_objects,
                key=lambda d: d.get(pagination.sort_attr, ""),
                reverse=pagination.sort_reverse,
            )

        pagination.elements = len(matched_objects)
        object_pages = [
            m
            for m in batch(
                matched_objects,
                pagination.page_size,
            )
        ]

        try:
            object_pages[pagination.page - 1]
        except IndexError:
            pagination.page = len(object_pages)

        pagination.pages = len(object_pages)

        matched_objects = (
            object_pages[pagination.page - 1] if pagination.page else object_pages
        )

        return [
            model_classes["base"][object_type].model_validate(o)
            for o in matched_objects
        ], pagination

    else:
        return (
            sorted(
                [
                    model_classes["base"][object_type].model_validate(o)
                    for o in matched_objects
                ],
                key=lambda doc: doc.name,
            ),
            pagination,
        )


@validate_call
async def form_options(object_type: Literal[*model_classes["types"]]):
    results, pagination = await search(
        object_type=object_type,
        q="",
        session_context=("", ["system"]),
    )
    return {
        UUID(result.id): {
            "name": result.name,
            "assigned_users": result.details.assigned_users,
            "radius": result.details.radius if hasattr(result.details, "radius") else 0,
            "location": result.details.location
            if hasattr(result.details, "location")
            else None,
        }
        for result in results
    }
