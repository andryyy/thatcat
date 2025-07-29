import asyncio
import re

from components.database import *
from components.models.users import *
from components.utils import batch, merge_models
from components.web.utils.quart import current_app, session

BACKGROUND_TASKS = set()


def session_context(fn):
    async def inner(*args, **kwargs):
        if current_app and session:
            if "user_id" in args:
                if not "system" in session["acl"]:
                    assert session["id"] == user_id
            if fn.__name__ == "create":
                assert "system" in session["acl"]

        return await fn(*args, **kwargs)

    return inner


async def _populate_form_cache():
    IN_MEMORY_DB["CACHE"]["FORMS"]["users"] = await form_options()


@validate_call
async def what_id(login: str):
    async with TinyDB(**dbparams()) as db:
        user = db.table("users").get(Q.login == login)

    if user:
        return user["id"]
    else:
        raise ValueError("login", "The provided login name is unknown")


@session_context
@validate_call
async def create(data: dict):
    create_user = UserAdd.model_validate(data)

    async with TinyDB(**dbparams()) as db:
        if db.table("users").search(Q.login == create_user.login):
            raise ValueError("name", "The provided login name exists")
        insert_data = create_user.model_dump(mode="json")
        db.table("users").insert(insert_data)

    t = asyncio.create_task(_populate_form_cache())
    BACKGROUND_TASKS.add(t)
    t.add_done_callback(BACKGROUND_TASKS.discard)

    return insert_data["id"]


@session_context
@validate_call
async def get(user_id: UUID):
    async with TinyDB(**dbparams()) as db:
        return User.model_validate(db.table("users").get(Q.id == str(user_id)))


@session_context
@validate_call
async def delete(user_id: UUID):
    user = await get(user_id=user_id)

    async with TinyDB(**dbparams()) as db:
        if len(db.table("users").all()) == 1:
            raise ValueError("name", "Cannot delete last user")

        db.table("users").remove(Q.id == str(user_id))

    t = asyncio.create_task(_populate_form_cache())
    BACKGROUND_TASKS.add(t)
    t.add_done_callback(BACKGROUND_TASKS.discard)

    return user.id


@session_context
@validate_call
async def create_credential(user_id: UUID, data: dict):
    credential = CredentialAdd.model_validate(data)
    user = await get(user_id=user_id)

    async with TinyDB(**dbparams()) as db:
        user.credentials.append(credential)
        db.table("users").update(
            {"credentials": user.model_dump(mode="json")["credentials"]},
            Q.id == str(user_id),
        )
        return credential.id


@session_context
@validate_call
async def delete_credential(
    user_id: UUID, hex_id: constr(pattern=r"^[0-9a-fA-F]+$", min_length=2)
):
    user = await get(user_id=user_id)
    matched_user_credential = next(
        (c for c in user.credentials if c.id == bytes.fromhex(hex_id)), None
    )

    if not matched_user_credential:
        raise ValueError(
            "hex_id",
            "The provided credential ID was not found in user context",
        )

    async with TinyDB(**dbparams()) as db:
        user.credentials.remove(matched_user_credential)
        db.table("users").update(
            {"credentials": user.model_dump(mode="json")["credentials"]},
            Q.id == str(user_id),
        )
        return hex_id


@session_context
@validate_call
async def patch(user_id: UUID, data: dict):
    user = await get(user_id=user_id)
    patch_data = UserPatch.model_validate(data)
    patched_user = merge_models(
        user,
        patch_data,
        exclude_strategies=["exclude_override_none"],
    )

    async with TinyDB(**dbparams()) as db:
        if db.table("users").get(
            (Q.login == patched_user.login) & (Q.id != str(user_id))
        ):
            raise ValueError("login", "The provided login name exists")

        db.table("users").update(
            patched_user.model_dump(mode="json"),
            Q.id == str(user_id),
        )

    t = asyncio.create_task(_populate_form_cache())
    BACKGROUND_TASKS.add(t)
    t.add_done_callback(BACKGROUND_TASKS.discard)

    return user.id


@session_context
@validate_call
async def patch_profile(user_id: UUID, data: dict):
    user = await get(user_id=user_id)
    patch_data = UserProfilePatch.model_validate(data)
    patched_user_profile = merge_models(
        user.profile, patch_data, exclude_strategies=["exclude_override_none"]
    )

    async with TinyDB(**dbparams()) as db:
        db.table("users").update(
            {"profile": patched_user_profile.model_dump(mode="json")},
            Q.id == str(user_id),
        )
        return user_id


@session_context
@validate_call
async def patch_credential(
    user_id: UUID, hex_id: constr(pattern=r"^[0-9a-fA-F]+$", min_length=2), data: dict
):
    user = await get(user_id=user_id)
    matched_user_credential = next(
        (c for c in user.credentials if c.id == bytes.fromhex(hex_id)), None
    )

    if not matched_user_credential:
        raise ValueError(
            "hex_id",
            "The provided credential ID was not found in user context",
        )

    user.credentials.remove(matched_user_credential)

    patched_credential = merge_models(
        matched_user_credential,
        CredentialPatch.model_validate(data),
        exclude_strategies=["exclude_override_none"],
    )

    user.credentials.append(patched_credential)

    async with TinyDB(**dbparams()) as db:
        db.table("users").update(
            {"credentials": user.model_dump(mode="json")["credentials"]},
            Q.id == str(user_id),
        )
        return hex_id


@validate_call
async def search(
    name: constr(strip_whitespace=True, min_length=0),
    filters: dict = {},
    pagination: dict = {},
):
    if pagination:
        pagination = UsersPagination.model_validate(pagination)

    query = Q.login.matches(name, flags=re.IGNORECASE)

    for filter_key, filter_value in filters.items():
        if f"list:{filter_key}" in USER_FILTERABLES:
            query &= Q[filter_key].any(ensure_list(filter_value))
        elif f"str:{filter_key}" in USER_FILTERABLES:
            query &= Q[filter_key].one_of(ensure_list(filter_value))

    async with TinyDB(**dbparams()) as db:
        matched_users = db.table("users").search(query)

    if pagination:
        pagination.elements = len(matched_users)
        users_pages = [
            m
            for m in batch(
                matched_users,
                pagination.page_size,
            )
        ]

        try:
            users_pages[pagination.page - 1]
        except IndexError:
            pagination.page = len(users_pages)

        pagination.pages = len(users_pages)

        matched_users = (
            users_pages[pagination.page - 1] if pagination.page else users_pages
        )

        return (
            sorted(
                [User.model_validate(o) for o in matched_users],
                key=lambda doc: getattr(doc, pagination.sort_attr, "login"),
                reverse=pagination.sort_reverse,
            ),
            pagination,
        )
    else:
        return (
            sorted(
                [User.model_validate(o) for o in matched_users],
                key=lambda doc: doc.login,
            ),
            pagination,
        )


async def form_options():
    users, pagination = await search(name="")
    return {
        UUID(user.id): {
            "name": user.login,
            "groups": user.groups,
        }
        for user in users
    }
