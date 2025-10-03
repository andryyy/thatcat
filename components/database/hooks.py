from components.logs import logger


async def build_indexes_and_list_views(db):
    from components.database.database import _LRU

    db._cache = None
    db._cache = _LRU(max_entries=2048)
    await db.build_index("cars", ["id", "vin", "assigned_users", "assigned_project"])
    await db.build_index("projects", ["id", "name", "assigned_users"])
    await db.build_index("users", ["id", "login", "credentials.id", "acl"])
    await db.build_index("processings", ["id", "assigned_user"])
    await db.define_list_view(
        "users",
        [
            "login",
            "groups",
            "created",
            "updated",
        ],
    )
    await db.define_list_view(
        "cars",
        [
            "vin",
            "created",
            "updated",
            "assigned_project",
            "assigned_users",
        ],
    )
    await db.define_list_view(
        "projects",
        [
            "name",
            "created",
            "updated",
            "radius",
            "location",
            "assigned_users",
        ],
    )
    logger.success("build_indexes_and_list_views: OK")
