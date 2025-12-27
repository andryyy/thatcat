import json

from quart import request
from components.utils.lang import LANG
from components.logs import logger


def trigger_notification(
    level: str,
    response_code: int,
    title: str,
    message: str | tuple,
    response_body: str = "",
    duration: int = 7000,
    additional_triggers: dict = {},
    fields: set | list = [],
):
    if isinstance(message, tuple):
        message, *message_params = message
    else:
        message_params = [""]

    logger_payload = {
        "level": level,
        "response_code": response_code,
        "title": title,
        "message": message.format(*message_params),
        "additional_triggers": {k: "*" for k in additional_triggers},
    }

    if level in ("system", "validationError"):
        logger_method = getattr(logger, "info")
    else:
        logger_method = getattr(logger, level)

    logger_method(logger_payload)

    return (
        response_body,
        response_code,
        {
            "HX-Retarget": "body",
            "HX-Trigger": json.dumps(
                {
                    "notification": {
                        "level": level,
                        "title": LANG[request.USER_LANG][title],
                        "message": LANG[request.USER_LANG][message].format(
                            *message_params
                        ),
                        "duration": duration,
                        "fields": fields,
                    },
                    **additional_triggers,
                }
            ),
        },
    )
