import pkgutil
import importlib
import inspect
import pathlib

models_dir = pathlib.Path(__file__).parent

__all__ = ["model_forms", "model_meta"]

for module_info in pkgutil.iter_modules([str(models_dir)]):
    if module_info.ispkg:
        continue
    module_name = (
        f"{__package__}.{module_info.name}"
        if __package__
        else f"components.models.{module_info.name}"
    )
    module = importlib.import_module(module_name)
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and hasattr(obj, "__dataclass_fields__"):
            __all__.append(name)
            globals()[name] = obj


model_forms = {
    "objects": {
        "projects": {
            "name": {
                "title": "Name",
                "description": "The name of the project",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assigned_users": {
                "title": "Administrative Users",
                "description": "These users are allowed to fully administer the project.",
                "type": "users:multi",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "location": {"title": "Location", "type": "location"},
            "radius": {"title": "Location Radius", "type": "number"},
            "notes": {
                "title": "Notes",
                "description": "Additional information; free text",
                "type": "textarea",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
        },
        "cars": {
            "vin": {
                "title": "VIN (Vehicle Identification Number)",
                "description": "The vehicle's identification number",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assigned_users": {
                "title": "Administrative Users",
                "description": "These users are allowed to fully administer the car.",
                "type": "users:multi",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "vendor": {
                "title": "Manufacturer",
                "description": "The vehicle's manufacturer",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "model": {
                "title": "Model",
                "description": "The manufacturer's model designation",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "year": {
                "title": "Year of Manufacture",
                "description": "The vehicle's year of manufacture",
                "type": "number",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assigned_project": {
                "title": "Assigned Project",
                "description": "Assign this car to a project.",
                "type": "project",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "location": {"title": "Location", "type": "location"},
            "notes": {
                "title": "Notes",
                "description": "Additional information; free text",
                "type": "textarea",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assets": {
                "title": "Assets",
                "description": "Associated files",
                "type": "assets",
            },
        },
    },
    "users": {
        "profile": {
            "vault": {
                "title": "Vault configuration",
                "type": "vault",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "first_name": {
                "title": "First name",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "last_name": {
                "title": "Last name",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "email": {
                "title": "Email address",
                "description": "Optional email address",
                "type": "email",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "access_tokens": {
                "title": "API keys",
                "description": "API keys can be used for programmatic access",
                "type": "list:text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "permit_auth_requests": {
                "title": "Interactive sign-in requests",
                "description": "Show a dialog on sign-in requests to signed in users to quickly confirm access",
                "type": "toggle",
                "input_extra": 'autocomplete="off"',
            },
        },
    },
    "system": {
        "settings": {
            "GOOGLE_VISION_API_KEY": {
                "title": "Google Vision API",
                "description": "API key for Google Vision",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            }
        }
    },
}

model_meta = {
    "objects": {
        "types": ["cars", "projects"],
        "patch": {
            "cars": ObjectPatchCar,
            "projects": ObjectPatchProject,
        },
        "add": {
            "cars": ObjectAddCar,
            "projects": ObjectAddProject,
        },
        "base": {
            "cars": ObjectCar,
            "projects": ObjectProject,
        },
        "unique_fields": {  # str only
            "cars": ["vin", "assigned_project"],
            "projects": ["name"],
        },
        "display_name": {
            "cars": "vin",
            "projects": "name",
        },
        "system_fields": {
            "cars": ["assigned_users"],
            "projects": ["assigned_users"],
        },
    }
}
