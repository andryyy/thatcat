model_forms = {
    "objects": {
        "processings": {
            "vin": {
                "title": "VIN (Vehicle Identification Number)",
                "description": "The vehicle's identification number",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assigned_project": {
                "title": "Assigned Project",
                "description": "Assign this car to a project",
                "type": "project",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assets": {
                "title": "Assets",
                "description": "Associated files",
                "type": "assets",
            },
        },
        "projects": {
            "name": {
                "title": "Name",
                "description": "The name of the project",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "assigned_users": {
                "title": "Administrative Users",
                "description": "These users are allowed to fully administer the project",
                "type": "users:multi",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "location": {"title": "Location", "type": "location"},
            "notes": {
                "title": "Notes",
                "vault": "true",
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
                "description": "These users are allowed to fully administer the car",
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
                "description": "Assign this car to a project",
                "type": "project",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "car_markers": {
                "title": "Car markers",
                "description": "Set markers for this car",
                "type": "car_markers",
            },
            "location": {"title": "Location", "type": "location"},
            "notes": {
                "title": "Notes",
                "vault": "true",
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
            "claude_api_key": {
                "title": "Claude API key",
                "description": "Claude API key",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "claude_model": {
                "title": "Claude API model",
                "description": "Claude API model",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
            "google_vision_api_key": {
                "title": "Google Vision API key",
                "description": "Google Vision API key",
                "type": "text",
                "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
            },
        }
    },
}
