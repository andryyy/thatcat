from webauthn import (
    generate_registration_options,
    options_to_json,
    verify_registration_response,
    verify_authentication_response,
    generate_authentication_options,
)
from webauthn.helpers import (
    parse_registration_credential_json,
    parse_authentication_credential_json,
)
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    AttestationConveyancePreference,
    UserVerificationRequirement,
    ResidentKeyRequirement,
    PublicKeyCredentialDescriptor,
)
