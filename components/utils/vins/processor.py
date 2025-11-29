from . import wmi_codes

import itertools
import re


class VINProcessor:
    @staticmethod
    def extract_from_text(text: str) -> list:
        text_upper = text.upper()
        candidates = set()  # Use set to automatically deduplicate candidates

        # Matches: "VIN: 1HGBH41JXMN109186 Model"
        # \b ensures word boundaries, preventing partial matches
        candidates.update(re.findall(r"\b([A-Z0-9]{17})\b", text_upper))

        # Matches: "1HG BH4 1JX MN1 091 86" (OCR sometimes adds spaces)
        # Allows 0-2 spaces between each character
        spaced_matches = re.findall(
            r"\b([A-Z0-9](?:\s{0,2}[A-Z0-9]){16})\b", text_upper
        )
        for match in spaced_matches:
            cleaned = re.sub(r"\s", "", match)  # Remove all spaces
            if len(cleaned) == 17:
                candidates.add(cleaned)

        return VINProcessor._process_candidates(candidates)

    @staticmethod
    def _process_candidates(candidates: set) -> tuple[set, str]:
        corrections = {}
        for vin in candidates:
            corrected_vin = VINProcessor._repair_and_validate(vin)
            if corrected_vin and corrected_vin not in corrections:
                corrections[corrected_vin] = vin

        processed_vins = set(corrections.keys())

        info_parts = []
        for corrected_vin in sorted(processed_vins):
            original_vin = corrections[corrected_vin]
            if corrected_vin != original_vin:
                info_parts.append(f"{original_vin} → {corrected_vin}")

        return processed_vins, ", ".join(info_parts) if info_parts else ""

    @staticmethod
    def _repair_and_validate(vin: str, max_combinations: int = 1000000):
        # 1. Global ILLEGAL fixes (O, Q, I are *never* valid in a VIN)
        ILLEGAL_CHAR_FIXES = {"O": "0", "Q": "0", "I": "1"}

        # 2. Renamed map for common OCR errors (B/8, S/5, T/1)
        #    This is used for the one-way fixes.
        OCR_FIXES = {"B": "8", "S": "5", "T": "1"}

        vin = vin.strip().upper()
        if len(vin) != 17:
            return None

        # 3. Apply global fixes (O, Q, I) to the *entire* string
        vin_cleaned_global = "".join(ILLEGAL_CHAR_FIXES.get(c, c) for c in vin)

        # 4. Apply the one-way OCR fixes *only* for position 9
        pos_9_char = vin_cleaned_global[8]
        vin_cleaned = (
            vin_cleaned_global[:8]
            + OCR_FIXES.get(pos_9_char, pos_9_char)  # Apply pos 9 fix
            + vin_cleaned_global[9:]
        )

        # 5. Validate this "smarter" cleaned version
        if VINProcessor.validate(vin_cleaned):
            return vin_cleaned

        # 6. If still invalid, proceed to brute-force

        prefix = vin_cleaned[:12]
        suffix = vin_cleaned[12:]

        prefix_options = []
        for i, c in enumerate(prefix):
            if i == 8:  # This is position 9 (0-indexed)
                # This character is already fixed and is NOT ambiguous
                prefix_options.append([c])

            # --- MODIFIED PART ---
            # Apply TWO-WAY ambiguous logic for *all other* prefix chars
            elif c == "B":
                prefix_options.append(["B", "8"])
            elif c == "8":
                prefix_options.append(["8", "B"])
            elif c == "S":
                prefix_options.append(["S", "5"])
            elif c == "5":
                prefix_options.append(["5", "S"])
            elif c == "T":
                prefix_options.append(["T", "1"])
            elif c == "1":
                prefix_options.append(["1", "T"])
            else:
                # Not an ambiguous character
                prefix_options.append([c])
            # --- END MODIFIED PART ---

        # Suffix logic: ONE-WAY fixes using the OCR_FIXES map
        suffix_options = [[OCR_FIXES.get(c, c)] for c in suffix]

        # 7. Run the iterator (unchanged)
        tried = 0
        for pre in itertools.product(*prefix_options):
            for suf in itertools.product(*suffix_options):
                candidate = "".join(pre) + "".join(suf)
                tried += 1
                if VINProcessor.validate(candidate):
                    return candidate
                if tried >= max_combinations:
                    return None

        return None

    @staticmethod
    def validate(vin: str) -> bool:
        """
        Validates a VIN using strict structural and checksum rules.

        VIN Structure:
        - Position 1-3: WMI (World Manufacturer Identifier)
        - Position 4-8: VDS (Vehicle Descriptor Section)
        - Position 9: Check digit (0-9, X, or Z)
        - Position 10: Model year (specific valid codes)
        - Position 11: Plant code
        - Position 12: First serial digit (can be letter for high-volume mfrs)
        - Position 13-17: Serial number (MUST be numeric)
        """
        if not vin or len(vin) != 17:
            return False

        vin = vin.upper()

        # Check for illegal characters (I, O, Q are never valid in VINs)
        if any(c in "IOQ" for c in vin):
            return False

        # Position 9 (check digit) must be 0-9, X, or Z
        if vin[8] not in "0123456789XZ":
            return False

        # Position 10 (model year) - valid year codes
        # A-H, J-N, P, R-Y = 1980-2009, 2010-2039 (cycles every 30 years)
        # 1-9 = 2001-2009, 2031-2039
        # Excludes: I, O, Q, U (U rarely used), Z (rarely used)
        # 0 for whatever it means
        valid_year_codes = "ABCDEFGHJKLMNPRSTUVWXY1234567890"
        if vin[9] not in valid_year_codes:
            return False

        # Position 11 (plant code) - alphanumeric
        if not vin[10].isalnum():
            return False

        # Position 12 - can be letter (for high-volume manufacturers) or digit
        if not vin[11].isalnum():
            return False

        # Positions 13-17 - MUST be numeric (last 5 digits required to be numbers)
        if not vin[12:17].isdigit():
            return False

        # Position 1-3 (WMI) should be alphanumeric
        if not vin[0:3].isalnum() or vin[0:3] not in wmi_codes:
            return False

        # Rest-of-World VIN, checksum "not used"
        if vin[8] == "Z":
            return True

        # Standard VIN transliteration table for checksum
        trans = {
            "A": 1,
            "B": 2,
            "C": 3,
            "D": 4,
            "E": 5,
            "F": 6,
            "G": 7,
            "H": 8,
            "J": 1,
            "K": 2,
            "L": 3,
            "M": 4,
            "N": 5,
            "P": 7,
            "R": 9,
            "S": 2,
            "T": 3,
            "U": 4,
            "V": 5,
            "W": 6,
            "X": 7,
            "Y": 8,
            "Z": 9,
        }
        weights = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)

        # Calculate checksum
        checksum = 0
        for pos, char in enumerate(vin):
            if char.isdigit():
                value = int(char)
            elif char in trans:
                value = trans[char]
            else:
                # Character not in valid set
                return False
            checksum += value * weights[pos]

        # Verify check digit matches position 9
        check_digit = "X" if (checksum % 11) == 10 else str(checksum % 11)

        return vin[8] == check_digit
