from . import wmi_codes

import itertools
import re


class VINProcessor:
    @staticmethod
    def extract_candidates(text: str) -> list:
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

        return list(candidates)

    @staticmethod
    def process_vin_candidates(candidates: list) -> tuple[list, str]:
        processed_vins = set()
        corrections = {}

        for vin in candidates:
            candidate = VINProcessor.process_vin(vin)
            if candidate:  # If validation/correction succeeded
                processed_vins.add(candidate)
                if candidate not in corrections:
                    corrections[candidate] = []
                corrections[candidate].append(vin)

        notes_parts = []
        for corrected_vin in sorted(processed_vins):
            originals = corrections[corrected_vin]
            original = originals[0]
            if corrected_vin != original:
                notes_parts.append(f"{original} → {corrected_vin}")
            else:
                notes_parts.append(corrected_vin)

        notes = ", ".join(notes_parts) if notes_parts else "No valid VINs found"
        return list(processed_vins), notes

    @staticmethod
    def process_vin(vin: str, max_combinations: int = 1000000):
        ILLEGAL_CHAR_FIXES = {"O": "0", "Q": "0", "I": "1"}

        vin = vin.strip().upper()
        if len(vin) != 17:
            return None

        vin_cleaned = "".join(ILLEGAL_CHAR_FIXES.get(c, c) for c in vin)

        if VINProcessor.validate_vin(vin_cleaned):
            return vin_cleaned

        prefix = vin_cleaned[:11]
        suffix = vin_cleaned[11:]
        prefix_options = [
            (["8", c] if c == "B" else ["B", c] if c == "8" else [c]) for c in prefix
        ]
        suffix_options = [(["8"] if c == "B" else [c]) for c in suffix]

        tried = 0
        for pre in itertools.product(*prefix_options):
            for suf in itertools.product(*suffix_options):
                candidate = "".join(pre) + "".join(suf)
                tried += 1
                if VINProcessor.validate_vin(candidate):
                    return candidate
                if tried >= max_combinations:
                    return None

        return None

    @staticmethod
    def validate_vin(vin: str) -> bool:
        """
        Validates a VIN using strict structural and checksum rules.

        VIN Structure:
        - Position 1-3: WMI (World Manufacturer Identifier)
        - Position 4-8: VDS (Vehicle Descriptor Section)
        - Position 9: Check digit (0-9 or X)
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

        # Position 9 (check digit) must be 0-9 or X
        if vin[8] not in "0123456789X":
            return False

        # Position 10 (model year) - valid year codes
        # A-H, J-N, P, R-Y = 1980-2009, 2010-2039 (cycles every 30 years)
        # 1-9 = 2001-2009, 2031-2039
        # Excludes: I, O, Q, U (U rarely used), Z (rarely used)
        valid_year_codes = "ABCDEFGHJKLMNPRSTUVWXY123456789"
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

        # Calculate expected check digit
        check_digit = "X" if (checksum % 11) == 10 else str(checksum % 11)

        # Verify check digit matches position 9
        return vin[8] == check_digit
