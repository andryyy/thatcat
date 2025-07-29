import base64
import itertools
import re

from components.utils.ai import google_vision_api


class VinTool:
    @classmethod
    async def extract_from_bytes(self, file_bytes: bytes):
        base64_image = base64.b64encode(file_bytes).decode("utf-8")

        try:
            ai_response = await google_vision_api(base64_image, ["TEXT_DETECTION"])
            text = ai_response["responses"][0]["fullTextAnnotation"]["text"]
        except (KeyError, IndexError):
            raise Exception("The provided image did not contain any readable text")
        except Exception:
            raise Exception("The Vision API request failed")

        vin_matches = re.findall(r"([A-Z0-9]{17})", text)
        if not vin_matches:
            text = re.sub(r"[^A-Z0-9 ]", "", text)
            vin_matches = re.findall(r"([A-Z0-9]{17})", text)

        vins = set()
        for vin in vin_matches:
            vin_guess = self.repair_vin(vin)
            if vin_guess:
                vins.add(vin_guess)

        return list(vins)

    @classmethod
    def repair_vin(self, ocr_vin: str, max_combinations: int = 1000000):
        ILLEGAL_CHAR_FIXES = {
            "O": "0",
            "Q": "0",
            "I": "1",
        }

        B8_SWAPS = {
            "B": ["8"],
            "8": ["B"],
        }

        def _get_b8_swap_options_prefix(c):
            return B8_SWAPS.get(c, []) + [c]

        def _get_b8_swap_options_suffix(c):
            return ["8", "B"] if c == "B" else [c]

        ocr_vin = ocr_vin.strip().upper()
        if len(ocr_vin) != 17:
            raise ValueError("VIN must be exactly 17 characters long")

        vin_cleaned = "".join(ILLEGAL_CHAR_FIXES.get(c, c) for c in ocr_vin)

        try:
            if self.verify_checksum(vin_cleaned):
                return vin_cleaned
        except Exception as e:
            pass

        prefix = vin_cleaned[:11]
        suffix = vin_cleaned[11:]

        prefix_options = [_get_b8_swap_options_prefix(c) for c in prefix]
        suffix_options = [_get_b8_swap_options_suffix(c) for c in suffix]

        prefix_product = itertools.product(*prefix_options)
        suffix_product = list(itertools.product(*suffix_options))

        tried = 0
        for pre in prefix_product:
            for suf in suffix_product:
                candidate = "".join(pre) + "".join(suf)
                tried += 1
                try:
                    if self.verify_checksum(candidate):
                        return candidate
                except:
                    pass
                if tried >= max_combinations:
                    return None

        return None

    @staticmethod
    def verify_checksum(vin: str):
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

        checksum = 0
        for pos, char in enumerate(vin):
            if char.isdigit():
                value = int(char)
            elif char in trans:
                value = trans[char]
            else:
                return False  # Invalid character
            checksum += value * weights[pos]

        check_digit = "X" if (checksum % 11) == 10 else str(checksum % 11)
        return vin[8] == check_digit
