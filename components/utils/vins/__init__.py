from pathlib import Path


def load_wmi_codes():
    wmi_data = {}
    filepath = Path(__file__).parent

    with open(f"{filepath}/wmi.data", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:  # Skip empty lines
                continue

            parts = line.split("|")
            if len(parts) >= 3:
                wmi = parts[0].strip()
                manufacturer = parts[1].strip()
                country = parts[2].strip()

                # Store as dict with manufacturer and country info
                wmi_data[wmi] = {"manufacturer": manufacturer, "country": country}

    return wmi_data


wmi_codes = load_wmi_codes()
