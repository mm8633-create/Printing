from cards.header_mapping import HeaderMapper, map_headers


def test_header_mapping_auto_detection():
    headers = [
        "patient_full_name",
        "clinic",
        "address1",
        "city_name",
        "State",
        "zip_code",
        "Appointment",
        "profile_url",
    ]
    result = map_headers(headers, "new_visit")
    assert result.mapping["name"] == "patient_full_name"
    assert result.mapping["clinic_name"] == "clinic"
    assert result.mapping["address"] == "address1"
    assert result.mapping["date_time"] == "Appointment"
    assert "zip" in result.mapping


def test_header_mapper_missing_required():
    headers = ["name", "address"]
    result = map_headers(headers, "reprint")
    assert "clinic_name" in result.missing
    assert "date_time" in result.missing
