from cards.address import normalize_address, normalize_state, normalize_zip, validate_and_normalize_address


def test_address_abbreviation_expansion():
    result = validate_and_normalize_address("123 main st", "los angeles", "ca", "90001")
    assert result.address == "123 Main Street"
    assert result.city == "Los Angeles"
    assert result.state == "CA"


def test_state_name_conversion():
    state, reasons = normalize_state("California")
    assert state == "CA"
    assert "state_name_converted" in reasons


def test_zip_parsing_with_plus4():
    zip5, zip4, reasons = normalize_zip("94107-1234")
    assert zip5 == "94107"
    assert zip4 == "1234"
    assert reasons == tuple()


def test_infer_city_state_from_zip_missing_city():
    result = validate_and_normalize_address("1711 245th st", "", "", "90717")
    assert result.city == "Lomita"
    assert result.state == "CA"
    assert "inferred_city_state_from_zip" in result.reasons
