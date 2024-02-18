from nadlan.nadlan_utils import nominatim_addr , govmap_addr
import pytest
import httpx


def test_nominatim_addr_success_mock():
    mock_data = [{
        "address": {
            "city": "Test City",
            "suburb": "Test Suburb",
            "road": "Test Road",
            "postcode": "12345"
        },
        "type": "residential",
        "lat": "10.12345",
        "lon": "20.12345"
    }]

    def mock_response(request):
        return httpx.Response(200, json=mock_data)

    with httpx.Client(transport=httpx.MockTransport(mock_response)) as client:
        # Modify nominatim_addr to accept and use this client parameter
        result = nominatim_addr("Test Query", client)

        assert result['success'] == True
        assert result['city'] == "Test City"
        assert result['neighborhood'] == "Test Suburb"
        assert result['street'] == "Test Road"
        assert result['zip'] == "12345"
        assert result['type'] == "residential"
        assert result['lat'] == round(float("10.12345"), 5)
        assert result['long'] == round(float("20.12345"), 5)


@pytest.mark.parametrize('response_code', [400, 404, 500])
def test_nominatim_addr_failure(response_code):
    def mock_response(request):
        return httpx.Response(status_code=response_code)

    # Use the mock response with the MockTransport
    transport = httpx.MockTransport(mock_response)

    with httpx.Client(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError):
            # Call your function, it should now raise an HTTPStatusError due to the mocked response
            nominatim_addr("Invalid Query", client=client)

@pytest.mark.parametrize('json', ['', 'Invalid JSON Query'])
def test_nominatim_addr_invalid_json(json):
    def mock_response(request):
        return httpx.Response(200, json=json)

    with httpx.Client(transport=httpx.MockTransport(mock_response)) as client:
        result = nominatim_addr("Invalid JSON Query", client=client)

        assert result['success'] == False, "Function should indicate failure on empty response"
        assert result['neighborhood'] == None, "Neighborhood should be empty for empty response"
        assert result['zip'] == None, "Zip should be empty for empty response"
        assert result['lat'] == None, "Lat should be empty for empty response"
        assert result['long'] == None, "Long should be empty for empty response"
        assert result['type'] == None, "Type should be empty for empty response"

def test_nominatim_addr_partial_address_details():
    '''
    Simulate a response where some address details are missing to ensure the function correctly handles partial data.
    '''
    mock_data = [{
        "address": {
            "city": "Partial City",
            # Missing "suburb", "road", and "postcode"
        },
        "type": "residential",
        "lat": "11.12345",
        "lon": "21.12345"
    }]

    def mock_response(request):
        return httpx.Response(200, json=mock_data)

    with httpx.Client(transport=httpx.MockTransport(mock_response)) as client:
        result = nominatim_addr("Partial Details Query", client=client)

        assert result['success'] == True, "Function should succeed even with partial address details"
        assert result['city'] == "Partial City", "City should be populated with partial address details"
        assert result['neighborhood'] == "", "Missing details should default to empty strings"



def test_nominatim_api():
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': '1600 Amphitheatre Parkway, Mountain View, CA',
        'format': 'json',
        'addressdetails': 1
    }
    response = httpx.get(base_url, params=params , timeout=30)
    data = response.json()
    assert response.status_code == 200, "Expected status code 200"
    assert isinstance(data, list), "Response should be a list"
    assert len(data) > 0, "Response list should not be empty"
    assert 'address' in data[0], "First item in response should contain 'address'"
    assert 'city' in data[0]['address'] or 'town' in data[0]['address'], "Address should contain 'city' or 'town'"
    assert 'lat' in data[0] , "First item in response should contain 'lat'"
    assert 'lon' in data[0] , "First item in response should contain 'lon'"



def test_govmap_generic_error(monkeypatch):
    # Mock the requests.get method to raise a generic exception
    def mock_requests_get(url, **kwargs):
        raise Exception("Generic error occurred")

    monkeypatch.setattr("requests.get", mock_requests_get)

    result = govmap_addr("Invalid Address")
    assert result["success"] is False