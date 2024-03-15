from .sql_reader_madlan import read_from_madlan_raw
import httpx
import math

def check_availability(headers):
    to_remove = []
    items_ids = read_from_madlan_raw(item_id=True)
    with httpx.Client(headers=headers) as client:
        client.timeout = httpx.Timeout(15, connect=60)
        for item_id in items_ids:
            link = f"https://www.madlan.co.il/listings/{item_id}"
            try:
                response = client.get(link, follow_redirects=True)
                url = response.url
                if str(item_id) not in str(url):
                    to_remove.append(item_id)
            except httpx.HTTPStatusError:
                print(httpx.HTTPStatusError)
                return []
    return to_remove



def min_point(x, y, df , target_distance= 50):
    distance = 10000
    neighborhood = None
    for index, row in df.iterrows():
        df_x = row['x']
        df_y = row['y']
        df_neighborhood = row['neighborhood']
        distance_check = math.sqrt(((df_x - x) ** 2) + ((df_y - y) ** 2))
        if distance_check < distance:
            if distance_check < target_distance:
                return df_neighborhood
            distance = distance_check
            neighborhood = df_neighborhood
    return neighborhood

payload = {
    "ObjectID": "5000",
    "CurrentLavel": 2,
    "PageNo": 1,
    "OrderByFilled": "DEALDATETIME",
    "OrderByDescending": True,
}

cookies = {
    'APP_CTX_USER_ID': 'a123e7c7-66fd-4c46-b286-eb7e43d52e38',
    'Infinite_user_id_key': 'a123e7c7-66fd-4c46-b286-eb7e43d52e38',
    'G_ENABLED_IDPS': 'google',
    'Infinite_ab_tests_context_v2_key': '{%22context%22:{%22_be_sortMarketplaceByDate%22:%22modeA%22%2C%22_be_sortMarketplaceAgeWeight%22:%22modeA%22%2C%22uploadRangeFilter%22:%22modeA%22%2C%22mapLayersV1%22:%22modeB%22%2C%22tabuViewMode%22:%22modeA%22%2C%22homepageSearch%22:%22modeA%22%2C%22removeWizard%22:%22modeB%22%2C%22whatsAppPoc%22:%22modeB%22%2C%22_be_addLastUpdateToWeights%22:%22modeB%22%2C%22quickFilters%22:%22modeA%22%2C%22projectPageNewLayout%22:%22modeB%22}}',
    'USER_TOKEN_V2': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNjkzOTEyNzc0NjEwLCJwYXlsb2FkIjoie1widWlkXCI6XCJkNDg5YWE2Zi01MDQ5LTQxMzAtOTU0Yi04ZWI3ZTZhMzRjNDJcIixcInNlc3Npb24taWRcIjpcIjJhZWM0ZGVhLWQ2ZTgtNGUwNS05YzUzLWUzOTUyZmY3OGRkN1wiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNjkzOTEyNzc0LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImQ0ODlhYTZmLTUwNDktNDEzMC05NTRiLThlYjdlNmEzNGM0MiIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiIyYWVjNGRlYS1kNmU4LTRlMDUtOWM1My1lMzk1MmZmNzhkZDciLCJ2IjoyLCJleHAiOjE3NTcwMjc5NzR9.VotKqMoFvOetPjvbtpXlfJdtRmrA7wPpV7fqlC8JC74',
    'PA_STORAGE_SESSION_KEY': '{%22marketPlaceDialog%22:{%22expiredDate%22:1693999364192%2C%22closeClickCount%22:2}%2C%22marketPlaceBanner%22:{%22expiredDate%22:1679688327477%2C%22closeClickCount%22:1}}',
    'MORTGAGE_STORAGE_SESSION_KEY': '{%22closeClickCount%22:0%2C%22time%22:null%2C%22sessionStartMs%22:1693912773463%2C%22hideElements%22:false%2C%22lastShownPopupOnListingPage%22:%22mortgage_popup%22%2C%22listingPagePopupShownAtMs%22:1693919946929%2C%22popupOpenCount%22:1}',
    'APP_CTX_SESSION_ID': '28c980dc-e695-4c6e-8974-54ba3dec9c60',
    'g_state': '{"i_l":2,"i_p":1694069673816}',
    '_sp_ses.549d': '*',
    'AWSALB': 'RBoIUI9A5zTMy8owT0PTfyyQVdVcdWziNE5l8DChnvekJJzQCeCEYSI2pEKlEFUwpkZiVwRIROebsMsKIUAqroTfTKdZyLF7n8rJ+lIV9so5UVKF+KASlsWWamMb',
    '_ud': 'e5e30c9d3f4fa07ff253e581a1332b8a88bd8de1-cid=93b255cf-c6cb-4747-af1a-e159b5a30449&_ts=1693984674278',
    '_sp_id.549d': '065d6cdd-43ee-42e4-aa08-b131f450b231.1693913431.2.1693984679.1693914698.00a8ce71-f3c1-42ae-bced-202ea803ffe5',
    'WINDOW_WIDTH': '1075',
    '_pxhd': 'jUtmbGaZxN8SdV0whjtJsE9e-LKDC/JYU42jxFi6v52gvWlse9yKPCoGmvHk0MRfm6cKsD1gcdLhwwU3StEZKA==:vKJHWREy1wPg4LAD0NnaornHxpl1WgD7sGHxfr410FudPlb-ykkAPl0Q9Oal9h635/4qI0npPrn8-JvfiOJDL4smJPT5eGPgOo7Cs9HeFn0=',
}

headers = {
    'authority': 'www.madlan.co.il',
    'accept': '*/*',
    'accept-language': 'he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNjkzOTEyNzc0NjEwLCJwYXlsb2FkIjoie1widWlkXCI6XCJkNDg5YWE2Zi01MDQ5LTQxMzAtOTU0Yi04ZWI3ZTZhMzRjNDJcIixcInNlc3Npb24taWRcIjpcIjJhZWM0ZGVhLWQ2ZTgtNGUwNS05YzUzLWUzOTUyZmY3OGRkN1wiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNjkzOTEyNzc0LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImQ0ODlhYTZmLTUwNDktNDEzMC05NTRiLThlYjdlNmEzNGM0MiIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiIyYWVjNGRlYS1kNmU4LTRlMDUtOWM1My1lMzk1MmZmNzhkZDciLCJ2IjoyLCJleHAiOjE3NTcwMjc5NzR9.VotKqMoFvOetPjvbtpXlfJdtRmrA7wPpV7fqlC8JC74',
    'content-type': 'application/json',
    # 'cookie': 'APP_CTX_USER_ID=a123e7c7-66fd-4c46-b286-eb7e43d52e38; Infinite_user_id_key=a123e7c7-66fd-4c46-b286-eb7e43d52e38; Infinite_user_id_key=a123e7c7-66fd-4c46-b286-eb7e43d52e38; G_ENABLED_IDPS=google; Infinite_ab_tests_context_v2_key={%22context%22:{%22_be_sortMarketplaceByDate%22:%22modeA%22%2C%22_be_sortMarketplaceAgeWeight%22:%22modeA%22%2C%22uploadRangeFilter%22:%22modeA%22%2C%22mapLayersV1%22:%22modeB%22%2C%22tabuViewMode%22:%22modeA%22%2C%22homepageSearch%22:%22modeA%22%2C%22removeWizard%22:%22modeB%22%2C%22whatsAppPoc%22:%22modeB%22%2C%22_be_addLastUpdateToWeights%22:%22modeB%22%2C%22quickFilters%22:%22modeA%22%2C%22projectPageNewLayout%22:%22modeB%22}}; USER_TOKEN_V2=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNjkzOTEyNzc0NjEwLCJwYXlsb2FkIjoie1widWlkXCI6XCJkNDg5YWE2Zi01MDQ5LTQxMzAtOTU0Yi04ZWI3ZTZhMzRjNDJcIixcInNlc3Npb24taWRcIjpcIjJhZWM0ZGVhLWQ2ZTgtNGUwNS05YzUzLWUzOTUyZmY3OGRkN1wiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNjkzOTEyNzc0LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImQ0ODlhYTZmLTUwNDktNDEzMC05NTRiLThlYjdlNmEzNGM0MiIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiIyYWVjNGRlYS1kNmU4LTRlMDUtOWM1My1lMzk1MmZmNzhkZDciLCJ2IjoyLCJleHAiOjE3NTcwMjc5NzR9.VotKqMoFvOetPjvbtpXlfJdtRmrA7wPpV7fqlC8JC74; PA_STORAGE_SESSION_KEY={%22marketPlaceDialog%22:{%22expiredDate%22:1693999364192%2C%22closeClickCount%22:2}%2C%22marketPlaceBanner%22:{%22expiredDate%22:1679688327477%2C%22closeClickCount%22:1}}; MORTGAGE_STORAGE_SESSION_KEY={%22closeClickCount%22:0%2C%22time%22:null%2C%22sessionStartMs%22:1693912773463%2C%22hideElements%22:false%2C%22lastShownPopupOnListingPage%22:%22mortgage_popup%22%2C%22listingPagePopupShownAtMs%22:1693919946929%2C%22popupOpenCount%22:1}; APP_CTX_SESSION_ID=28c980dc-e695-4c6e-8974-54ba3dec9c60; g_state={"i_l":2,"i_p":1694069673816}; _sp_ses.549d=*; AWSALB=RBoIUI9A5zTMy8owT0PTfyyQVdVcdWziNE5l8DChnvekJJzQCeCEYSI2pEKlEFUwpkZiVwRIROebsMsKIUAqroTfTKdZyLF7n8rJ+lIV9so5UVKF+KASlsWWamMb; _ud=e5e30c9d3f4fa07ff253e581a1332b8a88bd8de1-cid=93b255cf-c6cb-4747-af1a-e159b5a30449&_ts=1693984674278; _sp_id.549d=065d6cdd-43ee-42e4-aa08-b131f450b231.1693913431.2.1693984679.1693914698.00a8ce71-f3c1-42ae-bced-202ea803ffe5; WINDOW_WIDTH=1075; _pxhd=jUtmbGaZxN8SdV0whjtJsE9e-LKDC/JYU42jxFi6v52gvWlse9yKPCoGmvHk0MRfm6cKsD1gcdLhwwU3StEZKA==:vKJHWREy1wPg4LAD0NnaornHxpl1WgD7sGHxfr410FudPlb-ykkAPl0Q9Oal9h635/4qI0npPrn8-JvfiOJDL4smJPT5eGPgOo7Cs9HeFn0=',
    'origin': 'https://www.madlan.co.il',
    'referer': 'https://www.madlan.co.il/listings/7hIaD4oqflJ',
    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}



offset_value = 0
limit_value = 50
json_data = {
    'operationName': 'searchPoi',
    'variables': {
        'noFee': False,
        'dealType': 'unitBuy',
        'numberOfEmployeesRange': [
            None,
            None,
        ],
        'commercialAmenities': {},
        'qualityClass': [],
        'roomsRange': [
            None,
            None,
        ],
        'bathsRange': [
            None,
            None,
        ],
        'floorRange': [
            None,
            None,
        ],
        'areaRange': [
            None,
            None,
        ],
        'buildingClass': [],
        'sellerType': [],
        'generalCondition': [],
        'ppmRange': [
            None,
            None,
        ],
        'priceRange': [
            None,
            None,
        ],
        'monthlyTaxRange': [
            None,
            None,
        ],
        'amenities': {},
        'sort': [
            {
                'field': 'geometry',
                'order': 'asc',
                'reference': None,
                'docIds': [
                ],
            },
        ],
        'priceDrop': False,
        'underPriceEstimation': False,
        'isCommercialRealEstate': False,
        'userContext': None,
        'poiTypes': [
            'bulletin',
        ],
        'searchContext': 'marketplace',
        'cursor': {
            'seenProjects': None,
            'bulletinsOffset': 0,
        },
        'offset': 0,
        'limit': 59,
        'abtests': {
            '_be_sortMarketplaceByDate': 'modeA',
            '_be_sortMarketplaceAgeWeight': 'modeA',
            '_be_addLastUpdateToWeights': 'modeB',
        },
    },
    'query': 'query searchPoi($dealType: String, $userContext: JSONObject, $abtests: JSONObject, $noFee: Boolean, $priceRange: [Int], $ppmRange: [Int], $monthlyTaxRange: [Int], $roomsRange: [Int], $bathsRange: [Float], $buildingClass: [String], $amenities: inputAmenitiesFilter, $generalCondition: [GeneralCondition], $sellerType: [SellerType], $floorRange: [Int], $areaRange: [Int], $tileRanges: [TileRange], $tileRangesExcl: [TileRange], $sort: [SortField], $limit: Int, $offset: Int, $cursor: inputCursor, $poiTypes: [PoiType], $locationDocId: String, $abtests: JSONObject, $searchContext: SearchContext, $underPriceEstimation: Boolean, $priceDrop: Boolean, $isCommercialRealEstate: Boolean, $commercialAmenities: inputCommercialAmenitiesFilter, $qualityClass: [String], $numberOfEmployeesRange: [Float], $creationDaysRange: Int) {\n  searchPoiV2(noFee: $noFee, dealType: $dealType, userContext: $userContext, abtests: $abtests, priceRange: $priceRange, ppmRange: $ppmRange, monthlyTaxRange: $monthlyTaxRange, roomsRange: $roomsRange, bathsRange: $bathsRange, buildingClass: $buildingClass, sellerType: $sellerType, floorRange: $floorRange, areaRange: $areaRange, generalCondition: $generalCondition, amenities: $amenities, tileRanges: $tileRanges, tileRangesExcl: $tileRangesExcl, sort: $sort, limit: $limit, offset: $offset, cursor: $cursor, poiTypes: $poiTypes, locationDocId: $locationDocId, abtests: $abtests, searchContext: $searchContext, underPriceEstimation: $underPriceEstimation, priceDrop: $priceDrop, isCommercialRealEstate: $isCommercialRealEstate, commercialAmenities: $commercialAmenities, qualityClass: $qualityClass, numberOfEmployeesRange: $numberOfEmployeesRange, creationDaysRange: $creationDaysRange) {\n    total\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      seenProjects\n      __typename\n    }\n    totalNearby\n    lastInGeometryId\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      __typename\n    }\n    ...PoiFragment\n    __typename\n  }\n}\n\nfragment PoiFragment on PoiSearchResult {\n  poi {\n    ...PoiInner\n    ... on Bulletin {\n      rentalBrokerFee\n      eventsHistory {\n        eventType\n        price\n        date\n        __typename\n      }\n      insights {\n        insights {\n          category\n          tradeoff {\n            insightPlace\n            value\n            tagLine\n            impactful\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment PoiInner on Poi {\n  id\n  locationPoint {\n    lat\n    lng\n    __typename\n  }\n  type\n  firstTimeSeen\n  addressDetails {\n    docId\n    city\n    borough\n    zipcode\n    streetName\n    neighbourhood\n    neighbourhoodDocId\n    cityDocId\n    resolutionPreferences\n    streetNumber\n    unitNumber\n    district\n    __typename\n  }\n  ... on Project {\n    discount {\n      showDiscount\n      description\n      bannerUrl\n      __typename\n    }\n    dealType\n    apartmentType {\n      size\n      beds\n      apartmentSpecification\n      type\n      price\n      __typename\n    }\n    bedsRange {\n      min\n      max\n      __typename\n    }\n    priceRange {\n      min\n      max\n      __typename\n    }\n    images {\n      path\n      __typename\n    }\n    promotionStatus {\n      status\n      __typename\n    }\n    projectName\n    projectLogo\n    isCommercial\n    projectMessages {\n      listingDescription\n      __typename\n    }\n    previewImage {\n      path\n      __typename\n    }\n    developers {\n      id\n      logoPath\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    buildingStage\n    blockDetails {\n      buildingsNum\n      floorRange {\n        min\n        max\n        __typename\n      }\n      units\n      mishtakenPrice\n      urbanRenewal\n      __typename\n    }\n    __typename\n  }\n  ... on CommercialBulletin {\n    address\n    agentId\n    qualityClass\n    amenities {\n      accessible\n      airConditioner\n      alarm\n      conferenceRoom\n      doorman\n      elevator\n      fullTimeAccess\n      kitchenette\n      outerSpace\n      parkingBikes\n      parkingEmployee\n      parkingVisitors\n      reception\n      secureRoom\n      storage\n      subDivisible\n      __typename\n    }\n    area\n    availabilityType\n    availableDate\n    balconyArea\n    buildingClass\n    buildingType\n    buildingYear\n    currency\n    dealType\n    description\n    estimatedPrice\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    feeType\n    floor\n    floors\n    fromDateTime\n    furnitureDetails\n    generalCondition\n    images {\n      ...ImageItem\n      __typename\n    }\n    lastActiveMarkDate\n    leaseTerm\n    leaseType\n    matchScore\n    monthlyTaxes\n    newListing\n    numberOfEmployees\n    originalId\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ppm\n    price\n    qualityBin\n    rentalBrokerFee\n    rooms\n    source\n    status {\n      promoted\n      __typename\n    }\n    url\n    virtualTours\n    __typename\n  }\n  ... on Bulletin {\n    dealType\n    address\n    matchScore\n    beds\n    floor\n    baths\n    buildingYear\n    area\n    price\n    virtualTours\n    rentalBrokerFee\n    generalCondition\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    status {\n      promoted\n      __typename\n    }\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    commuteTime\n    dogsParkWalkTime\n    parkWalkTime\n    buildingClass\n    images {\n      ...ImageItem\n      __typename\n    }\n    __typename\n  }\n  ... on Ad {\n    addressDetails {\n      docId\n      city\n      borough\n      zipcode\n      streetName\n      neighbourhood\n      neighbourhoodDocId\n      resolutionPreferences\n      streetNumber\n      unitNumber\n      __typename\n    }\n    city\n    district\n    firstTimeSeen\n    id\n    locationPoint {\n      lat\n      lng\n      __typename\n    }\n    neighbourhood\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment ImageItem on ImageItem {\n  description\n  imageUrl\n  isFloorplan\n  rotation\n  __typename\n}\n',
}

''' 
Here you should add you cookies and headers to avoid from recaptcha.
You can use this site: https://curlconverter.com/
'''


city_code = {
    'תל אביב': 5000,
    'רמת גן': 8600,
    'גבעתיים': 6300,
    'ירושלים': 3000,
    'פתח תקווה': 7900,
    'חולון': 6600,
    'הרצליה': 6400,
    'רעננה': 8700,
    'בת ים': 6200,
    'בני ברק': 6100,
    'כפר סבא': 6900,
    'רמת השרון': 2650,
    'ראשון לציון': 8300,
    'חיפה': 4000,
    'אשדוד': 70,
    'נתניה': 7400,
    'באר שבע': 9000
}

headers_delete = {
    'authority': 'www.madlan.co.il',
    'accept': '*/*',
    'accept-language': 'he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNjkzOTEyNzc0NjEwLCJwYXlsb2FkIjoie1widWlkXCI6XCJkNDg5YWE2Zi01MDQ5LTQxMzAtOTU0Yi04ZWI3ZTZhMzRjNDJcIixcInNlc3Npb24taWRcIjpcIjJhZWM0ZGVhLWQ2ZTgtNGUwNS05YzUzLWUzOTUyZmY3OGRkN1wiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNjkzOTEyNzc0LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImQ0ODlhYTZmLTUwNDktNDEzMC05NTRiLThlYjdlNmEzNGM0MiIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiIyYWVjNGRlYS1kNmU4LTRlMDUtOWM1My1lMzk1MmZmNzhkZDciLCJ2IjoyLCJleHAiOjE3NTcwMjc5NzR9.VotKqMoFvOetPjvbtpXlfJdtRmrA7wPpV7fqlC8JC74',
    'content-type': 'application/json',
    # 'cookie': 'APP_CTX_USER_ID=a123e7c7-66fd-4c46-b286-eb7e43d52e38; Infinite_user_id_key=a123e7c7-66fd-4c46-b286-eb7e43d52e38; Infinite_user_id_key=a123e7c7-66fd-4c46-b286-eb7e43d52e38; G_ENABLED_IDPS=google; USER_TOKEN_V2=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNjkzOTEyNzc0NjEwLCJwYXlsb2FkIjoie1widWlkXCI6XCJkNDg5YWE2Zi01MDQ5LTQxMzAtOTU0Yi04ZWI3ZTZhMzRjNDJcIixcInNlc3Npb24taWRcIjpcIjJhZWM0ZGVhLWQ2ZTgtNGUwNS05YzUzLWUzOTUyZmY3OGRkN1wiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNjkzOTEyNzc0LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImQ0ODlhYTZmLTUwNDktNDEzMC05NTRiLThlYjdlNmEzNGM0MiIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiIyYWVjNGRlYS1kNmU4LTRlMDUtOWM1My1lMzk1MmZmNzhkZDciLCJ2IjoyLCJleHAiOjE3NTcwMjc5NzR9.VotKqMoFvOetPjvbtpXlfJdtRmrA7wPpV7fqlC8JC74; PA_STORAGE_SESSION_KEY={%22marketPlaceDialog%22:{%22expiredDate%22:1693999364192%2C%22closeClickCount%22:2}%2C%22marketPlaceBanner%22:{%22expiredDate%22:1679688327477%2C%22closeClickCount%22:1}}; _pxvid=ceadad7c-4c86-11ee-8882-3c0a9fe2d168; g_state={"i_l":3,"i_p":1695289301462}; _ud=abe70c5fe9f5fd842ea8e9049382599b39321117-cid=93b255cf-c6cb-4747-af1a-e159b5a30449&_ts=1695547087749; _sp_id.549d=065d6cdd-43ee-42e4-aa08-b131f450b231.1693913431.4.1695547089.1695127586.0351ba0a-0117-4111-babe-c07b111c9ce8; _pxhd=sH9roXQVLOicU3a7NUGzlH2W-4l20CZS7sPJHuKOVUnCNsA/Jp9bF-0MOdW2KTjX3y4K1wWDSzzHJeASPxMryQ==:0oOg2TKKuzFrk0ZdmUFKjy4YjlWMmWiEs6h/hMR1cF1yRyn1JsV3w2YMqxgmEJ-hD3TASYauhnnxVGtp5EP7gTnnLzvXKU2n/oJrQsvFuMU=; AWSALB=qmBRHqD7nxeZILhCzG2Milv6ReVbahY78+jqRl40zkJw7rOgDUQHul+JbAxHu4RUkbQO0axEHK9gVixdDaoyZusnycpIPSLUqY8qSXZsmJ+i9DY8tunU+7cYbVoF; APP_CTX_SESSION_ID=6c764566-1c5b-419d-95d7-93f3f13294d0; Infinite_ab_tests_context_v2_key={%22context%22:{%22searchResultsV3%22:%22modeA%22%2C%22whatsappSticky%22:%22modeA%22%2C%22_be_sortMarketplaceByDate%22:%22modeA%22%2C%22_be_sortMarketplaceAgeWeight%22:%22modeA%22%2C%22uploadRangeFilter%22:%22modeA%22%2C%22homepageSearch%22:%22modeA%22%2C%22removeWizard%22:%22modeB%22%2C%22whatsAppPoc%22:%22modeB%22%2C%22_be_addLastUpdateToWeights%22:%22modeB%22%2C%22quickFilters%22:%22modeA%22}}; MORTGAGE_STORAGE_SESSION_KEY={%22closeClickCount%22:0%2C%22time%22:null%2C%22popupOpenCount%22:0%2C%22sessionStartMs%22:1703760266730%2C%22hideElements%22:false%2C%22lastShownPopupOnListingPage%22:%22mortgage_popup%22%2C%22listingPagePopupShownAtMs%22:1693919946929}; WINDOW_WIDTH=1150',
    'origin': 'https://www.madlan.co.il',
    'referer': 'https://www.madlan.co.il/listings/gTmc5WQxehH',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}
