import requests
import json
import re
from bs4 import BeautifulSoup as bs
import csv
import os
from concurrent.futures import ThreadPoolExecutor


output_file = 'suedtirol_data.csv'
sorting_by = "accommodations_prod_en_trustY"
threads = 30
temp_db = []


def saveData(dataset):
    with open(output_file, mode='a+', encoding='utf-8-sig', newline='') as csvFile:
        fieldnames = ["Website", "Name", "Category", "Address",
                      "Village", "Region", "Email", "Booking link", "Phone number"]
        writer = csv.DictWriter(csvFile, fieldnames=fieldnames,
                                delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if os.stat(output_file).st_size == 0:
            writer.writeheader()
        writer.writerow({
            "Website": dataset[0],
            "Name": dataset[1],
            "Category": dataset[2],
            "Address": dataset[3],
            "Village": dataset[4],
            "Region": dataset[5],
            "Email": dataset[6],
            "Booking link": dataset[7],
            "Phone number": dataset[8]
        })


def checkTotalRecordsCount(region_id):
    print("Checking page record count")
    link = 'https://41yd8r4jkj-1.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.62.0)%3B%20JS%20Helper%20(3.16.0)&x-algolia-api-key=e4f581594e5150b5cc36909d14183498&x-algolia-application-id=41YD8R4JKJ'
    headers = {
        'content-type': 'application/json',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    }
    page_no = 0
    data = {
        "requests": [
            {
                "indexName": sorting_by,
                "params": "facetFilters=%5B%5B%22purgedCat%3A%22%5D%5D&facets=%5B%22accessibilityTags%22%2C%22boardsKeys%22%2C%22facilityTags%22%2C%22foodTags%22%2C%22guestCardFlag%22%2C%22hasApartment%22%2C%22hgvId%22%2C%22inspirationTags%22%2C%22purgedCat%22%2C%22sustainableLevel%22%2C%22trustYCategory%22%2C%22type%22%5D&filters=(regionId%3A'" + region_id + "')&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=30&maxValuesPerFacet=100&optionalFilters=%5B%22randomValue%3A50%22%2C%22randomValue%3A21%22%2C%22randomValue%3A35%22%2C%22randomValue%3A1%22%2C%22randomValue%3A38%22%2C%22randomValue%3A25%22%2C%22randomValue%3A97%22%2C%22randomValue%3A19%22%2C%22randomValue%3A73%22%2C%22randomValue%3A79%22%5D&page=" + str(page_no) + "&tagFilters=&userToken=anonymous-7175246c-5666-43af-890f-8062e9f68999"
            },
            {
                "indexName": sorting_by,
                "params": "analytics=false&clickAnalytics=false&facets=purgedCat&filters=(regionId%3A'" + region_id + "')&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=100&optionalFilters=%5B%22randomValue%3A50%22%2C%22randomValue%3A21%22%2C%22randomValue%3A35%22%2C%22randomValue%3A1%22%2C%22randomValue%3A38%22%2C%22randomValue%3A25%22%2C%22randomValue%3A97%22%2C%22randomValue%3A19%22%2C%22randomValue%3A73%22%2C%22randomValue%3A79%22%5D&page=" + str(page_no) + "&userToken=anonymous-7175246c-5666-43af-890f-8062e9f68999"
            }
        ]
    }

    loaded = False
    for i in range(3):

        try:
            resp = requests.post(link, headers=headers,
                                 data=json.dumps(data), timeout=30).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return 0

    return resp.get('results')[0].get('nbHits'), resp


def scrapeSuedtirol(region_id):
    total_records, resp = checkTotalRecordsCount(region_id)
    print("Total records: {}".format(total_records))
    if total_records <= 1000:
        print("Less than 1000 records, no sub category filtering will be applied")
        processContent(resp_old=resp)
    else:
        print("More than 1000 records, sub category filtering will be applied")
        for i in range(1, 6):
            print("Processing category of {} star(s)".format(i))
            processContent(category_number=i)


def processContent(category_number=None, resp_old=None):
    global temp_db
    link = 'https://41yd8r4jkj-1.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.62.0)%3B%20JS%20Helper%20(3.16.0)&x-algolia-api-key=e4f581594e5150b5cc36909d14183498&x-algolia-application-id=41YD8R4JKJ'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,bn;q=0.8',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Host': '41yd8r4jkj-1.algolianet.com',
        'Origin': 'https://www.suedtirol.info',
        'Referer': 'https://www.suedtirol.info/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    page_no = 0
    while True:
        if category_number:
            data = {
                "requests": [
                    {
                        "indexName": sorting_by,
                        "params": "facetFilters=%5B%5B%22purgedCat%3A" + str(category_number) + "%22%5D%5D&facets=%5B%22accessibilityTags%22%2C%22boardsKeys%22%2C%22facilityTags%22%2C%22foodTags%22%2C%22guestCardFlag%22%2C%22hasApartment%22%2C%22hgvId%22%2C%22inspirationTags%22%2C%22purgedCat%22%2C%22sustainableLevel%22%2C%22trustYCategory%22%2C%22type%22%5D&filters=(regionId%3A'" + region_id + "')&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=30&maxValuesPerFacet=100&optionalFilters=%5B%22randomValue%3A50%22%2C%22randomValue%3A21%22%2C%22randomValue%3A35%22%2C%22randomValue%3A1%22%2C%22randomValue%3A38%22%2C%22randomValue%3A25%22%2C%22randomValue%3A97%22%2C%22randomValue%3A19%22%2C%22randomValue%3A73%22%2C%22randomValue%3A79%22%5D&page=" + str(page_no) + "&tagFilters=&userToken=anonymous-7175246c-5666-43af-890f-8062e9f68999"
                    },
                    {
                        "indexName": sorting_by,
                        "params": "analytics=false&clickAnalytics=false&facets=purgedCat&filters=(regionId%3A'" + region_id + "')&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=100&optionalFilters=%5B%22randomValue%3A50%22%2C%22randomValue%3A21%22%2C%22randomValue%3A35%22%2C%22randomValue%3A1%22%2C%22randomValue%3A38%22%2C%22randomValue%3A25%22%2C%22randomValue%3A97%22%2C%22randomValue%3A19%22%2C%22randomValue%3A73%22%2C%22randomValue%3A79%22%5D&page=" + str(page_no) + "&userToken=anonymous-7175246c-5666-43af-890f-8062e9f68999"
                    }
                ]
            }
        else:
            data = {
                "requests": [
                    {
                        "indexName": sorting_by,
                        "params": "facetFilters=%5B%5B%22purgedCat%3A%22%5D%5D&facets=%5B%22accessibilityTags%22%2C%22boardsKeys%22%2C%22facilityTags%22%2C%22foodTags%22%2C%22guestCardFlag%22%2C%22hasApartment%22%2C%22hgvId%22%2C%22inspirationTags%22%2C%22purgedCat%22%2C%22sustainableLevel%22%2C%22trustYCategory%22%2C%22type%22%5D&filters=(regionId%3A'" + region_id + "')&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=30&maxValuesPerFacet=100&optionalFilters=%5B%22randomValue%3A50%22%2C%22randomValue%3A21%22%2C%22randomValue%3A35%22%2C%22randomValue%3A1%22%2C%22randomValue%3A38%22%2C%22randomValue%3A25%22%2C%22randomValue%3A97%22%2C%22randomValue%3A19%22%2C%22randomValue%3A73%22%2C%22randomValue%3A79%22%5D&page=" + str(page_no) + "&tagFilters=&userToken=anonymous-7175246c-5666-43af-890f-8062e9f68999"
                    },
                    {
                        "indexName": sorting_by,
                        "params": "analytics=false&clickAnalytics=false&facets=purgedCat&filters=(regionId%3A'" + region_id + "')&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=100&optionalFilters=%5B%22randomValue%3A50%22%2C%22randomValue%3A21%22%2C%22randomValue%3A35%22%2C%22randomValue%3A1%22%2C%22randomValue%3A38%22%2C%22randomValue%3A25%22%2C%22randomValue%3A97%22%2C%22randomValue%3A19%22%2C%22randomValue%3A73%22%2C%22randomValue%3A79%22%5D&page=" + str(page_no) + "&userToken=anonymous-7175246c-5666-43af-890f-8062e9f68999"
                    }
                ]
            }
        if not resp_old:
            try:
                resp = requests.post(link, headers=headers,
                                     data=json.dumps(data), timeout=30).json()
            except:
                print("Failed to open {}".format(link))
                continue
        else:
            resp = resp_old
            resp_old = None
        records = resp.get('results')[0].get('hits')
        print("Records available: {}".format(len(records)))
        records_ordered = []
        for idx, record in enumerate(records):
            details_link = record.get('pdpOnLinePath')
            hotel_type = record.get('type')
            print(details_link)
            records_ordered.append((idx, details_link, hotel_type))
        with ThreadPoolExecutor(max_workers=min(threads, len(records_ordered))) as executor:
            executor.map(getProductDetails, records_ordered)
        temp_db = sorted(temp_db, key=lambda x: x[0])
        for idx, dataset in temp_db:
            saveData(dataset)
        temp_db.clear()
        if resp.get('results')[0].get('page', 0) + 1 == resp.get('results')[0].get('nbPages'):
            break
        page_no += 1


def accessOriginaPage(link):
    print("Getting data under {}".format(link))
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9,bn;q=0.8',
        'cache-control': 'max-age=0',
        'dnt': '1',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    loaded = False
    for i in range(3):
        try:
            resp = requests.get(link, headers=headers, timeout=30).text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return None
    region_id = re.findall(r"regionId:'(.+?)'", resp)[0]
    return region_id


def getProductDetails(bucket):
    global temp_db
    idx, link, hotel_type = bucket
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9,bn;q=0.8',
        'cache-control': 'max-age=0',
        'dnt': '1',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    loaded = False
    for i in range(3):
        try:
            resp = requests.get(link, headers=headers, timeout=30).text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return
    soup = bs(resp, 'html.parser')
    accom_data = soup.find('div', id='card-accommodation')
    try:
        website = accom_data.get('data-web')
    except:
        website = ''
    try:
        name = accom_data.get('data-accommodation-name')
    except:
        name = ''
    try:
        script_json = json.loads(
            soup.find('script', id='seo-accommodation-script').text.strip())
    except:
        script_json = {}
    hotel_category = hotel_type
    address = script_json.get('address', {}).get('addressLocality')
    village = script_json.get('address', {}).get('streetAddress')
    region = script_json.get('address', {}).get('addressRegion')
    email = script_json.get('email')
    phone = script_json.get('telephone')
    print("Website: {}".format(website))
    print("Name: {}".format(name))
    print("Category: {}".format(hotel_category))
    print("Address: {}".format(address))
    print("Village: {}".format(village))
    print("Region: {}".format(region))
    print("Email: {}".format(email))
    print("Booking link: {}".format(link))
    print("Phone: {}".format(phone))
    dataset = [website, name, hotel_category, address, village,
               region, email, link, phone]
    temp_db.append((idx, dataset))


if __name__ == "__main__":
    main_links = open('links.txt', mode='r', encoding='utf-8').read().split('\n')
    for link in main_links:
        if not link.strip():
            continue
        region_id = accessOriginaPage(link)
        scrapeSuedtirol(region_id)
    # getProductDetails('https://www.suedtirol.info/en/en/accommodation/pdp-accommodation.A800CB8FA0FC03C82162F9AAEBA64E28.garni-zimmerhofer.campo-tures-sand-in-taufers')
