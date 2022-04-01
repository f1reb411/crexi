import asyncio
import csv
import re
import time

import aiohttp


start_time = time.time()


async def get_properties_urls():
    headers = {
        'authority': 'api.crexi.com',
        'client-timezone-offset': '3',
        'mixpanel-distinct-id': '17f51a7f0321188-034467d69eeb45-192b1e05-1fa400-17f51a7f03312f0',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/99.0.4844.51 Safari/537.36',
        'accept': 'application/json, text/plain, */*',
        'sec-ch-ua-platform': '"Linux"',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
        'origin': 'https://www.crexi.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.crexi.com/',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    json_data = {
        'types': [
            'Multifamily',
        ],
        'latitudeMax': None,
        'latitudeMin': None,
        'longitudeMax': None,
        'longitudeMin': None,
        'count': 100,
        'mlScenario': 'search-properties',
        'offset': 0,
        'userId': '17f51a7f0321188-034467d69eeb45-192b1e05-1fa400-17f51a7f03312f0',
        'sortDirection': 'Descending',
        'sortOrder': 'Rank',
        'brokerIds': [],
    }

    async with aiohttp.ClientSession() as session:
        tasks = []
        while True:
            response = await session.post('https://api.crexi.com/assets/search', headers=headers, json=json_data)
            properties_json = await response.json()
            if 'Data' in properties_json:
                for home in properties_json['Data']:
                    url = f'https://api.crexi.com/assets/{str(home["Id"])}'
                    task = asyncio.create_task(get_property_data(session, url))
                    tasks.append(task)
                json_data['offset'] += 100
                await asyncio.gather(*tasks)
            else:
                break


async def get_property_data(session, url):
    header = {
        'authority': 'api.crexi.com',
        'client-timezone-offset': '3',
        'accept': 'application/json, text/plain, */*',
        'authorization': 'Bearer CfDJ8BWNsBdynz9Nq28ACYuM-CJRBjtR11oqs26XQfLWTLvxXc_wlU-g5xstnj94eOXliArUleSZ_'
                         'Mau4r_exsJLFt68ABenOo6ipXDFZBgSeXjKVg8InBzf_oYwLrzQlPUqc7TgDyCR6VGwpxY8ex23-iG8TFWeu'
                         'Hwyn2ZwUnkcH_bYh-McKctzld_zKmPse7ff81zt7i46vrewexy5pwsJiVc0IaArBd3tHq_thjiMNDYDTrt7d'
                         'lJo4H_DFmyYYWY2DhWg4zgKV_wmBJREvLza1owX_wGNI2FUOSGMvP5Szk6mOngnjOLNzeThiEvEnqkfJl-iC'
                         'ubw7I7NyVi1k_V2Hzz-mtvUXCC4sqKs1FQasSlVXTd2D0ZR8NvndMhRloFjEESfeCFrmNzbE2BPEuu8CWI',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/99.0.4844.51 Safari/537.36',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
        'sec-ch-ua-platform': '"Linux"',
        'origin': 'https://www.crexi.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.crexi.com/',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    cleaner = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

    async with session.get(url, headers=header) as response:
        property_json = await response.json()
        try:
            post_created_at = property_json['CreatedOn']
        except KeyError:
            post_created_at = 'Post creation time missing'

        try:
            post_updated = property_json['UpdatedOn']
        except KeyError:
            post_updated = 'Post update information missing'

        try:
            property_type = ', '.join(property_json['Subtypes'])
        except KeyError:
            property_type = 'Subtypes information missing'

        try:
            investment_highlights = re.sub(cleaner, '', property_json['InvestmentHighlights']) \
                .replace('​', '').replace(' ', '')
        except KeyError:
            investment_highlights = 'Investment Highlights information missing'

        try:
            marketing_description = re.sub(cleaner, '', property_json['MarketingDescription']) \
                .replace('​', '').replace(
                ' ', '')
        except KeyError:
            marketing_description = 'Description information missing'

        try:
            asking_price = property_json['Details']['Asking Price']
        except KeyError:
            asking_price = 'Price information missing'

        try:
            property_class = property_json['Details']['Class']
        except KeyError:
            property_class = 'Property class information missing'

        try:
            square_footage = property_json['Details']['Square Footage']
        except KeyError:
            square_footage = 'Square Footage information missing'

        try:
            net_rentable_sqft = property_json['Details']['Net Rentable']
        except KeyError:
            net_rentable_sqft = 'Net Rentable sq / ft information missing'

        try:
            price_sqft = property_json['Details']['Price/Sq Ft']
        except KeyError:
            price_sqft = 'Price / sq ft  information missing'

        try:
            cap_rate = property_json['Details']['Cap Rate']
        except KeyError:
            cap_rate = 'Cap Rate information missing'

        try:
            pro_forma_cap_rate = property_json['Details']['Pro-Forma Cap Rate']
        except KeyError:
            pro_forma_cap_rate = 'Pro-Forma Cap Rate information missing'

        try:
            property_occupancy = property_json['Details']['Occupancy']
        except KeyError:
            property_occupancy = 'Occupancy information missing'

        try:
            noi = property_json['Details']['NOI']
        except KeyError:
            noi = 'NOI information missing'

        try:
            pro_forma_noi = property_json['Details']['Pro-Forma NOI']
        except KeyError:
            pro_forma_noi = 'Pro-Forma NOI information missing'

        try:
            units = property_json['Details']['Units']
        except KeyError:
            units = 'Units amount information missing'

        try:
            year_built = property_json['Details']['Year Built']
        except KeyError:
            year_built = 'Year built information missing'

        try:
            year_renovated = property_json['Details']['Year Renovated']
        except KeyError:
            year_renovated = 'Year renovated information missing'

        try:
            buildings = property_json['Details']['Buildings']
        except KeyError:
            buildings = 'Buildings amount information missing'

        try:
            stories = property_json['Details']['Stories']
        except KeyError:
            stories = 'Stories amount information missing'

        try:
            permitted_zoning = property_json['Details']['Permitted Zonning']
        except KeyError:
            permitted_zoning = 'Permitted zonning information missing'

        try:
            lot_acres = property_json['Details']['Lot Size']
        except KeyError:
            lot_acres = 'Lot size information missing'

        try:
            price_per_unit = property_json['Details']['Price/Unit']
        except KeyError:
            price_per_unit = 'Price / unit information missing'

        try:
            state = property_json['Locations'][0]['State']['Name']
        except KeyError:
            state = 'State information missing'

        try:
            property_zip = property_json['Locations'][0]['Zip']
        except KeyError:
            property_zip = 'Zip infromation missing'

        try:
            property_name = property_json['Name']
        except KeyError:
            property_name = 'Name information missing'

        try:
            apn = property_json['Details']['APN']
        except KeyError:
            apn = 'APN information missing'

        try:
            property_address = property_json['Locations'][0]['Address']
        except KeyError:
            property_address = 'Address information missing'

        try:
            latitude = property_json['Locations'][0]['Latitude']
        except KeyError:
            latitude = 'Latitude information missing'

        try:
            longitude = property_json['Locations'][0]['Longitude']
        except KeyError:
            longitude = 'Longitude information missing'

        try:
            property_city = property_json['Locations'][0]['City']
        except KeyError:
            property_city = 'City information missing'

        try:
            property_county = property_json['Locations'][0]['County']
        except KeyError:
            property_county = 'County information missing'

    async with session.get(f'{url}/tax-history') as tax_response:
        tax_json = await tax_response.json()
        try:
            tax_year = tax_json['Year']
        except KeyError:
            tax_year = 'Tax Year information missing'

        try:
            property_tax = tax_json['PropertyTax']
        except KeyError:
            property_tax = 'Property tax sum missing'

        try:
            tax_land = tax_json['Land']
        except KeyError:
            tax_land = 'Tax land information missing'

        try:
            tax_additions = tax_json['Additions']
        except KeyError:
            tax_additions = 'Tax additions information missing'

        try:
            tax_assesed_value = tax_json['AssessedValue']
        except KeyError:
            tax_assesed_value = 'Tax Assessed value information missing'

    async with session.get(f'{url}/gallery?imagesOnly=false') as gallery_response:
        gallery_json = await gallery_response.json()
        gallery = []
        for images in gallery_json:
            try:
                gallery.append(images['ImageUrl'])
            except KeyError:
                continue

    async with session.get(f'{url}/brokers') as broker_response:
        brokerage = await broker_response.json()
        try:
            brokers = brokerage
        except KeyError:
            brokers = 'Broker information missing'

    with open('properties.csv', 'a') as file:
        writer = csv.writer(file)
        writer.writerow(
            (
                property_name,
                state,
                property_city,
                property_county,
                property_zip,
                property_address,
                property_type,
                asking_price,
                price_per_unit,
                property_class,
                square_footage,
                price_sqft,
                net_rentable_sqft,
                cap_rate,
                property_occupancy,
                noi,
                pro_forma_cap_rate,
                pro_forma_noi,
                investment_highlights,
                marketing_description,
                units,
                apn,
                stories,
                lot_acres,
                buildings,
                year_built,
                year_renovated,
                permitted_zoning,
                longitude,
                latitude,
                tax_year,
                property_tax,
                tax_land,
                tax_additions,
                tax_assesed_value,
                url,
                brokers,
                post_created_at,
                post_updated,
                gallery
            )
        )


if __name__ == '__main__':
    asyncio.run(get_properties_urls())
    finish_time = time.time() - start_time
    print(f"Затраченное на работу скрипта время: {finish_time}")