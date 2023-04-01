import os
import aiohttp
import asyncio
import pandas as pd
import time
from dateutil import parser

year = {
    "2021_22": "418",
    "2020_21": "363",
    "2019_20": "274",
    "2018_19": "210",
    "2017_18": "79",
    "2016_17": "54",
    "2015_16": "42",
    "2014_15": "27",
    "2013_14": "22",
    "2012_13": "21",
    "2011_12": "20",
    "2010_11": "19",
    "2009_10": "18",
    "2008_09": "17",
    "2007_08": "16",
    "2006_07": "15",
    "2005_06": "14",
    "2004_05": "13",
    "2003_04": "12",
    "2002_03": "11",
    "2001_02": "10",
    "2000_01": "9",
    "1999_00": "8",
    "1998_99": "7",
    "1997_98": "6",
    "1996_97": "5",
    "1995_96": "4",
    "1994_95": "3",
    "1993_94": "2",
    "1992_93": "1"
}

headers = {
        'authority': "footballapi.pulselive.com",
        'accept': "*/*",
        'accept-language': "en-US,en;q=0.9",
        'content-type': "application/x-www-form-urlencoded; charset=UTF-8",
        'dnt': "1",
        'if-none-match': 'W/"03e5f65780221d7f2731a4126a16bd3db"',
        'origin': "https://www.premierleague.com",
        'referer': "https://www.premierleague.com/",
        # 'sec-ch-ua': '" Not A;Brand";v="99', "Chromium";v="101", "Google Chrome";v="101"",
        'sec-ch-ua-mobile': "?0",
        # 'sec-ch-ua-platform': ""Linux"",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "cross-site",
        'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36"
    }


page_size = '100'
no_of_pages = 12


def player_overview(file, year_id):
    overview_dir = os.path.join(os.getcwd(), 'player_overview')
    export_dir = os.path.join(overview_dir, file + '.csv') 
    print('Year id: ' ,year_id, ' :: ', export_dir, '\n')

    json_data = []
    
    async def fetch(client, page_no):
        url = 'https://footballapi.pulselive.com/football/players'
        querystring = {"pageSize": page_size,
                    "compSeasons": year_id,
                    "altIds": "true",
                    "page": page_no,
                    "type": "player",
                    "id": "-1",
                    }
        async with client.get(url, params=querystring, headers=headers) as resp:
            if resp.status != 200:
                print(f'Error: {page_no}, {year_id}')
            json =  await resp.json()
            json_data.append(json)


    async def get_overview():
        async with aiohttp.ClientSession() as client:
            results = await asyncio.gather(*(fetch(client, page_no) for page_no in range(no_of_pages)))
            return results

    def clean_data(json_data, data_frame):
        if json_data['content']:
            data = json_data['content']
            df_main = pd.DataFrame.from_dict(data)

            df_main['Birth Place'] = [i['country'].get('country') for i in df_main['birth']]
            birthdate_list = [i.get('date') for i in df_main['birth']]
            for i in range(len(birthdate_list)):
                if birthdate_list[i] is None:
                    birthdate_list[i] = None
                else:
                    birthdate_list[i] = parser.parse(birthdate_list[i].get('label'))

            df_main['Birth Date'] = birthdate_list

            df_main['Position'] = [i.get('position') for i in df_main['info']]
            df_main['Position Info'] = [i.get('positionInfo') for i in df_main['info']]
            df_main['Shirt Num'] = [i.get('shirtNum') for i in df_main['info']]

            df_main['Display Name'] = [i.get('display') for i in df_main['name']]
            df_main['First Name'] = [i.get('first') for i in df_main['name']]
            df_main['Last Name'] = [i.get('last') for i in df_main['name']]

            df_main['id'] = df_main['id'].astype('int')
            df_main['National Team'] = [i.get('country') for i in df_main['nationalTeam']]

            df_filtered = df_main[['id', 'playerId', 'Display Name', 'First Name', 'Last Name', 'Birth Date', 'Birth Place',
                                'National Team', 'Shirt Num', 'Position', 'Position Info']]

            df = pd.concat([data_frame, df_filtered], axis=0, ignore_index=True)
            return df
        

    def main():
        start_time = time.time()
        results = asyncio.run(get_overview())
        df = pd.DataFrame()
        for data in json_data:
            df = clean_data(data, df)

        print(df.head())

        print(df.shape)
        print("\n--- %s seconds ---\n" % (time.time() - start_time))

        df.to_csv(export_dir, index=False)
        
    
    return main()

    
    

if __name__ == '__main__':
    time_total = time.time()
    list(map(player_overview, year.keys(), year.values()))
    print("\n----- %s seconds -----\n" % (time.time() - time_total))