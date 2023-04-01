import os
import aiohttp
import asyncio
import pandas as pd
import time

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



def player_statistics(file, year_id):
    import_dir = os.path.join(os.getcwd(), 'player_overview', file+'.csv')
    export_dir = os.path.join(os.getcwd(), 'player_statistics', file+'.csv')
    
    player_ids_df = pd.read_csv(import_dir)
    ids = player_ids_df['id'].astype('int').astype('str').to_list()

    print('Year id:', year_id, ' ::  No. of entries:', len(ids))

    
    querystring = {"comps": "1", "compSeasons": year_id}

    json_data = []
    stats_list = []
    
    async def fetch(client, item):
        url = 'https://footballapi.pulselive.com/football/stats/player/{id}'.format(id=item)
        async with client.get(url, params=querystring, headers=headers) as resp:
            if resp.status != 200:
                print(f'Error: {item}')
            json =  await resp.json()
            json_data.append(json)


    async def get_stats():
        async with aiohttp.ClientSession() as client:
            await asyncio.gather(*(fetch(client, item) for item in ids))

    def clean_data(json_data):
        stats = dict()
        stats['id'] = int(json_data['entity']['id'])
        stats['year_id'] = year_id

        if json_data['stats'] != []:
            player_stats = json_data['stats']
            for i in range(len(player_stats)):
                key = player_stats[i]['name']
                value = player_stats[i]['value']
                stats[key] = value


        stats_list.append(stats)
        return stats

    def list_to_df(stats_list):
        df = pd.DataFrame.from_dict(stats_list)
        return df

    def main():
        start_time = time.time()
        results = asyncio.run(get_stats())
        
        for data in json_data:
            clean_data(data)
        df = pd.DataFrame.from_dict(stats_list)
        print(df.head())

        print(df.shape)
        print("\n--- %s seconds ---\n" % (time.time() - start_time))

        df.to_csv(export_dir, index=False)
    
    return main()


if __name__ == '__main__':
    time_total = time.time()
    list(map(player_statistics, year.keys(), year.values()))
    print("\n--- %s seconds ---\n" % (time.time() - time_total))