# imports
from bs4 import BeautifulSoup
import requests
import pandas
import time
import csv
import glob


start = time.time()
Link_batch = []
batch = []
all_data_count = None
N = 50   # N=50 means, 100 rows/shows data
batch_size = 10   # for large value of N change this


def find_link(url):
    # Find all the shows link and add them in a link_batch.
    global Link_batch
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(e)
    else:
        soup = BeautifulSoup(response.content, 'html.parser')
        soup = soup.find_all('tr', class_='ranking-list')
        for link in soup:
            Link_batch.append(link.find('a')['href'])
    return


def anime_data(url):
    # Scrape single anime show info from site and add to the dict.

    fields = [
        'English', 'Japanese', 'Type', 'Episodes', 'Status', 'Premiered',
        'Licensors', 'Studios', 'Source', 'Genres', 'Demographic',
        'Rating', 'Score'
    ]

    anime_info = {field: None for field in fields}

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    else:
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            soup = soup.find('div', class_='leftside')
            data = [((df.get_text(strip=True)).split(':')) for df in soup.find_all('div', class_='spaceit_pad')]
            for i in data:
                if len(i) < 2:
                    continue
                key = i[0]
                match key:
                    case 'English':
                        anime_info[key] = ':'.join(i[1:])
                    case 'Japanese':
                        anime_info[key] = i[1]
                    case 'Type':
                        anime_info[key] = i[1]
                    case 'Episodes':
                        anime_info[key] = i[1]
                    case 'Premiered':
                        anime_info[key] = i[1]
                    case 'Status':
                        anime_info[key] = i[1]
                    case 'Licensors':
                        anime_info[key] = i[1]
                    case 'Studios':
                        anime_info[key] = i[1]
                    case 'Source':
                        anime_info[key] = i[1]
                    case 'Genres':
                        anime_info[key] = i[1]
                    case 'Demographic':
                        anime_info[key] = i[1]
                    case 'Rating':
                        anime_info[key] = i[1][:6]
                    case 'Score':
                        anime_info[key] = i[1].split('(')[0]

        else:
            print('Unsuccessful get')
            return anime_info
    return anime_info

a = 0
for i in range(0, N+1, 50):
    find_link(f'https://myanimelist.net/topanime.php?limit={i}')
    all_data_count = i
    print(f'Total scraped: {all_data_count + 50}')
    time.sleep(2)

    if (all_data_count + 50) % batch_size == 0:
        df = pandas.DataFrame(Link_batch)
        df.to_csv(f'link_batch_{a+1}.csv', index=False)
        print(f'Link_batch_{a+1} saved.')
        a += 1
        Link_batch = []
        time.sleep(5)

csv_files = glob.glob("link_batch_*.csv")
df_link = [pandas.read_csv(file) for file in csv_files]
final_links = pandas.concat(df_link, ignore_index=True)
final_links.to_csv("Anime_Show_Links.csv", index=False)
print(f'All links extracted successfully. Total no. ---> {all_data_count+50}')


with open('Anime_Show_Links.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)[1:]

    for i, row in enumerate(reader):
        url = row[0]
        anime = anime_data(url)
        anime['Anime show link'] = url
        batch.append(anime)
        all_data_count = i
        print(f'Rows Count: {all_data_count+1}')
        time.sleep(2)

        # Save batch every 100 rows
        if (all_data_count+1) % batch_size == 0:
            df = pandas.DataFrame(batch)
            df.to_csv(f'anime_batch_{all_data_count // batch_size + 1}.csv', index=False)
            print(f'Saved batch {all_data_count // batch_size + 1}')
            batch = []       # clean for next batch
            time.sleep(5)    # pause between batches


csv_files = glob.glob("anime_batch_*.csv")
df_list = [pandas.read_csv(file) for file in csv_files]
final_df = pandas.concat(df_list, ignore_index=True)
final_df.to_csv("Anime_Shows_Data.csv", index=False)

print('File combining done')
print("All batches are saved.")
print('Time taken-->', time.time()-start)
