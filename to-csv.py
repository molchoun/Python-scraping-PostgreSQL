from scrape import get_category_links
import pandas as pd
from scrape_ad_data import scrape_apt_ad_page
#
# dict_apt = {'Apartments': get_category_links('https://www.list.am/category/60/', 250)}
# df1 = pd.DataFrame(dict_apt)
# df1.to_csv('apartments-evn.csv')
#
# dict_houses = {'Houses': get_category_links('https://www.list.am/category/62/', 88)}
# df2 = pd.DataFrame(dict_houses)
# df2.to_csv('houses-evn.csv')

df3 = pd.DataFrame(scrape_apt_ad_page()[0], columns=scrape_apt_ad_page()[1])
df = pd.read_csv('apartments-evn.csv')
df3['Links'] = df['Apartments']
df3.to_csv('ad-data.csv')

