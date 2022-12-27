import scrape
from db import DB


URL_HOME = 'https://www.list.am/en'
db = DB()
conn, cur = db.conn, db.cur


def select_categories():
    if db.check_table('property_type') == True:
        cur.execute('''SELECT name, q_string
                       FROM property_type;''')
        cat_paths = cur.fetchall()
        if len(cat_paths) > 0:
            conn.close()
            return cat_paths
    db.create_table_categories()

    return select_categories()


def select_regions():
    if db.check_table('regions') == True:
        cur.execute('''SELECT name, q_string
                       FROM property_type;''')
        reg_paths = cur.fetchall()
        if len(reg_paths) > 0:
            conn.close()
            return reg_paths
    db.create_table_regions()

    return select_regions()


def load_urls_todb(urls, cat_id, reg_id):
    if db.check_table('urls') == True:
        for url in urls:
            full_url = 'https://list.am' + url['href']
            cur.execute('''SELECT links
                           FROM tables
                           WHERE table_name=%s)''', (mytable,))
            if full in urls_data.tolist():
                continue
            if full_url not in links.values():
                links.update({len(links): full_url})

    return


def main():
    categories = select_categories()
    regions = select_regions()
    # For each category pages (Apartments, Houses, Lands, etc.) go over every region (Yerevan, Armavir, Ararat, etc.)
    for cat, cat_path in categories:
        for reg, reg_path in regions:
            url = URL_HOME + cat_path + reg_path
            urls = scrape.get_ad_links(url, cat, reg)
            load_urls_todb(urls, cat, reg)


if __name__ == '__main__':
    main()

