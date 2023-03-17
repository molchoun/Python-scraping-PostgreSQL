import scrape
from db import DB


URL_HOME = 'https://www.list.am/en'
db = DB()
conn, cur = db.conn, db.cur


def select_categories():
    if db.table_exists('property_type'):
        cur.execute('''SELECT name, q_string
                       FROM property_type;''')
        cat_paths = cur.fetchall()
        if len(cat_paths) > 0:
            return cat_paths
    db.create_table_categories()

    return select_categories()


def select_regions():
    if db.table_exists('regions'):
        cur.execute('''SELECT name, q_string
                       FROM regions;''')
        reg_paths = cur.fetchall()
        if len(reg_paths) > 0:
            return reg_paths
    db.create_table_regions()

    return select_regions()


def load_urls_todb(urls, cat, reg):
    if db.table_exists('urls'):
        cur.execute('SELECT id FROM property_type WHERE name = %s', (cat, ))
        cat_id = cur.fetchone()[0]
        cur.execute('SELECT id FROM regions WHERE name = %s', (reg, ))
        reg_id = cur.fetchone()[0]
        insert_into_table = '''
                            INSERT INTO urls(url, cat_id, reg_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (url) DO NOTHING
                            '''
        for url in urls:
            full_url = 'https://list.am' + url['href']
            cur.execute(insert_into_table, (full_url, cat_id, reg_id))
        conn.commit()
    else:
        db.create_table_urls()
        return load_urls_todb(urls, cat, reg)


def main():
    categories = select_categories()
    regions = select_regions()
    # For each category pages (Apartments, Houses, Lands, etc.) go over every region (Yerevan, Armavir, Ararat, etc.)
    for cat, cat_path in categories:
        for reg, reg_path in regions:
            url = URL_HOME + cat_path + reg_path
            urls = scrape.get_ad_links(url, cat, reg)
            load_urls_todb(urls, cat, reg)
    conn.close()


if __name__ == '__main__':
    main()

