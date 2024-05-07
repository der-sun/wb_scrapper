import datetime
import requests
import pandas as pd
from retry import retry
import sqlite3

db_cnt=0
get_list=[]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0",
    "Accept": "*/*",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.wildberries.ru",
    'Content-Type': 'application/json; charset=utf-8',
    'Transfer-Encoding': 'chunked',
    "Connection": "keep-alive",
    'Vary': 'Accept-Encoding',
    'Content-Encoding': 'gzip',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site"
}
CATALOG_URL = 'https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v2.json'


def get_catalogs_wb() -> dict:
    """получаем полный каталог Wildberries"""
    return requests.get(CATALOG_URL, headers=HEADERS).json()


def get_data_category(catalogs_wb: dict) -> list:
    """сбор данных категорий из каталога Wildberries"""
    catalog_data = []
    if isinstance(catalogs_wb, dict):
        has_children = False
        if 'childs' in catalogs_wb:
            has_children = True
        catalog_data.append({
            'id': catalogs_wb['id'],
            'name': f"{catalogs_wb['name']}",
            'URL': catalogs_wb['url'],
            'childs': has_children
        })
    elif isinstance(catalogs_wb, list):
        for child in catalogs_wb:
            catalog_data.extend(get_data_category(child))
    return catalog_data


def get_catalog_by_id(id: int, catalogs_list: list) -> dict:
    """Выводит информацию о каталоге с заданным id и все содержащиеся в нем каталоги"""
    result_catalog = []  # Переменная для хранения данных каталога
    catalog_with_id = []  # Переменная для хранения каталога с заданным id
    i=0

    # Функция для рекурсивного поиска каталога с заданным id и его содержимого
    def search_catalog_by_id(catalogs, target_id):
        nonlocal result_catalog, catalog_with_id, i
        
        for catalog in catalogs:
            try:
                if catalog['id'] == target_id and catalog['id']:
                    # Проверяем наличие дочерних каталогов
                    catalog_with_id.extend(catalogs[i]['childs']) # Сохраняем каталог с заданным id
                    for child in catalog['childs']:
                        has_children = False  # Флаг наличия дочерних каталогов
                        if 'childs' in child:  # Проверяем наличие ключа 'childs' в словаре subchild
                                has_children = True

                            # Сохраняем данные каталога в переменную result_catalog
                        result_catalog.append({
                        'id': child['id'],
                        'name': f"{child['name']}",
                        'URL': child['url'],
                        'childs': has_children  # Добавляем флаг наличия дочерних каталогов
                            })
                    break
                i+=1
            except KeyError:
                pass  # Продолжаем выполнение, если возникла ошибка KeyError

    # Вызываем функцию для поиска каталога с заданным id
    search_catalog_by_id(catalogs_list, id)

    # Возвращаем данные каталога и его содержимого, а также каталог с заданным id
    return result_catalog, catalog_with_id



def get_data_from_json(json_file: dict) -> list:
    """извлекаем из json данные"""
    data_list = []
    for data in json_file['data']['products']:
        data_list.append({
            'id': data.get('id'),
            'Наименование': data.get('name'),
            'Цена': int(data.get("priceU") / 100),
            'Цена со скидкой': int(data.get('salePriceU') / 100),
            'Скидка': data.get('sale'),
            'Бренд': data.get('brand'),
            'Рейтинг': data.get('rating'),
            'Продавец': data.get('supplier'),
            'Рейтинг продавца': data.get('supplierRating'),
            'Кол-во отзывов': data.get('feedbacks'),
            'Рейтинг отзывов': data.get('reviewRating'),
            'Промо текст карточки': data.get('promoTextCard'),
            'Промо текст категории': data.get('promoTextCat'),
            'Ссылка': f'https://www.wildberries.ru/catalog/{data.get("id")}/detail.aspx?targetUrl=BP'
        })
    return data_list


@retry(Exception, tries=-1, delay=0)
def scrap_page(page: int, shard: str, query: str, low_price: int, top_price: int, discount: int = None) -> dict:
    """Сбор данных со страниц"""
    url = f'https://catalog.wb.ru/catalog/{shard}/catalog?appType=1&curr=rub' \
          f'&dest=-1257786' \
          f'&locale=ru' \
          f'&page={page}' \
          f'&priceU={low_price * 100};{top_price * 100}' \
          f'&sort=popular&spp=0' \
          f'&{query}' \
          f'&discount={discount}'

    r = requests.get(url, headers=HEADERS)
    print(f'[+] Страница {page}')
    return r.json()


def save_excel(data: list, filename: str):
    """сохранение результата в excel файл"""
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter(f'{filename}.xlsx')
    df.to_excel(writer, sheet_name='data', index=False)
    # указываем размеры каждого столбца в итоговом файле
    writer.sheets['data'].set_column(0, 1, width=10)
    writer.sheets['data'].set_column(1, 2, width=34)
    writer.sheets['data'].set_column(2, 3, width=8)
    writer.sheets['data'].set_column(3, 4, width=9)
    writer.sheets['data'].set_column(4, 5, width=4)
    writer.sheets['data'].set_column(5, 6, width=10)
    writer.sheets['data'].set_column(6, 7, width=5)
    writer.sheets['data'].set_column(7, 8, width=25)
    writer.sheets['data'].set_column(8, 9, width=10)
    writer.sheets['data'].set_column(9, 10, width=11)
    writer.sheets['data'].set_column(10, 11, width=13)
    writer.sheets['data'].set_column(11, 12, width=19)
    writer.sheets['data'].set_column(12, 13, width=19)
    writer.sheets['data'].set_column(13, 14, width=67)
    writer.close()
    print(f'Все сохранено в {filename}.xlsx\n')


def parser(url, low_price, top_price, discount):
    """основная функция"""
    try:
        # поиск введенной категории в общем каталоге
        data_list = []
        for page in range(1, 51):  # вб отдает 50 страниц товара, беру 11
            data = scrap_page(
                page=page,
                low_price=low_price,
                top_price=top_price,
                discount=discount)
            if len(get_data_from_json(data)) > 0:
                data_list.extend(get_data_from_json(data))
            else:
                break
        print(f'Сбор данных завершен. Собрано: {len(data_list)} товаров.')
        # сохранение найденных данных
        save_excel(data_list, f'{category["name"]}_from_{low_price}_to_{top_price}')
        print(f'Ссылка для проверки: {url}?priceU={low_price * 100};{top_price * 100}&discount={discount}')
    except TypeError:
        print('Ошибка! Возможно не верно указан раздел. Удалите все доп фильтры с ссылки')
    except PermissionError:
        print('Ошибка! Вы забыли закрыть созданный ранее excel файл. Закройте и повторите попытку')

#вывод данных из бд 
def choose(length,c,db_name):
    for i in range(length):
        query = "SELECT name FROM "+db_name+" WHERE ROWID = " + str(i+1)
        c.execute(query)
        for var_name in c.fetchone(): #fetch(one/all/many) возращает нам список с кортежами
            print(str(i+1)+". "f"{var_name}") #выписываем через форматированную строку переменную, что взяли в цикле у fetchone
        i+=1
    db.commit()

#запись данных в бд
def insert_into_db(db, c,input, db_name):
    global get_list
    global db_cnt

    if (db_cnt!=1):
        c.execute("SELECT id FROM Catalog"+str(db_cnt-2)+" WHERE ROWID="+str(input)+";")
        for row_id in c.fetchone():
            catalog_data, get_list = get_catalog_by_id(row_id,get_list)
    elif(db_cnt==1):
        get_list=get_catalogs_wb()
        catalog_data = get_data_category(get_list)
        
    for i in range(len(catalog_data)):
        query = "INSERT OR REPLACE INTO " + str(db_name) + " " + str(tuple(catalog_data[i].keys())) + " VALUES " + str(tuple(catalog_data[i].values())) + ";"
        c.execute(query)
        db.commit()
        i+=1
    choose(len(catalog_data),c,db_name)

#очистка всех таблиц
def db_clear(db, c):
    i=0
    c.execute("SELECT * FROM sqlite_master WHERE type='table';")
    for everyone_table in c.fetchall():
        c.execute("DROP TABLE Catalog" + str(i))
        i+=1
        db.commit()

#создание бд
def db_create(c):
    global db_cnt
    db_name="Catalog"+str(db_cnt)
    c.execute("CREATE TABLE "+str(db_name)+" (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, URL VARCHAR(128) NOT NULL, childs BOOLEAN DEFAULT FALSE)")
    db.commit()
    db_cnt+=1
    return db_name


if __name__ == '__main__':
    db = sqlite3.connect('WB_Catalogs.db')
    c = db.cursor()
    db_clear(db,c)
    print("Выберите категорию из списка:")
    parent_id = 0
    checker = True
    insert_into_db(db, c, parent_id, db_name=db_create(c))
    while (checker == True):
        parent_id=int(input())
        c.execute("SELECT childs FROM Catalog"+str(db_cnt-1)+" WHERE ROWID = " +str(parent_id)+ ";")
        for childs in c.fetchone():
            if (childs != 0):
                insert_into_db(db, c, parent_id, db_name=db_create(c))
            else:
                checker = False
    c.execute("SELECT url FROM Catalog"+str(db_cnt-1)+" WHERE ROWID = " +str(parent_id)+ ";")
    for i in c.fetchone():
        url = 'https://www.wildberries.ru'+i
    
    parser(url=url, low_price=int(input("Укажите минимальный порог цен: ")), top_price=int(input("Укажите максимальный порог цен: ")), discount=int(input("Укажите минимальную скидку: ")))
    db.close()