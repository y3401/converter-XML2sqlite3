#!/usr/bin/env python
# модуль записи "RuTracker.org" xml в БД sqlite3

import sqlite3, zlib
from  xml2sqlite3 import show_status

CAT=[(1,'Обсуждения, встречи, общение'), (2,'Кино, Видео и ТВ'),(3,'Приватные форумы'), (4,'Новости'),
     (8,'Музыка'), (9,'Программы и Дизайн'), (10,'Обучающее видео'), (11,'Разное'),
     (18,'Сериалы'), (19,'Игры'), (20,'Документалистика и юмор'), (22,'Рок-музыка'),
     (23,'Электронная музыка'), (24,'Авто и мото'), (25,'Книги и журналы'),
     (26,'Apple'), (28,'Спорт'), (29,'Мобильные устройства'),
     (31,'Джазовая и Блюзовая музыка'), (33,'Аудиокниги'), (34,'Обучение иностранным языкам'),
     (35,'Популярная музыка'), (36,'ОБХОД БЛОКИРОВОК'),(37,'Hi-Res форматы, оцифровки'),
     (38,'Товары, услуги, игры и развлечения'),(39,'Музыкальное видео')]

def create_db(dirdb=''):    #Создание базы (INFOTOR-2) и заполнение таблицы категорий
    global DB,CAT
    DB=sqlite3.connect(dirdb + 'torrents2.db3')
    cur=DB.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS "category"
    ("code_category" smallint NOT NULL PRIMARY KEY,
    "name_category" varchar(50) NOT NULL,
    "load_category" bool NOT NULL);

    CREATE TABLE IF NOT EXISTS "forum"
    ("code_forum" smallint NOT NULL PRIMARY KEY,
    "name_forum" varchar(80) NOT NULL,
    "category_id" smallint NOT NULL REFERENCES "category" ("code_category"));

    CREATE INDEX IF NOT EXISTS "forum_category_id_48a15a32" ON "forum" ("category_id");

    CREATE TABLE IF NOT EXISTS "torrent"
    ("file_id" integer NOT NULL PRIMARY KEY,
    "title_up" varchar(255) NOT NULL,
    "full_info" blob NOT NULL,
    "cat_id" smallint NOT NULL REFERENCES "category" ("code_category"),
    "forum_id" smallint NOT NULL REFERENCES "forum" ("code_forum"));

    CREATE INDEX IF NOT EXISTS "torrent_forum_id_b67937c0" ON "torrent" ("forum_id");
    CREATE INDEX IF NOT EXISTS "torrent_category_id_b67607c1" ON "torrent" ("cat_id");

    CREATE TABLE IF NOT EXISTS "vers"
    ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    "vers" varchar(8) NOT NULL);
    """)

    cur.executescript("""DELETE FROM category; DELETE FROM forum; DELETE FROM torrent; DELETE FROM vers;""")
    cur.executemany('INSERT INTO category(code_category,name_category,load_category) VALUES (?, ?, 1);', CAT)
    dbc()
    cur.close()
    #DB.execute("PRAGMA synchronous = OFF;")
    DB.execute("PRAGMA temp_store = MEMORY;")

def create_db_2(dirdb=''):    #Создание базы (INFOTOR) и заполнение таблицы категорий
    global DB2,CAT
    DB2=sqlite3.connect(dirdb + 'torrents.db3')
    cur=DB2.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS "category"
    ("code_category" smallint NOT NULL PRIMARY KEY,
    "name_category" varchar(50) NOT NULL,
    "load_category" bool NOT NULL);

    CREATE TABLE IF NOT EXISTS "forum"
    ("code_forum" smallint NOT NULL PRIMARY KEY,
    "name_forum" varchar(80) NOT NULL,
    "category_id" smallint NOT NULL REFERENCES "category" ("code_category"));

    CREATE INDEX IF NOT EXISTS "forum_category_id_48a15a32" ON "forum" ("category_id");

    CREATE TABLE IF NOT EXISTS "torrent"
    ("file_id" integer NOT NULL PRIMARY KEY,
    "hash_info" varchar(40) NOT NULL,
    "title" varchar(255) NOT NULL,
    "size_b" integer NOT NULL,
    "date_reg" varchar(20) NOT NULL,
    "forum_id" smallint NOT NULL REFERENCES "forum" ("code_forum"));

    CREATE INDEX IF NOT EXISTS "torrent_forum_id_b67937c0" ON "torrent" ("forum_id");

    CREATE TABLE IF NOT EXISTS "vers"
    ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    "vers" varchar(8) NOT NULL);
    """)
    cur.executescript("""DELETE FROM category; DELETE FROM forum; DELETE FROM torrent; DELETE FROM vers;""")
    cur.executemany('INSERT INTO category(code_category,name_category,load_category) VALUES (?, ?, 1);', CAT)
    dbc()
    cur.close()
    #DB.execute("PRAGMA synchronous = OFF;")
    DB2.execute("PRAGMA temp_store = MEMORY;")

def create_db_content(dirdb=''): # Создание доп. БД для хранения описаний раздач
    global DB1
    DB1=sqlite3.connect(dirdb + 'content.db3')
    cur=DB1.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS "contents"
    ("tid" integer NOT NULL PRIMARY KEY,
    "cont" text NOT NULL);

    CREATE TABLE IF NOT EXISTS "files"
    ("tid" integer NOT NULL PRIMARY KEY,
    "files" text NOT NULL);

    CREATE INDEX IF NOT EXISTS "files_tid_id_b67937c1" ON "files" ("tid");

    DELETE FROM contents;
    DELETE FROM files;
    """)
    cur.close()
    #DB1.execute("PRAGMA synchronous = OFF;")
    DB1.execute("PRAGMA temp_store = MEMORY;")

def open_db(dirdb=''):    #Открыть базу (INFOTOR-2) 
    global DB
    DB=sqlite3.connect(dirdb + 'torrents2.db3')

def open_db_2(dirdb=''):    #Открыть базу (INFOTOR-2) 
    global DB2
    DB2=sqlite3.connect(dirdb + 'torrents.db3')

def open_db_content(dirdb=''):    #Открыть базу () 
    global DB1
    DB1=sqlite3.connect(dirdb + 'content.db3')

def dbc():
    try:
        DB.commit()
    except:
        pass
    try:
        DB1.commit()
    except:
        pass
    try:
        DB2.commit()
    except:
        pass

def ins_vers(dt):
    DB.execute('INSERT INTO vers(vers) VALUES (?);', (dt,))
    DB.commit()
    DB2.execute('INSERT INTO vers(vers) VALUES (?);', (dt,))
    DB2.commit()
    
def check_podr(kod_podr,name_podr,cat_id):
    try:
        DB.execute('INSERT INTO forum(code_forum,name_forum,category_id) VALUES (?,?,?)', (kod_podr,name_podr,cat_id))
    except:
        pass
    try:
        DB2.execute('INSERT INTO forum(code_forum,name_forum,category_id) VALUES (?,?,?)', (kod_podr,name_podr,cat_id))
    except:
        pass

def ins_tor(id_podr,id_file,hash_info,title,size_b,date_reg):

    try:
        DB2.execute('INSERT INTO torrent(forum_id,file_id,hash_info,title,size_b,date_reg) SELECT ?,?,?,?,?,?;', (id_podr,id_file,hash_info,title,size_b,date_reg))
    except:
        dbc()

def insert_tor(cat_id,forum_id,tid,magnet,title,size_b,date_reg):
    title_up = title.upper()
    full_info = zlib.compress(str('{0}‰{1}‰{2}‰{3}‰{4}'.format(tid,title,magnet,size_b,date_reg)).encode(),8)
    try:
        DB.execute('INSERT INTO torrent(file_id,title_up,full_info,cat_id,forum_id) SELECT ?,?,?,?,?;', (tid,title_up,full_info,cat_id,forum_id))
    except:
        dbc()

def ins_content(id_tor, cont,filesList):
    C = zlib.compress(str(cont).encode(),8)
    C1 = zlib.compress(str(filesList).encode(),8)
    #C = proc(id_tor,cont)
    DB1.execute('INSERT INTO contents(tid,cont) SELECT ?,?', (id_tor,C))
    DB1.execute('INSERT INTO files(tid,files) VALUES (?, ?);', (id_tor,C1))

def ins_files(id_tor, filesList):
    C1 = zlib.compress(str(filesList).encode())
    DB1.execute('INSERT INTO files(tid,files) VALUES (?, ?);', (id_tor,C1))

def close_db(vac=0):
    try:
        if vac==1:
            show_status("Сжатие torrents2.db3")
            DB.execute('vacuum')
        DB.close()
        if vac==1:
            show_status("Сжатие content.db3")
            DB1.execute('vacuum')
        DB1.close()
        if vac==1:
            show_status("Сжатие torrents.db3")
            DB2.execute('vacuum')
        DB2.close()
    except:
        pass

if __name__ == '__main__':
    dbc()
