# converter-XML2sqlite3

  ![Окно программы](/progface.JPG)
 
Обработка XML дампа базы раздач rutracker.org несколько иным способом - чтение файла, выделение блоков **'\<torrent\>..\</torrent\>'** и парсинг их через re. Парсинг через регулярные выражения работает быстрее DOM.
Относительно предыдущей версии парсера (SAX-parser) тут добавлено чтение списка каталогов и файлов и сохранение их в БД *content.db3*.

Создаются две БД: *torrents.db3* и *torrents2.db3* - форматы для INFOTOR и INFOTOR-2.

БД *content.db3* не менялась

Реализовано через интерфейс tkinter

P.S. Исходный xml-дамп на https://rutracker.org/forum/viewtopic.php?t=5591249 или https://disk.yandex.ru/d/eVzauRIH3ZCKam  
