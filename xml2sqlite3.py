#
import tkinter as tk 
from tkinter.filedialog import askopenfilename, askdirectory
from tkinter.ttk import Checkbutton, Progressbar, Entry, Label
from tkinter import messagebox as mbox
from tkinter import BooleanVar
#from xml.dom import minidom
import os, os.path
import modsql3_ as lite
import export_in_CSV as expCSV
import time
import re
import pickle
import threading


pickle.DEFAULT_PROTOCOL
k = 0
time_begin = 0  
time_end = 0
CAT_INV = {}
forums = {}
param = {}
sz = 0
backup = ''
period = ''
fr = ''
n=0
f_list = []
tid=''
stp=True
started=False
stime = True


def calc(bsize=0):
    it=0
    razmer=['б','Кб','Мб','Гб','Тб']
    while bsize>1024:
        bsize/=1024
        it+=1
    if it>0:
        p=str(bsize).split(".")
        return ('{} '+razmer[it]).format(round(bsize,2))
    else:
        return ('{} '+razmer[0]).format(int(bsize))

def sepp(bsize=0):
    s = str(bsize)
    return s[-9:-6] + ' ' + s[-6:-3] + ' ' +s[-3:]

def save_param():
    global dirDB
    dirDB = path_DB.get()
    param['dirDB'] = dirDB
    param['vacuum'] = chk_state.get()
    output = open('param.pkl', 'wb')
    pickle.dump(param, output)
    output.close()

def load_param():
    try:
        infile = open('param.pkl', 'rb')
        param = pickle.load(infile)
        infile.close()
        path_DB.delete(0,len(path_DB.get()))
        path_DB.insert(0,param.get('dirDB','C:/DB'))
        chk_state.set(param.get('vacuum', True))
    except:
        output = open('param.pkl', 'wb')
        output.close()
    
def init_start():
    k=0
    bar['value'] = 0
    lbl_percent.config(text = "0 %")
    lbl_progress.config(text="0")
    lbl_status.config(text="")
    window.update()

def select_file():
    filepath = askopenfilename(
        filetypes=[("Backup files", "*.xml")]
    )
    if not filepath:
        return
    up_select_file(filepath)

def SelectDir():
    dir_name = askdirectory(initialdir=os.getcwd())
    if dir_name == "":
        return None
    else:
        path_DB.delete(0,tk.END)
        path_DB.insert(0,dir_name)
        check_exist()
        
def up_select_file(fl):
    global period, sz, catalog
    xmlfile.delete(0,tk.END)
    xmlfile.insert(0,fl)
    sz = os.path.getsize(fl)
    lbl_size.config(text = "Размер: " + calc(sz))
    btn_start['state']='normal'
    try:
        period = re.findall(r'\d{8}', fl)[0]
    except:
        period = str(time.localtime()[0])+('0'+str(time.localtime()[1]))[-2:]+('0'+str(time.localtime()[2]))[-2:]
    catalog = './'+period
    window.title(f"XML > SQLite3 - {period}")
    init_start()
    k = 0

def check_exist():                      # проверка наличия torrents2.db3
    bd = path_DB.get() + "/torrents2.db3"
    if os.path.exists(bd):
        if os.path.getsize(bd)>500000000:
            btn_csv['state']='normal'

def readFileList(tid,lines):            # читаем список файлов
    n = 0
    f_list = ''

    relines = re.findall(r"<.*?>",lines,re.S)
    for line in relines:
        
        line = line.replace("\n"," ")
        if line.startswith("<dir",0):
            named = re.findall(r'''name="(.*?)">''',line,re.S+re.MULTILINE)
            try:
                fileName = named[0]
            except:
                pass
            n += 1
            f_list += ',("{0}","{1}",0,{2})'.format(tid,fileName,n)
        elif line.startswith("<file",0):
            namef = re.findall(r'''name="(.*?)"/>''',line,re.S+re.MULTILINE)
            try:
                fileName = namef[0]
            except:
                pass
            sizef = re.findall(r'size="(.+)"\s',line,re.S)
            fileSize = sizef[0]
            n += 1
            f_list += ',("{0}","{1}","{2}",{3})'.format(tid, fileName,fileSize,n)
    return tid,f_list

def extract(buff):                          # Парсер. DOM заменен на re - быстрее работает 
    tids=re.findall(r'torrent\sid="(.*?)\"',buff)
    tid=tids[0]
    rd = re.findall(r'registred_at="(.*?)\"',buff)
    reg_date = rd[0]
    sizes = re.findall(r'size="(.*?)\">',buff)
    b_size = sizes[0]

    tl = re.findall(r"<title>(.*?)</title>",buff,re.MULTILINE+re.S)
    titles = tl[0]
    if titles.find('CDATA')>-1:
        title=titles[9:-3]
    else:
        title = titles
    title=title.replace('"',"'")
    mg = re.findall(r'hash=\"(.*?)\"',buff)
    magnet = mg[0]
    frm = re.findall(r'forum\sid="(.*?)\">',buff)
    forum_id = frm[0]
    nn = len(forum_id) +13
    forums = re.findall(r'<forum .*?forum>',buff)
    ftmp = forums[0][nn:-8]
    
    if ftmp.find('CDATA')>-1:
        forum=ftmp[9:-3]
    else:
        forum = ftmp
    invers_category()
    forum=forum.replace('"',"'")
    n=CAT_INV.get(forum.split(sep=" - ")[0],0)
    fr=forum.split(sep=" - ")[-1]

    cnt = re.findall(r"<content>(.*?)</content>",buff,re.MULTILINE+re.S)
    contents= cnt[0]
    if contents.find('CDATA')>1:
        content = contents[9:-3]
    else:
        content = contents
    
    fls = re.findall(r"/content>(.*?)</tor",buff,re.MULTILINE+re.S)
    dirs = fls[0] 

    return ([forum_id,fr,n,tid,magnet,title,b_size,reg_date,content,('[{0}]'.format(readFileList(tid,dirs)[1][1:]))])
    #return ([forum_id,fr,n,tid,magnet,title,b_size,reg_date])   
    # forum_id,fr,n,tid,magnet,title,b_size,reg_date,content,'[{0}]'.format(readFileList(tid,dirs)[1][1:]) 
    #    0     1  2   3    4      5    6        7        8          9   


def invers_category():
    for la in lite.CAT:
        CAT_INV[la[1]]=la[0]

def show_status(message):
    lbl_status.config(text=message)
    window.update()

def readXML(file):                  # тред чтения xml-файла и записи в базы
    global k,stime,stp,started
    poz=0
    key = 0
    buff = ''
    invers_category()
    stp=False
    #t2 = threading.Thread(target=recording,args=(Q,))
    #t2.daemon=True    
    #t2.start()
##
    #lite.create_db(dirDB+'/')
    #lite.create_db_2(dirDB+'/')
    #lite.create_db_content(dirDB+'/')
    #lite.ins_vers(period)
    #time.sleep(0.1)
    #pool = multiprocessing.Pool(3)
##
    with open(file,encoding='utf8') as fn:
        
        while True:
            
            line= fn.readline()
            
            if not line:
                break
            if line.startswith('<torrent '):
                key=1
            if line.startswith('</torrent>') and key == 1:
                buff += line
                poz=fn.tell()
                if started == False:
                    lite.dbc()
                    lite.close_db(0)
                    fn.close()
                    window.config(cursor="")
                    btn_start.config(text="Старт")
                    stime=False
                    stp=True
                    break
                #Q.put(buff)
                try:
                    #res=pool.apply_async(extract,args=(buff,))
                    res = extract(buff)
                except:
                    pass
                else:
                    data=res #.get()
                    lite.check_podr(data[0],data[1],data[2])
                    lite.insert_tor(data[2],data[0],data[3],data[4],data[5],data[6],data[7])    # torrents2.db3
                    lite.ins_tor(data[0],data[3],data[4],data[5],data[6],data[7])               # torrents.db3
                    lite.ins_content(data[3],data[8],data[9])                                   # content.db3
                lb = len(buff)
                buff = ''
                key=0
                k += 1
                if k%1000==0:
                    lite.dbc()
                    lbl_progress.config(text = sepp(k))
                    perc=round(100*poz/sz,3)
                    bar['value'] = perc
                    lbl_percent.config(text = str(perc) + " %")
                    window.update()
                

            if key==1:
                buff += line
    
    perc=round(100*(poz+12)/sz,2)
    bar['value'] = perc
    lbl_percent.config(text = str(perc) + " %")
    fn.close()
    stp=True
    lite.dbc()
    stime=False

def xml_test(file):
    if file=="":
        mbox.showerror("Ошибка","Не указан файл для обработки")
        btn_start['state']='disabled'
        return False
    else:
        fl = open(file,encoding='utf8')
        ln1 = fl.readline()
        ln2 = fl.readline()
        fl.close()
        if ln2.startswith('<torrents>'):
            return True
        else:
            mbox.showerror("Ошибка","Структура файла не соответствует требуемой")
            btn_start['state']='disabled'
            return False

def exportCSV():                                # Экспорт в CSV
    dirDB = path_DB.get() + '/'
    period = expCSV.DBvers(dirDB)
    catalog = dirDB + period
    show_status(catalog)
    j = 0
    cou = len(expCSV.seq)
    if not os.path.exists(catalog):
        os.mkdir(catalog)
    for i in expCSV.seq:
        expCSV.DBExport(dirDB,i)
        show_status('Выгрузка category_'+str(i)+'.csv')
        j += 1
        prc=round(100*j/cou,1)
        bar['value'] = prc
        lbl_percent.config(text = str(prc) + " %")
        lbl_progress.config(text = str(j))
        window.update()
    expCSV.expCategory(dirDB)
    show_status('Выгрузка списка категорий category_info.csv')
    expCSV.expForums(dirDB)
    show_status('Выгрузка списка форумов forums.csv')
    show_status('Готово!')
    mbox.showinfo("Обработка","Готово!")

def vremya(tsec):                       # приведение времени к удобочитаемому виду
    tsec=int(tsec)
    seconds=tsec % 60
    minutes=(tsec//60) % 60
    hours=(tsec//3600) % 24
    return ("{0}:{1}:{2}".format(str(hours),('0'+str(minutes))[-2:],('0'+str(seconds))[-2:]))

def time_status(tb,infile):             # подсчет времени исполнения 
    global stime
    while True:
        if started == False:
            break
        if stime == False: 
            break
        var=vremya(round(time.time() - tb))
        lbl_status.config(text=f"Обработка файла {infile} - " + var)
        window.update()
        time.sleep(.1)
        
def thread1(infile):
    global k,time_begin,stime,started
    #pid=os.getpid()
    #print(pid)
    #if started==False:
    #    os.kill(pid,3)
    #btn_start['state']='disabled'
    if not os.path.exists(dirDB):
        os.mkdir(dirDB)
    window.config(cursor="wait")
    
    lite.open_db(dirDB+'/')
    lite.open_db_2(dirDB+'/')
    lite.open_db_content(dirDB+'/')
     
    readXML(infile)
    lbl_progress.config(text = sepp(k))   
    par = 0
    if chk_state.get()==True:
        par = 1
        show_status("Сжатие баз")
    lite.close_db(par)
        
    tsec=time.time()-time_begin
    window.config(cursor="")
    started=False
    show_status('Затраченное время - ' + vremya(tsec) + ' Обработано {0} записей'.format(sepp(k)))
    k = 0
    #bar['value'] = 100
    #lbl_percent.config(text = "100 %")
    btn_csv['state']='normal'
    btn_start.config(text="Старт")
    window.update()

def starts(event):                        # Запуск основного процесса и таймера
    global k,time_begin,infile,started
    
    init_start()
    infile = xmlfile.get()
    if xml_test(infile):
        show_status('Создание БД')
        save_param()        
        time_begin=time.time()
        invers_category()
        btn_start.config(text="Стоп")
        btn_start.unbind('<ButtonPress>')
        btn_start.bind('<ButtonPress>',stops)
        started=True
        t = threading.Thread(target=time_status,args=(time_begin,infile))
        t.daemon=True
        t.start()
        lite.create_db(dirDB+'/')
        lite.create_db_2(dirDB+'/')
        lite.create_db_content(dirDB+'/')
        lite.ins_vers(period)
        lite.close_db(0)
        t1 = threading.Thread(target=thread1,args=(infile,))
        t1.start()
        #t1.join()
        if t1.is_alive==True:
            while 1:
                if t1.is_alive==False or started == False:
                    break
    else:
        pass

def stops(event):
    global started,stime
    started=False
    stime=False
    btn_start.unbind('<ButtonPress>')
    btn_start.bind('<ButtonPress>',starts)




def show_window():                      # главное окно программы
    global window, xmlfile, path_DB, btn_start, lbl_size, chk, lbl_progress, bar, lbl_percent, lbl_status, btn_csv
    global chk_state
    
    window = tk.Tk()
    window.title("XML > SQLite3")
    x = (window.winfo_screenwidth() - 450)/2
    y = (window.winfo_screenheight() - 450)/2
    window.geometry('450x204+%d+%d' % (x,y))
    window.resizable(width=False, height=False)
    pic = tk.PhotoImage(file='python.png')
    window.iconphoto(False,pic)

    window.columnconfigure(0, minsize=15, weight=1)
    window.columnconfigure(3, minsize=15, weight=1)
    window.rowconfigure(0,minsize=1)

    lbl = Label(window, text="Файл:")  
    lbl.grid(column=0, row=0,padx=5,sticky="e")

    xmlfile = Entry(window,width=35)  
    xmlfile.grid(column=1, row=0)

    photo1 = tk.PhotoImage(file = r"file.png").subsample(2, 2) 
    btn_open = tk.Button(window, image=photo1, command=select_file, width=15)
    btn_open.grid(column=2, row=0, padx=0, pady=5, sticky="w")

    lbl_size = Label(window, text="-", font=("arial", 10, "bold"))  
    lbl_size.grid(column=1, row=1,padx=5)

    lbl_DB = Label(window, text="Путь к DB:")  
    lbl_DB.grid(column=0, row=2,padx=5,pady=10,sticky="e")

    path_DB = Entry(window,width=35)
    path_DB.insert(0,"C:/DB")
    path_DB.grid(column=1, row=2,padx=5)

    photo2 = tk.PhotoImage(file = r"folder.png").subsample(2, 2) 
    btn_dir = tk.Button(window, image=photo2, command=SelectDir, width=15)
    btn_dir.grid(column=2, row=2, padx=0, pady=5, sticky="w")

    btn_start = tk.Button(window, text="Старт",cursor="")
    btn_start.grid(column=3, row=3, padx=5, pady=5, sticky="ew")
    btn_start.bind('<ButtonPress>',starts)
    btn_start['state']='disabled'
    

    chk_state = BooleanVar()  
    chk_state.set(True)  # 
    chk = Checkbutton(window, text='Сжать БД', var=chk_state)
    chk.grid(column=1, row=3, padx=0, pady=5)

    lbl_progress = Label(window, text="0")  
    lbl_progress.grid(column=0, row=4,padx=5,pady=5,sticky="e")

    bar = Progressbar(window, length=215)
    bar['value'] = 0
    bar.grid(column=1, row=4, padx=5, pady=5)

    lbl_percent = Label(window, text="0 %")  
    lbl_percent.grid(column=1, row=4, padx=5, pady=5)

    btn_csv = tk.Button(window, text="Сохранить в CSV",command=exportCSV)
    btn_csv.grid(column=3, row=4, padx=5, pady=5, sticky="ew")
    btn_csv['state']='disabled'

    statusbar = Entry(window,width=100)
    statusbar['state'] = 'readonly'
    statusbar.grid(column=0,columnspan=4, row=6,padx=0,pady=20)

    lbl_status = Label(window, text="")  
    lbl_status.grid(column=0,columnspan=5, row=6, padx=0, pady=20,sticky="w")
    
    backup=''
    for f in os.listdir('.'):
        if (f[:8] == 'backup.2' or f[:10] == 'rutracker-') and f[-3:] == 'xml':
            backup = f

    if len(backup)>0:
        up_select_file(backup)
    load_param()
    check_exist()

    window.mainloop()

# START PROG ->
if __name__ == '__main__':
    
    show_window()
