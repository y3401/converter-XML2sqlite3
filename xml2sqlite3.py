import tkinter as tk 
from tkinter.filedialog import askopenfilename, asksaveasfilename, askdirectory
from tkinter.ttk import Combobox, Checkbutton, Progressbar, Entry, Label
from tkinter import messagebox as mbox
from tkinter import BooleanVar
from xml.dom import minidom
import os, os.path
import modsql3_ as lite
import export_in_CSV as expCSV
import time
import re
import pickle


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
        pass
    
    
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
    dir_name = askdirectory(initialdir='C:')
    if dir_name == "":
        return None
    else:
        path_DB.delete(0,tk.END)
        if dir_name[-1:] != '/': dir_name = dir_name + '/'
        if dir_name[-3:] != 'DB/': dir_name = dir_name + 'DB'
        path_DB.insert(0,dir_name)
        
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
    #period = fl.split(".")[1][:8]
    catalog = './'+period
    window.title(f"XML > SQLite3 - {period}")
    init_start()
    #bar['value'] = 0
    #lbl_percent.config(text = "0 %")
    #lbl_status.config(text="")
    #lbl_progress.config(text="0")
    k = 0

def ttt(childList):
    global n, f_list,tid
    for c in childList:
        if c.nodeType!=c.TEXT_NODE:
            if c.nodeName=='dir':
                fileName = c.attributes.item(0).value
                n += 1
                f_list.append((tid, fileName,0,n))
            elif c.nodeName=='file':
                fileName = c.attributes.getNamedItem("name").value
                fileSize = c.attributes.getNamedItem("size").value
                n += 1
                f_list.append((tid, fileName,fileSize,n))
            cc = c.childNodes
            ttt(cc)

def extract(buff):
    global k,n,f_list,tid
    
    mydoc= minidom.parseString(buff)
    mydoc.normalize()
    node1 = mydoc.getElementsByTagName("torrent")[0]
    tid = node1.getAttribute("id")
    reg_date = node1.getAttribute("registred_at")
    b_size = node1.getAttribute("size")
    tl = mydoc.getElementsByTagName("title")[0]
    title = tl.childNodes[0].nodeValue
    title=title.replace('"',"'")
    node2 = mydoc.getElementsByTagName("torrent")[1]
    magnet = node2.getAttribute("hash")
    frm = mydoc.getElementsByTagName("forum")[0]
    forum_id = frm.getAttribute("id")
    forum = frm.childNodes[0].nodeValue
    forum=forum.replace('"',"'")
    cntnt = mydoc.getElementsByTagName("content")[0]
    content = cntnt.childNodes[0].nodeValue
    k += 1
    n=CAT_INV.get(forum.split(sep=" - ")[0],0)
    # если n=0 тогда внести в базу
    fr=forum.split(sep=" - ")[-1]
    if forums.get(forum_id,0) == 0: #добавление в словарь и в базу
        forums[forum_id] = ''
        lite.check_podr(forum_id,fr,n)  
    lite.ins_tor(forum_id,tid,magnet,title,b_size,reg_date)
    lite.ins_content(tid,content)

    n = 0
    f_list = []
    nodedir = mydoc.childNodes
    ttt(nodedir)
    lite.ins_files(tid,f_list)
    
'''    nodedir = mydoc.getElementsByTagName("dir")
    if len(nodedir)!=0:
        ndir = mydoc.getElementsByTagName("dir")[0]
        dirName = ndir.attributes.item(0).value
        n += 1
        a = (tid, dirName,0,n)
        f_list.append(a)
        childList=ndir.childNodes
        for child in childList:
            if child.nodeType!=child.TEXT_NODE:
                if child.nodeName=='dir':
                    fileName = child.attributes.item(0).value
                    n += 1
                    a = (tid, fileName,0,n)
                    f_list.append(a)
                elif child.nodeName=='file':
                    fileName = child.attributes.getNamedItem("name").value
                    fileSize = child.attributes.getNamedItem("size").value
                    n += 1
                    a = (tid, fileName,fileSize,n)
                    f_list.append(a)
                cnodes=child.childNodes
                for cn in cnodes:
                    if cn.nodeType!=cn.TEXT_NODE:
                        if cn.nodeName=='file':
                            fileName = cn.attributes.getNamedItem("name").value
                            fileSize = cn.attributes.getNamedItem("size").value
                            n += 1
                            a = (tid, fileName,fileSize,n)
                            f_list.append(a)      
    else:
        nodefile = mydoc.getElementsByTagName("file")
        if len(nodefile)!=0:
            nfile = mydoc.getElementsByTagName("file")[0]
            fileName = nfile.attributes.getNamedItem("name").value
            fileSize = nfile.attributes.getNamedItem("size").value
            n += 1
            a = (tid, fileName,fileSize,n)
            f_list.append(a)

    lite.ins_files(f_list)
'''

def invers_category():
    for la in lite.CAT:
        CAT_INV[la[1]]=la[0]

def show_status(message):
    lbl_status.config(text=message)
    window.update()

'''def sectXML(file):
    k = 1
    size_buff = 500000000 
    buff = ''
    buff1 = ''

    with open(file,"r",encoding='utf8') as fn:
        buff= fn.read(size_buff)
           
        while len(buff)>0:
            
            line= fn.readline()
            if not line:
                fout = open("1/" + file + "." + str(k),"w",encoding='utf8',newline ='\n')            
                fout.write(buff)
                fout.close()
                show_status(str(k))
                break
            #if not line.startswith('<torrent '): 
            buff1 += line
            if line.startswith('</torrent>'):
                fout = open("1/" + file + "." + str(k),"w",encoding='utf8',newline ='\n')            
                fout.write(buff + buff1)
                fout.close()
                show_status(str(k))
                k += 1
                buff = ''
                buff1 = ''
                buff= fn.read(size_buff)
            
                 
    fn.close()
'''
def readXML(file):
    key = 0
    buff = ''
     

    with open(file,encoding='utf8') as fn:
        
        while True:
            line= fn.readline()
            if not line:
                break
            if line.startswith('<torrent '):
                key=1
            if line.startswith('</torrent>') and key == 1:
                buff += line
                extract(buff)
                buff = ''
                key=0
                if k%1000==0:
                    lite.dbc()
                    lbl_progress.config(text = sepp(k))
                    window.update()
        
                if k%5000==0:
                    perc=round(100*fn.tell()/sz,3)
                    bar['value'] = perc
                    lbl_percent.config(text = str(perc) + " %")
                    window.update()
              
            if key==1:
                buff += line 
    fn.close()

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

def exportCSV():
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
    
def start():
    global k
    init_start()
    infile = xmlfile.get()
    if xml_test(infile):
        show_status('Создание БД')
        save_param()
        if not os.path.exists(dirDB):
            os.mkdir(dirDB)
        lite.create_db(dirDB+'/') 
        lite.create_db_content(dirDB+'/')
        lite.ins_vers(period)
        time_begin=time.time()
        invers_category()
        show_status(f"Обработка файла {infile}")
        #sectXML(xmlfile.get())
        readXML(xmlfile.get())
        lite.dbc()
        lbl_progress.config(text = sepp(k))
        #btn_start['state']='disabled'
        window.update()
        par = 0
        if chk_state.get()==True:
            par = 1
            show_status("Сжатие баз")
        lite.close_db(par)
        time_end=time.time()
        tsec=time_end-time_begin
        stsec=(str(tsec)).split('.')
        tsec=int(stsec[0])
        seconds=0
        minutes=0
        hours=0
        n=0
        seconds=tsec % 60
        minutes=(tsec//60) % 60
        hours=(tsec//3600) % 24
        show_status('Затраченное время - %s:%s:%s Обработано %s записей' % (str(hours),('0'+str(minutes))[-2:],('0'+str(seconds))[-2:], sepp(k)))
        k = 0
        bar['value'] = 100
        lbl_percent.config(text = "100 %")
        btn_csv['state']='normal'
    else:
        pass
    
def show_window():
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

    btn_start = tk.Button(window, text="Старт", command=start)
    btn_start.grid(column=3, row=3, padx=5, pady=5, sticky="ew")
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
    #statusbar.bind("<Key>", lambda e: "break")
    statusbar.grid(column=0,columnspan=4, row=6,padx=0,pady=20)

    lbl_status = Label(window, text="")  
    lbl_status.grid(column=0,columnspan=3, row=6, padx=0, pady=20,sticky="w")

    backup=''
    for f in os.listdir('.'):
        if (f[:8] == 'backup.2' or f[:10] == 'rutracker-') and f[-3:] == 'xml':
            backup = f

    if len(backup)>0:
        up_select_file(backup)
    load_param()
    
    window.mainloop()


# START PROG ->
if __name__ == '__main__':

    show_window()

