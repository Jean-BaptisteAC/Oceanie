"""
Ce script crée la base de données en téléchargeant les informations de Wikipédia (offline = False)
ou les extrait d'un fichier préalablement téléchargé( offline = True)
"""


import wptools
import re
import sqlite3
import os
import zipfile
import json

def get_info(country):
    page = wptools.page(country)
    page.get_parse(False)
    return page.data['infobox']


def offlne_parse():
    listdir = os.listdir()
    print(listdir)
    i = 1
    c = create_connection('BD_Appliwab.sqlite')
    create_table(c)
    for file in listdir:
        if file[-3:]=='zip':
            z_path = file
            z = zipfile.ZipFile('.dat\{}'.format(z_path))
            for jsn in z.namelist():
                info = json.loads(z.read(jsn))
                args = get_args(info,i)
                print(args)
                i += 1
                add_country(c, args)
                c.commit()



def reformat_coord(c):
    while len(c) < 3:
        c = c + ['0']
    return c


def reformat_pop(c):
    res=''
    for ch in c:
        try:
            if ch!=',':
                i=int(ch)
            res = res + ch
        except ValueError:
            break
    return res


def get_numbers(st):
    l=st.split('|')
    r = []
    for i in l:
        try:
            r.append(int(i))
        except:
            continue
    return r

def get_coord(info):
    try:
        c = info['coordinates']
        direction = {'E': 1, 'W': -1, 'N': 1, 'S': -1}
        D1 = 0
        D2 = 0
        f1 = True
        for ch in c:
            if ch in direction and f1:
                D1 = ch
                f1 = False
            if ch in direction and not f1:
                D2 = ch
        S = c.split(D1)
        s = reformat_coord(get_numbers(S[0]))
        E = S[1].split(D2)
        e = reformat_coord(get_numbers(E[0]))
        lat = (int(s[0]) + int(s[1]) / 60 + int(s[2]) / 3600) * direction[D1]
        lon = (int(e[0]) + int(e[1]) / 60 + int(e[2]) / 3600) * direction[D2]
        return {'lat': lat, 'lon': lon}
    except:
        return {'lat': '', 'lon': ''}



def get_name(info):
    try:
        name= info['common_name']
        if len(name)>80:
            return ""
        return name
    except KeyError:
        name = info['conventional_long_name']
        if len(name) > 80:
            return ""
        return name


def get_capital(info):
    try:
        capital = info['capital']
        capital = re.match("\[\[(\w+)\]\]", capital).group(1)
        return capital
    except :
        return info['common_name']


def get_population(info):
    try:
        return reformat_pop(info['population_census'])
    except KeyError:
        return reformat_pop(info['population_estimate'])


def get_flag(info):
    try:
        return info['image_flag'][:-3]+'png'
    except KeyError:
        return ''


def get_surface(info):
    try:
        return info['area_km2']
    except KeyError:
        return ''

def get_args(info,i):
    coord = get_coord(info)
    args = [i + 1,get_name(info), get_capital(info)
        , coord['lat'], coord['lon'], get_population(info)
        ,get_flag(info), get_surface(info)]
    return args

def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn


def create_table(conn):
    conn.execute(
        '''CREATE TABLE 
        info_country(key integer, name text, capital text,latitude text,longitude text, population
         text, flag text ,surface text)''')


def add_country(conn, args):
    req = '''INSERT INTO info_country (key, name , capital, latitude, longitude, population,
                 flag ,surface ) VALUES ({},"{}","{}","{}","{}","{}","{}","{}")'''.format(*args)
    conn.execute(req)



def main(offline):
    if offline:
        offlne_parse()
    else:
        c = create_connection('BD_Appliwab.sqlite')
        create_table(c)
        raw = open('countries', 'r')
        countries = raw.read().split('\n')
        for i in range(len(countries)):
            info = get_info(countries[i])
            args = get_args(info,i)
            print(args)
            add_country(c,args)


if __name__ == '__main__':
    main(True)
