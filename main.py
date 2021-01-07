

#----------------------------------------------------------------------------------------

# system configuration information :

# Java 11.0.9 with jre 8
# Python 3.9.1/Tinkter
# Postgis Bundle 3.0.3 for PostgreSQL 10
# Psycopg2 driver 2.8.6
# Matplotlib 3.3.3

# database information :

# data.nodes
# it is assumed there is atleast 1 identification column uid, with variable
# number of other attributes columns
# it is assumed that 2 new columns added in this code (geo_point and geom) do not
# perviously exist

# data.edges
# it is assumed that there are 2 columns corresponding to uids from
# data.nodes table

# data.inputdcrules
# it is assumed that there are atleast 3 dcrule columns, identification, lhs and rhs
# it is assumed that rules have the format
# idc1 -- ((key1=val1)or(key2=val2))and(key3=val3) -- key4=val4

# data.inputrmrules
# it is assumed that there are atleast 3 rmrule columns, identification, condition
# it is assumed that rules have the format
# irm1 -- ((key1=val1)or(key2=val2))and(key3=val3)

# data.queries
# it is assumed that there are atleast 2 queries columns, identification, query where condition
# it is assumed that rules have the format
# q1 -- ((key1=val1)or(key2=val2))and(key3=val3)

#----------------------------------------------------------------------------------------

# importing libraries

import array
import copy
from copy import deepcopy
import functools
from functools import lru_cache
import logging
import matplotlib.pyplot as plt
import operator
import pickle
import psycopg2
from psycopg2 import pool
import queue
import re
import random
import string
import sys
import threading
from threading import Lock
from PIL import Image
import os
#----------------------------------------------------------------------------------------

# variable with scope on entire code

lock    = Lock()
fd      = dict()
tfd     = dict()
points  = dict()
qval    = dict()
rprov   = dict()
rmdat   = dict()
summar  = dict()
thresh  = 50
samplesiz = 10000
nodessiz = 50
print("----------------")
print("command line input ..")
print("database name[data to be imported is provided in *.sql file and "
      "can be imported using command \i 'C:/pathtofile/test.sql'] ..")
dbname = input()
#dbname = 'test'
print("username ..")
username = input()
#username = 'postgres'
print("password ..")
password = input()
#password = 'abc'
print("port number ..")
portnumber = input()
#portnumber = '5432'
print("path to directory where the images can be saved and gif generated ..")
imgpath = input()
#imgpath = 'C:\\Users\\abc\\OneDrive\\Desktop\\'

#----------------------------------------------------------------------------------------

# enable logging

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

#----------------------------------------------------------------------------------------

# connection pooling for the postgresql database
# min 1 connection, max 50 connections

pool = psycopg2.pool.SimpleConnectionPool(1, 50, dbname=dbname, port=portnumber,
                                          user=username, password=password, host='')

#----------------------------------------------------------------------------------------

# sql query to add spatial geopoint column representing visual display positions

@lru_cache(maxsize=64)
def sqlAGC():

    sqlQuery_addGeoColumn = """ alter table data.nodes add column geo_point POINT;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_addGeoColumn)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlAGC()

#----------------------------------------------------------------------------------------

# sql query to add spatial geopoint data representing visual display locations

@lru_cache(maxsize=64)
def sqlAGP():

    sqlQuery_agp = """ update data.nodes set geo_point = point(random()*(180 - (-180)-1)+(-180), random()*(180 - (-180)-1)+(-180));"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlAGP()

#----------------------------------------------------------------------------------------

# sql query to add geometry column representing visual display locations

@lru_cache(maxsize=64)
def sqlAGD():

    sqlQuery_agp = "alter table data.nodes add column geom geometry(POINT,4326);"

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlAGD()

#----------------------------------------------------------------------------------------

# sql query to add geometry data representing visual display locations

@lru_cache(maxsize=64)
def sqlAGM():

    sqlQuery_agp = "update data.nodes set geom = st_setsrid(st_makepoint(random()*(180 - (-180)-1)+(-180), random()*(180 - (-180)-1)+(-180)),4326);"

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlAGM()

#----------------------------------------------------------------------------------------

# sql queries to add indexes for geopoint and geometry columns

@lru_cache(maxsize=64)
def sqlgeos():

    sqlQuery_agp = "create index newgeoidx on data.nodes using gist (geom);"

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    sqlQuery_agp = "create index newgeoidx on data.nodes using gist (geo_point);"

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlgeos()

#----------------------------------------------------------------------------------------

# sql function to extract and check presence of substring

@lru_cache(maxsize=64)
def sqlcheck2():

    sqlQuery_agp = """CREATE OR REPLACE FUNCTION data.check2(IN col character varying,
                     IN start character varying,IN en character varying,
                     IN col1 character varying)
                     RETURNS integer
                     LANGUAGE 'plpgsql'
                     VOLATILE
                     PARALLEL UNSAFE
                     COST 100
                     AS $BODY$
                     DECLARE 
                       tmp varchar;
                       t integer = 0;
                     BEGIN
                         tmp =  (substring(col, position(start in col) + length(start), 
                             position(en in col) - position(start in col) - length(start)));
                         IF position(tmp in col1) > 0 THEN
                             RETURN 1;   
                         END IF;   
                     RETURN t;          
                     END;
                     $BODY$;
                  """

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlcheck2()

# ---------------------------------------------------------------------------------------

# sql function to handle keys in rules

@lru_cache(maxsize=64)
def sqlcheckkey5():

    sqlQuery_agp = """CREATE OR REPLACE FUNCTION data.checkkey5(IN col character varying)
                        RETURNS character varying
                        LANGUAGE 'plpgsql'
                        VOLATILE
                        PARALLEL UNSAFE
                        COST 100
                        AS $BODY$
                        DECLARE
                            t varchar = col;
                            count integer = 0;
                            keys varchar = '';
                            c varchar[];
                            vals varchar[];
                            x integer =0;
                            str varchar;
                            r varchar;
                        BEGIN
                         c = regexp_split_to_array(t,'='::text); 
                        for i in 1 .. cardinality(c) loop
                          if(position('(' in c[i]) > 0) then
                          keys = concat(keys, substring(c[i], position('(' in c[i])+1, length(c[i])));
                          keys = concat(keys,',');
                          end if;
                        end loop;  
                        return keys; 
                        END;
                        $BODY$;
                    """
    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlcheckkey5()

# ---------------------------------------------------------------------------------------

# sql function to count the number of ands and ors

@lru_cache(maxsize=64)
def sqlcnt():

    sqlQuery_agp = """CREATE OR REPLACE FUNCTION data.prob4(IN searchterm character varying)
                    RETURNS integer
                    LANGUAGE 'plpgsql'
                    VOLATILE
                    PARALLEL UNSAFE
                    COST 100
                    AS $BODY$
                    DECLARE 
                      t integer = 0;
                    BEGIN
                        if (((length(searchTerm)-length(REPLACE(searchTerm, 'and', '')))/length('and'))-((length(searchTerm)-length(REPLACE(searchTerm, 'or', '')))/length('or'))) > 0 then
                            t = 1;
                        end if;  
                        return t;
                    END;    
                    $BODY$;
                 """

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlcnt()

# ---------------------------------------------------------------------------------------

# sql function to handle values in rules

@lru_cache(maxsize=64)
def sqlcheckkey6():

    sqlQuery_agp = """CREATE OR REPLACE FUNCTION data.checkkey6(IN col character varying)
                        RETURNS character varying
                        LANGUAGE 'plpgsql'
                        VOLATILE
                        PARALLEL UNSAFE
                        COST 100
                        AS $BODY$
                        DECLARE
                        t varchar = col;
                        count integer = 0;
                        keys varchar ;
                        c varchar[];
                        vals varchar ;
                        x integer =0;
                        str varchar;
                        r varchar;
                        BEGIN
                        c = regexp_split_to_array(t,'='::text); 
                        for i in 1 .. cardinality(c) loop
                          if(position('"' in c[i]) > 0) then
                            vals = concat(vals, (substring(c[i], position('"' in c[i]) + length('"'), 
                                                    position(')' in c[i]) - position('"' in c[i]) - length('"'))));
                            vals = concat(vals,',');  
                          end if;
                        end loop;  
                        return vals;
                        END;
                        $BODY$;
                     """

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_agp)

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return

# sqlcheckkey6()

# ---------------------------------------------------------------------------------------

# class to read rmrules from the database

class selectrmrules:

    def __init__(self,calledfrom):
        self.calledfrom = calledfrom

    @lru_cache(maxsize=64)
    def selectrmrule(self):

        sqlQuery_rm = """ select inputrule from data.inputrmrules; """

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()
        cur.itersize = samplesiz

        cur.execute(sqlQuery_rm)
        res_rm = cur.fetchall()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

        for row in res_rm:
            yield row

# ---------------------------------------------------------------------------------------

# function to populate nodes for display

def selectnodes():

    rmrules = selectrmrules("selectnodes")

    for rm_condition in rmrules.selectrmrule():

        sqlQuery_populate = """select p1.uid, e.uid2, p1.geopoint from
                       data.nodes p1 tablesample system_rows("""+str(nodessiz)+""")
                       ,data.edges e
                       where p1.geom in
                       (select geom from data.nodes order by geom <-> (p1.geom)
                       limit ("""+str(nodessiz)+""")) and p1.uid = e.uid1
                       and """ + rm_condition[0].replace('\"','\'')+ """; """

    #sqlQuery_populate = """select p1.uid, e.uid2, p1.geopoint from
    #                       data.nodes p1 tablesample system_rows("""+str(nodessiz)+""")
    #                       ,data.edges e
    #                       where p1.geom in
    #                       (select geom from data.nodes order by geom <-> (p1.geom)
    #                       limit """+str(nodessiz)+""") and p1.uid = e.uid1 ;"""

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()
        cur.itersize = samplesiz

        cur.execute(sqlQuery_populate)
        lst = cur.fetchall()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

        if(len(lst) > 0):

            print("------------------------------")
            print("find nodes to be put on screen")
            #print(lst)
            print("------------------------------")

            yield lst

# ---------------------------------------------------------------------------------------

# function to compute levenshtein distance between strings

def levenshteinDistance(s1, s2):

    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)

    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]

        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))

        distances = distances_

    return distances[-1]

# ---------------------------------------------------------------------------------------

# function to showheatmap based on display spatial locations and quality scores

def showheatmap(dict1, dict2):

    len_dict1 = len(dict1)

    f1 = array.array('f',(0.1 for i in range(0,len_dict1)))
    f2 = array.array('f',(0.1 for i in range(0,len_dict1)))
    f3 = array.array('f',(0.1 for i in range(0,len_dict1)))

    for index, (key,val) in enumerate(dict1.items()):
        if ((str(val)).count("POINT")>0):
            tmp = ((val[0].replace("POINT(",'')).replace(")",'')).split(' ')

        else:
            tmp= ((val.replace("(", '')).replace(")", '')).split(',')

        f1[index] = float(tmp[0])
        f2[index] = float(tmp[1])

        if key in dict2.keys():
            f3[index] = float(dict2[key])

    #plt.scatter(x=f1[:],y=f2[:],c=f3[:])
    #plt.xlim(-180,+180)
    #plt.ylim(-180,+180)
    #plt.show()

    if(len(f1)>3 and len(f2)>3 and len(f3)>3):
        axe = plt.axes(projection='3d')
        axe.plot_trisurf(f1[:], f2[:], f3[:], cmap='jet', alpha=0.5)
        axe.set_xlabel("x-lat")
        axe.set_ylabel("y-lon")
        axe.set_zlabel("Quality")
        axe.view_init(elev=25, azim=-45)
        axe.set_xlim(-180, +180)
        axe.set_ylim(-180, +180)
        axe.set_zlim(0, +1)
        plt.savefig(imgpath + '\\imgs\\' + str(len(qval)) + '.png')

    pass

# ---------------------------------------------------------------------------------------

# function to compute centroids between records for display locations

@lru_cache(maxsize=64)
def computeCentroid(pt1, pt2):

    quer = """select st_astext(st_transform(st_centroid
                                           (st_union(st_setsrid(
                                            st_makepoint""" + pt1 + """,4326),
                                            st_setsrid(st_makepoint""" + pt2 + """,4326))),4326));"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(quer)
    newp = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    if ((str(newp[0])).count("POINT") > 0):
        newp[0] = (str(newp[0][0]).replace("POINT", '')).replace(" ", ",")

    return newp[0]

# ---------------------------------------------------------------------------------------

# function to find hop neighbors of nodes

@lru_cache(maxsize=64)
def findDepth(inputstr, valid):

    sqlQuery_recursive = """with recursive path as (
                                   select 1 as n, uid2 as u from data.edges where uid1 = """ + valid + """
                                   UNION ALL
                                   select n+1, e.uid2 from path, data.edges e where
                                   (path.u = e.uid1) and (strpos(\'""" + inputstr + """\'
                                   , cast(e.uid1 as varchar)) = 0)
                                   and (path.n<3))
                                   select n from path;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_recursive)
    depth = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return depth

# ---------------------------------------------------------------------------------------

# function to initialize move and display nodes on display

def moveAndDisplay():

    qap = str(prov[0])
    score = prov[1]
    rmp = prov[3]
    var2 = prov[6]

    threadlist = []

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute("select count(*) from data.inputrmrules;")
    tmp = cur.fetchall()[0][0]

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    q = queue.Queue()

    for i in range(1, tmp):
        display(qap, score, rmp, var2, q)

    switch = 0
    img1 = []
    imglist = list()
    dirFiles = os.listdir(imgpath + "imgs\\")
    dirFiles.sort(key=lambda f: int(re.sub('\D', '', f)))
    for file in dirFiles:
        if switch == 0:
            img1 = Image.open(imgpath + "imgs\\" + file)
            switch = 1
        imglist.append(Image.open(imgpath + "imgs\\" + file))

    img1.save(imgpath + "new.gif", save_all=True, append_images=imglist, duration=350,
              loop=0)

    os.system(imgpath + "new.gif")

    print("overall dynamic heatmap has been generated and saved as new.gif in path specified")

    return

# ---------------------------------------------------------------------------------------

# function to perform movement of nodes using record similarity and update heatmap display,
# and update quality scores

def display(qap,score,rmp,var2,q):

    for elem in selectnodes():
        keys = dict()
        indicator = False

        for vals in elem:
            keys[vals[0]] = vals[2]

            lock.acquire()

            ps_conn = pool.getconn()
            cur = (ps_conn).cursor()
            cur.itersize = samplesiz

            cur.execute("select geopoint from data.nodes where uid = " + str(vals[1]) + ";")
            getgeo = cur.fetchall()[0]

            cur.close()
            pool.putconn(ps_conn)

            lock.release()

            keys[vals[1]] = getgeo

            global points
            global qval

            lock.acquire()

            points[vals[0]] = vals[2]
            qval[vals[0]] = 0.01

            lock.release()

            print("-------------------------")
            print("number of nodes on screen " + str(len(qval)))
            #print("points " + str(points))
            #print("quality score " + str(qval))
            print("-------------------------")
            #showheatmap(points, qval)

            for key in keys:
                if ((qap).count(', (' + str(key) + ',') > 0):
                    if (score > 0):
                        if (key in qval and qval[key] != score):
                            print(str(key) + '-uid quality score changed from ..' + str(qval[key]) + ' to ' + str(score))

                        else:
                            print(str(key) + '-uid quality score assigned to new nodes ..' + str(score))

                        lock.acquire()
                        qval[key] = score
                        lock.release()

                        indicator = True

                else:
                    inputstr = ''.join(str(q[0]) + ',' for q in qap)
                    depth = findDepth(inputstr, str(key))

                    if depth[len(depth) - 1][0] == 3:
                        lock.acquire()

                        ps_conn = pool.getconn()
                        cur = (ps_conn).cursor()
                        cur.itersize = samplesiz

                        cur.execute("select * from data.nodes where uid = " + str(key) + " limit 1;")
                        tmpcount = float(((str(cur.fetchall()[0])).count('NULL')))

                        cur.close()
                        pool.putconn(ps_conn)

                        lock.release()

                        tmpval = float(tmpcount / (prov[4]))

                        if(tmpval>0):
                            if (key in qval and qval[key] != tmpval):
                                print(str(key) + '-uid quality score changed from ..' + str(qval[key]) + ' to ' + str(tmpval))
                            else:
                                print(str(key) + '-uid quality score assigned to new nodes ..' + str(tmpval))

                            lock.acquire()
                            qval[key] = tmpval
                            lock.release()

                            indicator = True
                    else:
                        tmpcount = float(depth[len(depth) - 1][0])
                        tmpval = float(tmpcount / 3)

                        if (tmpval > 0):
                            if (key in qval and qval[key] != tmpval):
                                print(str(key) + '-uid quality score changed from ..' + str(qval[key]) + ' to ' + str(tmpval))
                            else:
                                print(str(key) + '-uid quality score assigned to new nodes ..' + str(tmpval))

                            lock.acquire()
                            qval[key] = tmpval
                            lock.release()

                            indicator = True

                for rprov in rmp:
                    if (str(rmp.get(rprov))).count(',' + str(key) + ',') > 0:
                        tmp_list1 = (rmp.get(rprov)).split(',')

                        lock.acquire()
                        ps_conn = pool.getconn()
                        cur = (ps_conn).cursor()
                        cur.itersize = samplesiz

                        cur.execute("""select * from data.nodes where uid = """ + str(key) + """;""")
                        tmp_list2 = cur.fetchall()

                        cur.close()
                        pool.putconn(ps_conn)

                        lock.release()

                        for elem in tmp_list1:
                            if ((elem != '') and (str(points.keys())).count(', ' + elem + ',') > 0):

                                lock.acquire()

                                ps_conn = pool.getconn()
                                cur = (ps_conn).cursor()
                                cur.itersize = samplesiz

                                cur.execute("""select * from data.nodes where uid = """ + str(elem) + """;""")
                                tmp_list3 = cur.fetchall()

                                cur.close()
                                pool.putconn(ps_conn)

                                lock.release()

                                if (levenshteinDistance(tmp_list2[:-2], tmp_list3[:-2]) < 25):
                                    pt1 = tmp_list3[0][-2]
                                    if (key not in points):
                                        lock.acquire()
                                        points[key] = keys[key][0]
                                        lock.release()

                                    pt2 = points[key]

                                    newp = computeCentroid(pt1, pt2)

                                    if newp not in points.values():
                                        if (key in points):
                                            print(str(key) + '-uid position changed from ..' + str(
                                                points[key]) + ' to ' + str(newp))
                                        else:
                                            print(str(key) + '-uid position assigned to new nodes ..' + str(newp))

                                        lock.acquire()
                                        points[key] = newp
                                        lock.release()

                                        indicator = True

                    else:
                        for rmatch in var2:
                            if (str(var2.get(rmatch))).count(',' + str(key) + ','):
                                tmp_list1 = (var2.get(rmatch)).split(',')
                                for elem in tmp_list1:
                                    if ((elem != '') and (str(points.keys())).count(', ' + elem + ',') > 0):

                                        lock.acquire()
                                        ps_conn = pool.getconn()
                                        cur = (ps_conn).cursor()
                                        cur.itersize = samplesiz

                                        cur.execute("""select * from data.nodes where uid = """ + str(elem) + """;""")
                                        tmp_list2 = cur.fetchall()

                                        cur.close()
                                        pool.putconn(ps_conn)

                                        lock.release()

                                        if (key not in points):
                                            lock.acquire()
                                            points[key] = keys[key][0]
                                            lock.release()

                                        pt1 = points[key]
                                        pt2 = tmp_list2[0][-2]

                                        newp = computeCentroid(pt1, pt2)

                                        if (newp not in points.values()):
                                            if (key in points):
                                                print(str(key) + '-uid position changed from ..' + str(
                                                    points[key]) + ' to ' + str(newp))
                                            else:
                                                print(str(key) + '-uid position assigned to new node ..' + str(newp))
                                            lock.acquire()
                                            points[key] = newp
                                            lock.release()
                                            indicator = True
        if (indicator):
            q.put((points, qval))
            showheatmap(points, qval)
            print("-------------------------")
            print("number of nodes on display " + str(len(qval)))
            print("-------------------------")
    return

#moveAndDisplay(prov,points,qval)

# ---------------------------------------------------------------------------------------

# sql query to find database fds and tfds

@lru_cache(maxsize=64)
def fdquery(element1, element2):

    if (element1.count("geo") == 0 and element2.count("geo") == 0
            and element1 != element2):

        sqlQuery = """ select count(p1.*)/2 
                                    from data.nodes as p1 tablesample system_rows(""" +str(samplesiz)+"""), 
                                    data.nodes as p2 tablesample system_rows("""+str(samplesiz)+""")
                                    where p1."""
        sqlQuery += element1 + '=' + 'p2.' + element1 + ' and p1.' + element2 + '= p2.' + element2 +';'
       # sqlQuery += " and p1." + element1 + ' IS NOT NULL and p1.' + element2  + ' IS NOT NULL '
       # sqlQuery += " and p2." + element1 + ' IS NOT NULL and p2.' + element2  + ' IS NOT NULL;'
        #print(sqlQuery)

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()
        cur.itersize = samplesiz

        cur.execute(sqlQuery)
        number = cur.fetchall()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

        return number
    else:
        return None

# ---------------------------------------------------------------------------------------

# function to update global variables for fds and tfds

def funcfd(element1, element2):
    #global fd
    number = fdquery(element1, element2)

    if(number != None and number):
        #print(str(number[0][0]/25000))
        #print(number)
        if (number[0][0] / samplesiz) > 0.005:
            if element1 in fd :
                addedlist = fd.get(element1) + '    ' + element2

                lock.acquire()
                fd[element1] = addedlist
                lock.release()
            else:
                lock.acquire()
                fd[element1] = element2
                lock.release()

# ---------------------------------------------------------------------------------------

# function to initialize finding fds and tfds

def findDP():

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute("""select * 
                    from data.nodes limit 0;""")
    schema = [desc[0] for desc in cur.description]

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    threadlist = []
    schemacp = deepcopy(schema)

    for element1 in enumerate(schema):
        for element2 in enumerate(schemacp):
            if("geo" not in element1[1] and "geo" not in element2[1]
                    and element1[1] != element2[1]):

                t = threading.Thread(target=funcfd, args=(element1[1], element2[1]))
                threadlist.append(t)

    for thread in threadlist:
        thread.start()

    for thread in threadlist:
        thread.join()

    #global tfd

    for key, value in fd.items():
        vals = re.split(r'   ', value)
        for val in vals:
            val = val.replace(" ", '')
            if val in fd.keys():
                if key in tfd:
                    if (str(tfd.get(key))).count(fd.get(val)) == 0:
                        if str(key) != fd.get(val):
                            addedlist = tfd.get(key) + '    ' + fd.get(val)

                            lock.acquire()
                            tfd.update({key: addedlist})
                            lock.release()
                else:
                    if str(key) != fd.get(val):

                        lock.acquire()
                        tfd.update({key: fd.get(val)})
                        lock.release()

    return (fd,tfd)

print("----------------------")
print("started bootstrapping .. ")
fdtfd = findDP()
tfd = fdtfd[1]
fd = fdtfd[0]
print("----------------------")

# ---------------------------------------------------------------------------------------

# function to get schema attributes

def getAttribs():

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(""" select column_name from information_schema.columns 
            where table_name = 'nodes';""")
    schema = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    tmpstr = ''
    cnt = 1
    count = dict()

    for col in schema[1:-2]:
        switch = random.randint(0, 1)
        if (switch == 1):
            tmpstr += "sum(case when " + col[0] + " like '%NULL%' then 1 else 0 end), "
            count[col[0]] = 0
            cnt += 1

    tmpstr = tmpstr[:-2]

    return (len(schema), count, tmpstr)

# ---------------------------------------------------------------------------------------

# function to find database samples
@lru_cache(maxsize=64)
def smpQuery(var):

    sqlQuery_smp = """ select uid,  """ + var + """ from 
        data.nodes tablesample system_rows("""+str(samplesiz)+""") group by uid;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_smp)
    smp = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return smp

# ---------------------------------------------------------------------------------------

# function to create database indexes based on provenance

def crIndexes(dct):

    for key in dct:
        if dct[key] < 1000:
            lock.acquire()

            ps_conn = pool.getconn()
            cur = (ps_conn).cursor()
            cur.itersize = samplesiz

            cur.execute("create index p" + str(key) + " on data.nodes("+key+");")
            cur.close()

            pool.putconn(ps_conn)
            lock.release()

    return

# ---------------------------------------------------------------------------------------

# function to find QA provenance with user involvement
@lru_cache(maxsize=64)
def findQprov(inputstr):

    sqlQuery = """ select uid1 from data.edges r1 
                    tablesample system_rows("""+str(samplesiz)+""")
                    where r1.uid2 in (select r2.uid1 from data.edges r2 
                    where \'""" + inputstr + """\' like '%'
                    || r2.uid2 || '%');"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    results = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return results

# ---------------------------------------------------------------------------------------

# function to retrieve queries and count from the database
@lru_cache(maxsize=64)
def findQrs():

    sqlQuery1 = """ select query from data.queries;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery1)
    results1 = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    sqlQuery2 = "select count(*) from data.queries;"

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery2)
    results2 = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return (results1, results2)

# ---------------------------------------------------------------------------------------

# function to make lists of nodes and counts for QA provenance

def findQlist(qdata, qcount):

    qlist = dict()

    for condition in qdata:
        tmp = (str(condition).replace(')or(', ')#(')).replace(')and(', ')&(')
        xlist = tmp.split('&')
        ylist = []

        for idx, elem in enumerate(xlist):
            ylist.append(elem.split('#'))
        z = functools.reduce(operator.iconcat, ylist, [])
        rstring = ''

        for elem in z:
            rstring = re.search('\((.*?)=', elem)[0]
            rstring = (rstring.replace('=', '')).replace('(\'(', '')
            if rstring in qlist:
                qlist[rstring] += 1
            else:
                qlist[rstring] = 1

    fstring = ''

    for key in qlist:
        if qlist[key] > (int(qcount)) / 4:
            fstring += key + ','

    fstring = fstring[:-1]
    fstring = fstring.replace('(', '')

    return fstring

# ---------------------------------------------------------------------------------------

# function find provenance for rm with user involvement
#lru_cache(maxsize=64)
def findRProv(row, fqlist):

    sqlQuery = """ select uid,name,""" + str(fqlist) + """ 
            from data.nodes tablesample system_rows("""+str(samplesiz)+""") 
            where """ + str(row[0]).replace('"', '\'') + """;"""
    #print(sqlQuery)

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    rest = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return rest

# ---------------------------------------------------------------------------------------

# function to find similar uids in database based on rules
#@lru_cache(maxsize=64)
def rmdata(row):

    sqlQuery = """ select *
                from data.nodes  where """ + str(row[0]).replace('"', '\'') + """;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    results = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return results

# ---------------------------------------------------------------------------------------

# function to initialize making provenance

def computeProvenance():

    attribs = getAttribs()

    smp = smpQuery(attribs[2])
    cnt = attribs[1]

    tmplist = list()
    for row in smp:
        tmplist.append(row[0])
        n=0
        for key in attribs[1]:
            cnt[key] = cnt[key]+row[n+1]
            n+=1

    score = (20000 - max(cnt.values()))/20000
    #crIndexes(attribs[1])

    qprov = findQprov(str(tmplist).replace('\'', ''))

    qdata = findQrs()

    fqlist = findQlist(qdata[0], qdata[1][0][0])

    rmrules = selectrmrules('CP')
    irmrules = rmrules.selectrmrule()

    threadlist = []
    for row in irmrules:
        t = threading.Thread(target=rmfunc, args=(row, fqlist, rprov,rmdat))
        t.setDaemon(True)
        threadlist.append(t)

    for thread in threadlist:
        thread.start()

    for thread in threadlist:
        thread.join()

    return (qprov,score,tmplist,rprov,attribs[0], tmplist, rmdat)

# ---------------------------------------------------------------------------------------

# function to update rm provenance global variables

def rmfunc(row, fqlist, rprov,rmdat):

    rest = findRProv(row, fqlist)
    results = rmdata(row)

    if (len(rest) > 0):
        tmp = str(row[0]).replace('"', '\'')
        for i in range(len(rest)):
            if tmp == row[0].replace('\"',"\'") and tmp in rprov:
                rprov[tmp] = rprov[tmp] + "," + str(rest[i][0]) + ","
            else:
                rprov[tmp] = str(rest[i][0])

    if (len(results) > 0):
        tmp = str(row[0]).replace('"', '\'')
        for i in range(len(results)):
            if tmp in rmdat:
                rmdat[tmp] = rmdat.get(tmp) + "," + str(results[i][0]) + ","
            else:
                rmdat[tmp] = str(results[i][0])

print("----------------------")
print("bootstrapping midway .. ")
prov = computeProvenance()
print("done with bootstrapping")
print("----------------------")

# ---------------------------------------------------------------------------------------

# class for creating graphs

class Graph(object):

    # --------------------------------------------------------------------------

    # initializing
    def __init__(self):

        self._vertices = {}

        self._edges = {}

        self._neighbors = {}

        self._solutions = []

        self._matchHistory = []

    # --------------------------------------------------------------------------

    # add edges
    def addEdge(self, n, m):

        if isinstance(n, str):  # n is vid
            n = self._vertices[n]
        else:  # n is Vertex
            self.addVertex(n)

        if isinstance(m, str):  # m is vid
            m = self._vertices[m]
        else:  # m is Vertex
            self.addVertex(m)

        self._edges[n.id].append(m)
        self._neighbors[n.id].append(m)
        self._neighbors[m.id].append(n)

        n.degree = n.degree + 1
        m.degree = m.degree + 1

    # --------------------------------------------------------------------------

    # add vertices
    def addVertex(self, vertex):

        if vertex.id not in self._vertices:
            self._vertices[vertex.id] = vertex
            self._edges[vertex.id] = []
            self._neighbors[vertex.id] = []
        else:
            vertex = self._vertices[vertex.id]

        return vertex

    # --------------------------------------------------------------------------
    def deleteEdge(self, startVID, endVID):

        if startVID not in self._vertices or \
                endVID not in self._vertices:
            return False

        startVertex = self._vertices[startVID]
        endVertex = self._vertices[endVID]

        if endVertex not in self._edges[startVID]:
            # startVID does not point to endVID.
            return False

        self._edges[startVID].remove(endVertex)

        self._neighbors[startVID].remove(endVertex)
        self._neighbors[endVID].remove(startVertex)

        startVertex.degree = startVertex.degree - 1
        endVertex.degree = endVertex.degree - 1

        return True

    # --------------------------------------------------------------------------

    # deleting vertices
    def deleteVertex(self, vid):

        if vid not in self._vertices:
            return None

        # Remove any edges leading out of vid.
        for endVertex in self._edges[vid]:
            self.deleteEdge(vid, endVertex.id)

        # Remove any edges leading to vid.
        for startVID in self._vertices:
            self.deleteEdge(startVID, vid)

        # Remove vid from list of edges.
        self._edges.pop(vid)

        # Remove the vertex from any neighbor lists.
        for id in self._neighbors:
            if id in self._neighbors[id]: self._neighbors[id].remove(vid)
        self._neighbors.pop(vid)

        # Delete the vertex itself.
        return self._vertices.pop(vid)

    # --------------------------------------------------------------------------

    # retrieve edges
    @property
    def edges(self):

        for startVID in self._edges:
            for endVertex in self._edges[startVID]:
                startVertex = self._vertices[startVID]
                yield (startVertex, endVertex)

    # --------------------------------------------------------------------------

    # find vertices
    def findVertex(self, name):

        for vertex in self.vertices:
            if vertex.name == name:
                return vertex
        return None

    # --------------------------------------------------------------------------

    #check if there is edge
    def hasEdgeBetweenVertices(self, startVID, endVID):

        if startVID not in self._vertices or endVID not in self._vertices:
            return False
        endVertex = self._vertices[endVID]
        return endVertex in self._edges[startVID]

    # --------------------------------------------------------------------------

    # retrieve labels
    @property
    def labels(self):

        return [v.label for v in self.vertices]

    # --------------------------------------------------------------------------

    # retrieve names
    @property
    def names(self):

        return [v.name for v in self.vertices]

    # --------------------------------------------------------------------------

    # retrieve number of vertices
    @property
    def numVertices(self):

        return len(self.vertices)

    # --------------------------------------------------------------------------

    # retrieve vertices

    @property
    def vertices(self):

        return self._vertices.values()

    # --------------------------------------------------------------------------

    # perform search

    def search(self, q):

        logging.debug("In Graph.search")
        logging.debug("Searching for %s in %s" % (q, self))

        matches = dict()

        self._solutions = []
        if self._findCandidates(q):
            self._subgraphSearch(matches, q)

        logging.debug("Out Graph.search")
        return self._solutions

    # filter candidates
    def _filterCandidates(self, u):

        return [vertex for vertex in self.vertices if vertex.label == u.label]

    # --------------------------------------------------------------------------

    # find candidates

    def _findCandidates(self, q):

        logging.debug("In _findCandidates")
        if self.numVertices == 0 or q.numVertices == 0:
            logging.debug("No candidates")
            return False

        for u in q.vertices:
            u.candidates = self._filterCandidates(u)
            if len(u.candidates) == 0:
                logging.debug("No candidates")
                return False

        cand_str = ""
        for i in u.candidates:
            cand_str += str(i) + " "
        logging.debug('Raw candidates for %s are %s' % (u, cand_str))
        return True

    # --------------------------------------------------------------------------

    # find neighbors

    def _findMatchedNeighbors(self, u, matches):

        if u is None or matches is None or len(matches) == 0:
            return []

        return [neighborVertex for neighborVertex in self._neighbors[u.id] \
                if neighborVertex.id in matches]

    # --------------------------------------------------------------------------

    # check if joinable vertices and edges

    def _isJoinable(self, u, v, q, matches):


        # If nothing has been matched, then we can join u and v.
        if len(matches) == 0:
            return True

        neighbors = q._findMatchedNeighbors(u, matches)
        for n in neighbors:
            m = self._vertices[matches[n.id]]
            if q.hasEdgeBetweenVertices(u.id, n.id) and \
                    self.hasEdgeBetweenVertices(v.id, m.id):
                return True
            elif q.hasEdgeBetweenVertices(n.id, u.id) and \
                    self.hasEdgeBetweenVertices(m.id, v.id):
                return True
            else:
                return False

        return False

    # --------------------------------------------------------------------------

    # find unmatched vertices

    def _nextUnmatchedVertex(self, matches):

        for vertex in self.vertices:
            if vertex.id not in matches:
                return vertex
        return None

    # --------------------------------------------------------------------------

    # refine candidates

    def _refineCandidates(self, candidates, u, matches):

        new_candidates = []
        for c in candidates:
            if c.degree >= u.degree and c.id not in matches.values():
                new_candidates.append(c)

        return new_candidates

    # --------------------------------------------------------------------------

    #restore state

    def _restoreState(self, matches):

        return pickle.loads(self._matchHistory.pop())

    # --------------------------------------------------------------------------
    def __repr__(self):

        s = "digraph {\n"

        if len(self._vertices) == 1:
            # Only one vertex. Print it's name.
            for vertexID, vertex in self._vertices.items():
                s += "%s_%s" % (vertex.name, vertex.id)
        else:
            for vertexID, neighbors in self._edges.items():
                for neighbor in neighbors:
                    s += '%s_%s->%s_%s;\n' % (self._vertices[vertexID].name,
                                              self._vertices[vertexID].id, neighbor.name, neighbor.id)
                    # s += '%s->%s;\n' % (self._vertices[vertexID].name,
                    #    neighbor.name)

        s = s + "\n}"
        return s

    # --------------------------------------------------------------------------

    # function for subgraph search

    def _subgraphSearch(self, matches, q):

        logging.debug("In _subgraphSearch")

        if len(matches) == len(q.vertices):
            logging.debug('found a solution %s' % matches)
            self._solutions.append(copy.deepcopy(matches))
            return

            # Get the next query vertex that needs a match.
        u = q._nextUnmatchedVertex(matches)
        logging.debug('Next unmatched vertex is %s' % u)
        cand_str = ""
        for i in u.candidates:
            cand_str += str(i) + " "
        logging.debug('Raw candidates of %s are [%s]' % (u, cand_str))

        old_candidates = u.candidates

        u.candidates = self._refineCandidates(u.candidates, u, matches)
        cand_str = ""
        for i in u.candidates:
            cand_str += str(i) + " "
        logging.debug('Refined candidates are [%s]' % cand_str)

        # Check each candidate for a possible match.
        for v in u.candidates:
            logging.debug('Checking candidate %s for vertex %s' % (v, u))
            # Check to see u and v are joinable in self.
            if self._isJoinable(u, v, q, matches):
                logging.debug("%s is joinable to %s" % (v, u))

                # Yes they are, so store the mapping and try the next vertex.
                self._updateState(u, v, matches)
                logging.debug('Matches is now %s' % matches)
                self._subgraphSearch(matches, q)

                # Undo the last mapping.
                matches = self._restoreState(matches)


        u.candidates = old_candidates
        logging.debug("Out _subgraphSearch")

    # update state

    def _updateState(self, u, v, matches):
       self._matchHistory.append(pickle.dumps(matches))
       matches[u.id] = v.id

# ---------------------------------------------------------------------------------------

# class for graph vertices

class Vertex(object):

    def __init__(self, id, label=None, number=None):

        self.id    = id
        self.label = label
        self.number = number
        self.degree = 0      # used by Graph
        self.candidates = [] # used for Graph.search

    @staticmethod
    def makeName(label, number):

        if label is None and number is None:
            return ''
        if number is None:
            return label
        elif label is None:
            return str(number)
        else:
            return '%s%s' % (label, number)

    @property
    def name(self):

        return self.makeName(self.label, self.number)

    def __str__(self):

        return '<%s,%s%s>' % (self.id, self.label, self.number if self.number is not None else "")


# ---------------------------------------------------------------------------------------

# function to intialize transforms

def transform(input, output, operator_str, graph_rule, param):

    lim = random.randint(1, 5000000)
    itr = 0
    count=0

    #print(operator_str)
    while (itr < lim):

        res = FindOP(input,output)

        if(param==0):
            operator_str += '&'+str(count)+str(res[0])
        if(param==1):
            operator_str += "*"+str(count)+str(res[0])
        if(param==2):
            operator_str += "$"+str(count)+str(res[0])
        if(len(res[1]) == 0 or len(res[2])==0):
            return operator_str

        else:

            count += 1
            tmpx = str(transform(input,res[1],operator_str,graph_rule,1))
            tmpy = str(transform(input,res[2],operator_str,graph_rule,2))

            if(graph_rule.findVertex(str(operator_str)) == None):
                vr = Vertex(str(operator_str),'')
                graph_rule.addVertex(vr)

            if(graph_rule.findVertex(str(tmpx)) == None):
                vr = Vertex(str(tmpx),'')
                graph_rule.addVertex(vr)

            if(graph_rule.findVertex(str(tmpy)) == None):
                vr = Vertex(str(tmpy),'')
                graph_rule.addVertex(vr)

            graph_rule.addEdge(str(operator_str), str(tmpx))
            graph_rule.addEdge(str(operator_str), str(tmpy))

    return graph_rule

# ---------------------------------------------------------------------------------------

# function to find transform operator

def FindOP(input, output):

    lim = random.randint(1,5000000)
    itr = 0
    op = ''
    leftr = ''
    rightr = ''

    while (itr<lim):
        itr += 1
        op = genOP(input,output)
        execrt = exec(input,op)[-1]

        if (len(execrt)<len(input) and len(execrt) > 0.06*len(input)) and (levenshteinDistance(input,execrt) < 0.96*len(input)):
            l=len(execrt)
            leftr=output[:l]
            rightr=output[l:]
            break

    return (op,leftr,rightr)

# ---------------------------------------------------------------------------------------

# function to generate transform operator

def genOP(input,output):

    pc = string.punctuation
    lst = list()
    for elem in pc:
        if (str(input)).count(elem)>0:
            lst.append(elem)
    sp_ch = random.choice(lst)

    k = 1
    ln = 0

    while(k >= ln):
        k = random.randint(0,len(input))
        ln = random.randint(0, len(output))

    start = random.randint(0,k)

    i=0
    operator = dict()

    while i<5:
        r_tmp = random.randint(0,100)
        i+=1

        if r_tmp > 25 and r_tmp < 60:
            operator["SUBSTRING"]= str(start)+":"+str(ln)
        if r_tmp < 25:
            operator["SPLIT"] = sp_ch
        if r_tmp > 80 and r_tmp < 90:
            operator["CONSTANT"] = sp_ch
        if r_tmp > 90:
            operator["SELECTK"] = str(k)
        if r_tmp > 60 and r_tmp < 80:
            operator["CONCAT"] = ''

    final = list(tuple())
    for it in iter(operator):
        final.append((it,operator.get(it)))

    return final

# ---------------------------------------------------------------------------------------

# function to execute transform operators

def exec(inputstr, ops):
    tmp = list()
    tmp.append(inputstr)

    for elem in ops:

        if(elem[0] == "SPLIT"):
            if(elem[1] != ''):
                tmp_z = str(tmp[-1]).split(elem[1])
                if tmp_z != "NULL":
                    tmp.append(tmp_z)

        if (elem[0] == "SELECTK"):
            tmp_g=int(elem[1])
            if(int(len(str(tmp[-1]))) > tmp_g):
                tmp_z=(str(tmp[-1]))[tmp_g]
                tmp.append(tmp_z)

        if (elem[0] == "CONCAT"):
            if(len(tmp)>1):
                tmp_z = random.sample(range(len(tmp)),len(tmp))
                tmp_k=(str(tmp[tmp_z[0]])+''+str(tmp[tmp_z[1]]))
                tmp.append(str(tmp_k))

        if (elem[0] == "CONSTANT"):
            if(len(str(tmp[-1]))>1):
                tmp_z=str(tmp[-1]).rstrip(elem[1])
                tmp.append(tmp_z)

        if (elem[0] == "SUBSTRING"):
            tmp_z = elem[1].split(':')
            if(int(tmp_z[1]) != '0' and int(len(str(tmp[-1]))>int(tmp_z[0])) and int(len(str(tmp[-1]))>int(tmp_z[1]))):
                tmp_g=int(tmp_z[0])
                tmp_h=int(tmp_z[1])
                tmp_k=(str(tmp[-1]))[tmp_g:tmp_h+1]
                tmp.append(tmp_k)

    return tmp

# ---------------------------------------------------------------------------------------

# function to execute modified transform operators

def exec1(inputstr, ops):
    tmp = list()
    tmp.append(inputstr)

    for key in ops:

        if(key == "SPLIT"):
            if(ops.get(key) != ''):
                tmp_z = str(tmp[-1]).split(ops.get(key))
                if tmp_z != "NULL":
                    tmp.append(tmp_z)

        if (key == "SELECTK"):
            tmp_g=int(ops.get(key))
            if(int(len(str(tmp[-1]))) > tmp_g):
                tmp_z=(str(tmp[-1]))[tmp_g]
                tmp.append(tmp_z)

        if (key == "CONCAT"):
            if(len(tmp)>1):
                tmp_z = random.sample(range(len(tmp)),len(tmp))
                tmp_k=(str(tmp[tmp_z[0]])+''+str(tmp[tmp_z[1]]))
                tmp.append(str(tmp_k))

        if (key == "CONSTANT"):
            if(len(str(tmp[-1]))>1):
                tmp_z=str(tmp[-1]).rstrip(ops.get(key))
                tmp.append(tmp_z)

        if (key == "SUBSTRING"):
            tmp_z = (ops.get(key).split(':'))
            if(int(tmp_z[1]) != '0' and int(len(str(tmp[-1]))>int(tmp_z[0])) and int(len(str(tmp[-1]))>int(tmp_z[1]))):
                tmp_g=int(tmp_z[0])
                tmp_h=int(tmp_z[1])
                tmp_k=(str(tmp[-1]))[tmp_g:tmp_h+1]
                tmp.append(tmp_k)

    return tmp

# ---------------------------------------------------------------------------------------

# function that calls the transforms function

def process(input,output):
    #operator_dict = dict()

    graph_rule = Graph()

    operator_str = ''
    transform(input,output,operator_str,graph_rule,0)

    ed = list()
    fl = list()
    mx = 0
    if graph_rule.numVertices > 0 :

        for j in graph_rule.edges:
            if(len(str(j[1])) > mx):
                mx = len(str(j[1]))
                ed = j

        for j in graph_rule.edges:
            if str(j[0]) == str(ed[0]):

                if(len(fl)==0):
                    fl.append(str(j[0]))
                    tmp = str(j[1])[str(j[1]).index(str(j[0])[:-2])+len(str(j[0])[:-2]):]
                    fl.append(tmp)
                else:
                    tmp = str(j[1])[str(j[1]).index(str(j[0])[:-2]) + len(str(j[0])[:-2]):]
                    fl.append(tmp)
        #print(fl)
        yield fl
    #return(fl)

#process(ex[0],ex[1])

# ---------------------------------------------------------------------------------------

# function to extract key/vals from rules

def getkeyvals(record):

    str_x = ((record.split("*"))[0].replace(')or(', ')#(')).replace(')and(', ')&(').split('&')
    str_y = []

    for idx, elem in enumerate(str_x):
        str_y.append(elem.split('#'))
    str_z = functools.reduce(operator.iconcat, str_y, [])

    cons = dict()

    for elem in str_z:
        if (str(elem)).count('=")') == 0:
            strkey = re.search('\((.*?)=', elem)[0]
            strval = re.search('=(.*?)\)', elem)[0]
            strval = (strval.replace('="', '')).replace('")', '')
            strkey = (strkey.replace('=', '')).replace('(', '')
            cons[strkey] = strval

    return cons

# ---------------------------------------------------------------------------------------

# function to retrieve dcrules from database

class selectdcrules:

    def __init__(self, calledfrom):
        self.calledfrom = calledfrom

    @lru_cache(maxsize=64)
    def selectdcrule(self):

        sqlQuery_dc = """ select inputlhs from data.inputdcrules; """

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()

        cur.execute(sqlQuery_dc)
        res_dc = cur.fetchall()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

        for row in res_dc:
            yield row

# ---------------------------------------------------------------------------------------

# sql query to retrieve incomplete dc rules

def sqlquerimpute(condition):

    if (condition[0].count('=")') == 0):
        tmp = (condition[0].split("*"))[1]

        sqlQuery = """ select pdc.inputlhs, p.uid, concat(concat(inputlhs,'*'),inputrhs)
                           from data.inputdcrules pdc tablesample system_rows("""+str(samplesiz)+"""), 
                           data.nodes p 
                           where ( pdc.inputlhs like '%NULL%' ) 
                           and 
                           (select case when count(f)>1 then true else false end from
                           regexp_split_to_table(
                           data.checkkey5('""" + tmp + """'),',') as f 
                           where position(f in pdc.inputlhs)>0)
                           and (data.prob4('""" + tmp + """' ) = 1)
                           and p.uid in
                           (select uid from data.nodes where """ + tmp.replace('"', '\'') + """);"""

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()
        cur.itersize = samplesiz

        cur.execute(sqlQuery)
        res = cur.fetchall()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

        for row in res:
            yield row

# ---------------------------------------------------------------------------------------

# sql query to intialize imputation on incomplete dc rules

def ruleimpute():

    tmp = dict()
    # dcrules = selectdcrules("GNA")

    # for condition in dcrules.selectdcrule(cur):

    threadlist = []
    q = queue.Queue()
    tmp_l = list()

    for condition in alter():

        print("-----------------------------------")
        print("altered rule using transformation .." + str(condition))
        print("-----------------------------------")

        tmp_l.append(condition)
        tmp[condition] = tmp_l

        t = threading.Thread(target=imputfunc, args=(tmp, condition, q))
        threadlist.append(t)

    for thread in threadlist:
        thread.start()

    for thread in threadlist:
        thread.join()

    finalrules = set()

    for elem in q.queue:
        finalrules.add(elem)

    # con = condition.split('*')
    # finalrules.add((con[0],con[1]))

    for elem in finalrules:
        print("---------------------------------")
        print("imputed rule post transformation.." + str(elem))
        print("---------------------------------")
        yield elem

# ---------------------------------------------------------------------------------------

# function to perform imputation on incomplete dc rules

def imputfunc(tmp, condition, q):

    tmp_z = list()
    tmp_l = list()
    #con = condition.split('*')
    tmp_l.append(condition)

    for record in sqlquerimpute(tmp_l):
        if record[2] not in tmp:
            tmp_z.append(tmp_l[0])
            tmp[record[2]] = tmp_z
        else:
            tmp_z = tmp[record[2]]
            tmp_z.append(tmp_l[0])
            tmp[record[2]] = tmp_z
        tmp_z = []

    tmp_y = list()
    tmp_m = list()
    tmp_p = dict()
    tmp_dc = deepcopy(tmp)

    for rec in tmp_dc:
        tmp_v1 = getkeyvals(rec)

        for i in tmp_v1:
            if tmp_v1[i] == "NULL":
                tmp_m.append(i)

        for tmp_e in tmp_m:
            for tmp_x in tmp_dc[rec]:
                tmp_v2 = getkeyvals(tmp_x)
                if levenshteinDistance(tmp_v1, tmp_v2) < 100:
                    for tmp_r in tmp_v2:
                        if tmp_e == tmp_r and tmp_v2[tmp_r] != 'NULL':
                            if (tmp_r + "-" + tmp_v2[tmp_r]) not in tmp_p:
                                tmp_p[tmp_r + "-" + tmp_v2[tmp_r]] = 1
                            else:
                                tmp_p[tmp_r + "-" + tmp_v2[tmp_r]] += 1


            if len(tmp_p) > 0:
                tmp_w = sorted(tmp_p.items(), key=lambda tmp_p: tmp_p[1], reverse=True)
                fin = str(tmp_w[0]).split("-")
                fin[1] = fin[1].split('\'')

                if tmp_e != '' and tmp_e == fin[0].replace("('", ''):
                    s = (str(rec)).replace(tmp_e + "=" + "\"NULL\"", tmp_e + "=" + "\"" + fin[1][0] + "\"")
                    tmp_y.append(s)
                    # yield (s, con[1])
                    #tmp_s = s.split("*")[1]
                    #if not tmp_s:
                    #    s = s + '*' + (condition.split('*'))[1]
                    q.put(s)

        tmp_m = []
        tmp_p = {}
    # return tmp_y

# print(list(ruleimpute()))

# ---------------------------------------------------------------------------------------

# function to formulate examples

def exmake(res_ex, tmp_x, tmp_y, examplepair, q):

    for record1 in res_ex[0:]:
        for record2 in res_ex[1:]:
            if (levenshteinDistance(str(record1[tmp_x]), str(record2[tmp_x])) < 100):
                tst = (record1[tmp_y], record2[tmp_y])
                if (record1[tmp_y] != 'NULL' and record2[tmp_y] != 'NULL' and
                        record1[tmp_y] != record2[tmp_y]):
                    examplepair.append(tst)
                    # yield tst
                    q.put(tst)

# ---------------------------------------------------------------------------------------

# function to retrieve example candidates from database
#@lru_cache(maxsize=64)
def makeexamples(tmp_x, tmp_y, examplepair, conditionrm, conditiondc, q):

    sqlQuery_ex = """select p.*
                   from data.inputdcrules pdc, data.nodes p tablesample system_rows("""+str(samplesiz/100)+""")
                   where (data.check2(pdc.inputrhs,'(','=', """ + '\'' + conditionrm[0] + '\'' + """) = 1 ) 
                   and (data.prob4('""" + conditionrm[0] + """' ) = 0) 
                   and p.uid in
                   (select uid from data.nodes where """ + conditionrm[0].replace('"', '\'') + """
                   and """ + conditiondc[0].replace('"', '\'') + """ )
                   and (pdc.inputlhs like \'""" + conditiondc[0] + """\' )
                   ;"""

    # print(sqlQuery_ex)
    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_ex)
    res_ex = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    if (len(res_ex) > 0):
        # print(res_ex)
        for row in res_ex:
            q.put((row, tmp_x, tmp_y, examplepair))

# ---------------------------------------------------------------------------------------

# function to initialize generating examples that calls other functions

def genexamples():

    examplepair = list(tuple())
    global fd
    global tfd
    #val = fdtfd
    #fd = val[0]

    sqlQuery_sc = """ select column_name from information_schema.columns where table_name = 'nodes';"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_sc)
    res_sc = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    tmp = list()
    for elem in res_sc:
        tmp.append(elem[0])

    rmrules = selectrmrules("GNA")

    threadlist1 = []
    q1 = queue.Queue()

    for conditionrm in rmrules.selectrmrule():

        dcrules = selectdcrules("GNA")
        for conditiondc in dcrules.selectdcrule():
            for key in fd:
                tmp_x = tmp.index(key)
                # fd.get(key))
                if ('    ' in fd.get(key)):
                    tmp_tl = (fd.get(key)).split('    ')
                    for tmp_ec in tmp_tl:

                        tmp_y = tmp.index(tmp_ec)
                        t1 = threading.Thread(target=makeexamples,
                                              args=(tmp_x, tmp_y, examplepair, conditionrm, conditiondc, q1))
                        threadlist1.append(t1)
                else:

                    tmp_y = tmp.index(fd.get(key))
                    t1 = threading.Thread(target=makeexamples,
                                          args=(tmp_x, tmp_y, examplepair, conditionrm, conditiondc, q1))
                    threadlist1.append(t1)

    for thread in threadlist1:
        thread.start()

    for thread in threadlist1:
        thread.join()

    q2 = queue.Queue()
    threadlist2 = []
    itr = 0

    for item in q1.queue:
        tmp_k = []
        tmp_k.append(item[0])

        for elem in q1.queue:
            if (item != elem):

                tmp_k.append(elem[0])
                tmp_x = item[1]
                tmp_y = item[2]
                examplepair = item[3]
                # tmp_k = makeexamples(cur,conditionrm,conditiondc)

                t2 = threading.Thread(target=exmake, args=(list(tmp_k), tmp_x, tmp_y, examplepair, q2))
                threadlist2.append(t2)

    for thread in threadlist2:
        thread.start()

    for thread in threadlist2:
        thread.join()

    for elem in q2.queue:
        # tmp_list = elem
        # example = exmake(list(tmp_k), tmp_x, tmp_y,examplepair)
        # tmp_list = list(example)
        # print(elem)
        # for i in range(len(elem)):
        #    print(tmp_list[i])
        yield elem

    # return examplepair

# ---------------------------------------------------------------------------------------

# function to get concatenated rules from database
@lru_cache(maxsize=64)
def sqlquerconcat():

    sqlQuery = """ select concat(concat(inputlhs,'*'),inputrhs) from data.inputdcrules;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    res = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    for row in res:
        yield row

# ---------------------------------------------------------------------------------------

# function to get count from database meeting condition
#@lru_cache(maxsize=64)
def sqlquercount(condition):

    sqlQuery = """ select count(*) from data.nodes where """ + (condition[0].split('*')[0]).replace('"', '\'') + """;"""

    lock.acquire()
    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    res_c = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    for row in res_c:
        yield row

# ---------------------------------------------------------------------------------------

# function to initialize rule altering process

def alter():

    threadlist3 = []
    q = queue.Queue()
    lst = list(set(list(genexamples())))

    print("---------------")
    print("generated examples are .." + str(lst))
    print("---------------")

    lst = [('Obama, Barack, smith(67)', 'Barack Obama'), ('sdd, Badffgfdck, smfsdsth(643)', 'Bargfdfk Obsdfgma')]

    for ex in lst:
        t = threading.Thread(target=altercall, args=(ex, q))
        threadlist3.append(t)

    for thread in threadlist3:
        thread.start()
    for thread in threadlist3:
        thread.join()

    for elem in q.queue:
        yield elem

# ---------------------------------------------------------------------------------------

# function that is called by rule altering process

def altercall(ex, q):

    rules = list()

    if (ex[0] != ex[1]):
        while (True):
            tmp = process(ex[0], ex[1])
            tmp_lst = list(tmp)

            print("---------------")
            print("transform rule found is .." + str(tmp_lst))
            print("---------------")

            if (len(tmp_lst)) > 0:
                # print(tmp_lst)
                rules.append(tmp_lst[0])
                break


    modifieddcrules = list()
    cons=[]

    for condition in sqlquerconcat():
        tmpcount = sqlquercount(condition)
        if (int(list(tmpcount)[0][0]) == 0):
            cons = getkeyvals(condition[0])

        modifieddcrule = condition[0]

        for key in cons:
            tmp_v = cons.get(key)
            vtmp = '"' + cons.get(key) + '"'
            cond1 = re.search('\&' + str(0) + '\[' + '\(\'(.*?)\)]', rules[0][0])

            cond1 = (cond1[0].replace('&0[', '')).replace(']', '')
            lst = (((cond1.replace("'", '')).replace('(', '')).replace(')', '')).split(', ')

            op = makeoperator(lst)

            tmp1 = exec1(str(vtmp), op)[-1]
            tmp1 = ((str(tmp1).replace('[', '')).replace(']', '')).replace("\'", '')
            tmp_u = len(tmp1)
            tmp_g = '"' + str(tmp1)[:tmp_u] + '"'

            if (tmp_u > len(str(vtmp))):
                tmp_u = 0

            tmp_h = '"' + str(vtmp)[tmp_u:] + '"'
            vtmp2 = execute(tmp_g, rules[0][0], 0, 0)

            vtmp2 = ((str(vtmp2).replace('[', '')).replace(']', '')).replace("\'", '')
            vtmp3 = execute(tmp_h, rules[0][0], 0, 1)

            vtmp3 = ((str(vtmp3).replace('[', '')).replace(']', '')).replace("\'", '')
            vtmp = '"' + (str(vtmp2)).replace('"', '') + str(vtmp3).replace('"', '') + '"'

            if (vtmp != ""):
                cond1 = re.search('\*' + str(0) + '\[' + '\(\'(.*?)\)]', rules[0][1])
                cond1 = ((cond1[0].replace(',>', '')).replace('*0[', '')).replace(']', '')
                lst = (((cond1.replace("'", '')).replace('(', '')).replace(')', '')).split(', ')

                op = makeoperator(lst)

                tmp1 = exec1(str(vtmp), op)[-1]
                tmp1 = ((str(tmp1).replace('[', '')).replace(']', '')).replace("\'", '')
                tmp_u = len(str(tmp1))
                tmp_g = '"' + str(tmp1)[:tmp_u] + '"'

                if (tmp_u > len(str(vtmp))):
                    tmp_u = 0

                tmp_h = '"' + str(vtmp)[tmp_u:] + '"'

            if (vtmp != ""):
                cond1 = re.search('\$' + str(0) + '\[' + '\(\'(.*?)\)]', rules[0][2])
                cond1 = ((cond1[0].replace(',>', '')).replace('$0[', '')).replace(']', '')
                lst = (((cond1.replace("'", '')).replace('(', '')).replace(')', '')).split(', ')

                op = makeoperator(lst)

                tmp2 = exec1(tmp_h, op)[-1]
                tmp2 = ((str(tmp2).replace('[', '')).replace(']', '')).replace("\'", '')
                vtmp = '"' + str(tmp_g).replace('"', '') + str(tmp2).replace('"', '') + '"'

            if (str(vtmp) != "None"):
                modifieddcrule = modifieddcrule.replace(str(tmp_v), str(vtmp))
                modifieddcrules.append(modifieddcrule)

                modifieddcrule = re.sub('\"' + '{2,}', '\"', modifieddcrule)

                q.put(modifieddcrule)
                # yield modifieddcrule


# for r in modifieddcrules:
# print(r)
# return modifieddcrules

# ---------------------------------------------------------------------------------------

# function to perform execution of data transform on rules

def execute(st, tmp_r, cnt, tmp_s):

    vtmp = '"' + str(st).replace('"', '') + '"'

    if (tmp_s == 0) and (tmp_r.count('*' + str(cnt)) > 0):
        cond1 = re.search('\*' + str(cnt) + '\[' + '\(\'(.*?)\)\]', tmp_r)
        cond1 = (cond1[0].replace('*' + str(cnt) + '[', '')).replace(']', '')
        tmp_d = datamanipulate(cond1, vtmp, tmp_r, cnt)

        vtmp = tmp_d[0]
        cnt = tmp_d[1]

    if (tmp_s == 1) and (tmp_r.count('\$' + str(cnt)) > 0):
        cond1 = re.search('\$' + str(cnt) + '\[' + '\(\'(.*?)\)\]', tmp_r)
        cond1 = (cond1[0].replace('$' + str(cnt) + '[', '')).replace(']', '')
        tmp_d = datamanipulate(cond1, vtmp, tmp_r, cnt)

        vtmp = tmp_d[0]
        cnt = tmp_d[1]

    return vtmp

# ---------------------------------------------------------------------------------------

# function to make transform operator dictionary

def makeoperator(st):

    operator = dict()

    for elem in st:
        if elem.count("SUBSTRING") > 0:
            tmp_z = st[st.index("SUBSTRING") + 1]
            operator["SUBSTRING"] = tmp_z

        if st.count("SPLIT") > 0:
            tmp_z = st[st.index("SPLIT") + 1]
            operator["SPLIT"] = tmp_z

        if st.count("CONSTANT") > 0:
            tmp_z = st[st.index("CONSTANT") + 1]
            if (tmp_z == ''):
                operator["CONSTANT"] = ' '
            else:
                operator["CONSTANT"] = tmp_z

        if st.count("SELECTK") > 0:
            tmp_z = st[st.index("SELECTK") + 1]
            operator["SELECTK"] = tmp_z

        if st.count("CONCAT") > 0:
            tmp_z = st[st.index("CONCAT") + 1]
            operator["CONCAT"] = tmp_z

    return operator

# ---------------------------------------------------------------------------------------

# function to manipulate transforms string and call execution modules for altering rules

def datamanipulate(cond1, vtmp, tmp_r, cnt):

    lst = (((cond1.replace("'", '')).replace('(', '')).replace(')', '')).split(', ')
    op = makeoperator(lst)

    tmp1 = exec1(str(vtmp), op)[-1]
    tmp1 = ((str(tmp1).replace('[', '')).replace(']', '')).replace("\'", '')
    tmp_u = len(str(tmp1))
    tmp_g = '"' + str(tmp1)[:tmp_u] + '"'

    if (tmp_u > len(str(vtmp))):
        tmp_u = 0

    tmp_h = '"' + str(tmp1)[tmp_u:] + '"'
    cnt += 1

    vtmp2 = execute(tmp_g, tmp_r, cnt, 0)
    vtmp3 = execute(tmp_h, tmp_r, cnt, 1)

    vtmp = '"' + (str(vtmp2)).replace('"', '') + str(vtmp3).replace('"', '') + '"'

    return (vtmp, cnt)

# ---------------------------------------------------------------------------------------

# function for bitbloom filters

class BitBloomFilter(object):
    def __init__(self, m=1024, k=3):
        self.m = m
        self.k = k
        self.items = [0] * m

    def __repr__(self):
        return '<BloomFilter {}>'.format(self.items)

    def add(self, item):
        h = hash(item)

        for i in range(self.k):
            p = (h >> i) % self.m
            self.items[p] = 1

    def remove(self, item):
        h = hash(item)

        for i in range(self.k):
            p = (h >> i) % self.m
            self.items[p] = 0

    def has(self, item):
        h = hash(item)

        for i in range(self.k):
            p = (h >> i) % self.m

            if not self.items[p]:
                return False

        return True

# ---------------------------------------------------------------------------------------

# function for int bloom filters

class IntBloomFilter(object):
    def __init__(self, m=1024, k=3):
        self.m = m
        self.k = k
        self.items = [0] * m

    def __repr__(self):
        return '<BloomFilter {}>'.format(self.items)

    def add(self, item):
        h = hash(item)

        for i in range(self.k):
            p = (h >> i) % self.m
            self.items[p] += 1

    def remove(self, item):
        h = hash(item)

        for i in range(self.k):
            p = (h >> i) % self.m

            if not self.items[p]:
                raise ValueError('Bloom filtersfield is ZERO.Thisitemwasnot addedbefore.')

                self.items[p] -= 1

    def has(self, item):
        h = hash(item)

        for i in range(self.k):
            p = (h >> i) % self.m

            if not self.items[p]:
                return False

        return True

# ---------------------------------------------------------------------------------------

# find dcrules meeting conditions and and/or properties in conditions
#@lru_cache(maxsize=64)
def querlhs(condition):

    sqlQuery = """ select concat(concat(inputlhs,'*'),inputrhs)
                   from data.inputdcrules
                   where data.prob4('""" + condition + """') = 1;
                   """

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    res = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    if res:
        for row in res:
            yield row

# ---------------------------------------------------------------------------------------

# find keys meeting conditions
#@lru_cache(maxsize=64)
def quercheck(condition):

    tmp = condition.split("*")
    tmp_str = tmp[1]

    sqlQuery = """select ltrim(rtrim(data.checkkey5(
             '""" + tmp_str + """'),','),'(') ;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    rest = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return rest

# ---------------------------------------------------------------------------------------

# function to make database summary from input conditions
#@lru_cache(maxsize=64)
def quersummar(tmp_u, strn, rest, condition):

    tmp = condition.split("*")
    tmp_str = tmp[0]

    sqlQuery = """ select """ + tmp_u + """ from 
                   data.nodes p
                   where (levenshtein('""" + strn + """',
                   concat( \'""" + rest + """\')) 
                   < """ + str(thresh) + """) 
                   and (data.prob4('""" + tmp_str + """') = 1);
                   """
    #print(sqlQuery)

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    res = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    for row in res:
        yield row

# ---------------------------------------------------------------------------------------

# function to count rules meeting conditions
#@lru_cache(maxsize=64)
def quercnt(condition):

    tmp = condition.split('*')
    tmp_str = tmp[1]

    sqlQueryx = """ select count(*) from 
                                             data.nodes p
                                             where (data.prob4('""" + tmp_str + """') = 1);
                                             """

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQueryx)
    tmp_p = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    return tmp_p

# ---------------------------------------------------------------------------------------

# function to retrieve queries from database
@lru_cache(maxsize=64)
def querQs():

    sqlQuery = """ select query from data.queries;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    res = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    for row in res:
        yield row


def querQs2():

    sqlQuery = """ select query from data.queries;"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery)
    res = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    for row in res:
        yield row

# ---------------------------------------------------------------------------------------

# function to find counts of database items meeting conditions
def quercnt1(quer):

    if (quer[0].count('=")') == 0):
        sqlQuery = """ select count(uid) from data.nodes
                                where """ + (quer[0].replace('"', '\'')).replace('=',' ~ ') + """;
                           """

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()
        cur.itersize = samplesiz

        cur.execute(sqlQuery)
        res = cur.fetchall()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

        return res

# ---------------------------------------------------------------------------------------

# function to build summary of database for executing altered/imputed rules

def buildsummary(condition,cons):

    #dcrules = selectdcrules("SER")

    #for condition in dcrules.selectdcrule(cur):
    #for condition in prt1:

    for rec in querlhs(condition):
        record = list(rec)
        tmp_x = ((record[0].split("*"))[0].replace(')or(', ')#(')).replace(')and(', ')&(').split('&')
        tmp_y = []

        for idx, elem in enumerate(tmp_x):
            tmp_y.append(elem.split('#'))
        tmp_z = functools.reduce(operator.iconcat, tmp_y, [])

        for elem in tmp_z:
            if ('NULL' not in elem):
                strkey = re.search('\((.*?)=', elem)[0]
                strval = re.search('=(.*?)\)', elem)[0]
                strval = (strval.replace('="', '')).replace('")', '')
                strkey = (strkey.replace('=', '')).replace('(', '')
                test = ((re.search('\((.*?)=', record[0].split("*")[1])[0]).replace('=', '')).replace('(', '')

                if test in fd :
                    if (str(fd[test])).count(strkey) > 0:
                        cons[strkey] = strval

        #print("len of cons is .."+str(len(cons)))
        rest = quercheck(condition)
        tmp_r = list()

        if ((str(rest[0][0])).count('=') == 0):
            tmp_t = str(rest[0][0]).split(',')
            for tmp_k in tmp_t:
                if tmp_k in cons:
                    tmp_r.append(str(tmp_k) +',')
                    if tmp_k not in summar:

                        lock.acquire()
                        summar[tmp_k] = BitBloomFilter(75000)
                        lock.release()

            tmp_u=(((str(tmp_r).replace("[",'')).replace(',\'','')).replace(']','')).replace('\'','')

            tmp_t = (condition.replace(')or(', ')#(')).replace(')and(', ')&(')
            tmp_x = tmp_t.split('&')
            tmp_y = []

            for idx, elem in enumerate(tmp_x):
                tmp_y.append(elem.split('#'))
            tmp_z = functools.reduce(operator.iconcat, tmp_y, [])
            strn = ','

            for elem in tmp_z:
                if ('NULL' not in elem):
                    strn += re.search('=(.*?)\)', elem)[1] + ','
                if ('NULL' in elem):
                    strn += 'NULL' + ','

            strn = (strn.replace('="', '')).replace('")', '')

            tmp_w = (condition.split('*'))[1]
            lst1 = list()
            lst1.append(tmp_w)
            tmp_p = quercnt1(lst1)

            cnt1 = int(str(tmp_p[0][0]))

            tmp_x = tmp_u.split(',')

            tmp_rest = ((rest[0][0]).replace('(', '')).replace(')', '')
            counter = 0

            if tmp_rest in cons:
                for rec in quersummar(tmp_u, strn, cons[tmp_rest], condition):
                    record = list(rec)
                    for i in range(len(tmp_x)):
                        if(len(tmp_x[i])>0):

                            lock.acquire()
                            summar[(tmp_x[i]).replace(' ','')].add(record[i])
                            lock.release()
                            counter += 1

            global thresh
            if cnt1 > 0:
                thresh = thresh - (thresh * (counter) / cnt1)

        pass

# ---------------------------------------------------------------------------------------

# function to check smmary using tfds

def summarychecktfd(key,keys):

    tmp_y = list()
    if key not in keys:
        for tmp in tfd:
            if (str(tfd.get(tmp))).count(key) > 0 and tmp in keys:
                tmp_y.append(tmp)
    return tmp_y

# ---------------------------------------------------------------------------------------

# function to execute queries from database and count

def QueryExec():
    cnt = 0
    for quer in querQs():
        res = quercnt1(quer)
        if res:
            cnt += res[0][0]
    return cnt

# function to execute queries from database and count

def QueryExec2():

    cnt=0
    for quer in querQs2():
        res = quercnt1(quer)
        if res:
            cnt += res[0][0]
    return cnt

# ---------------------------------------------------------------------------------------

def syndata(rule):

    cons = getkeyvals(rule)
    sqlQuery_sc = """ select column_name from information_schema.columns where table_name = 'nodes';"""

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute(sqlQuery_sc)
    res_sc = cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    rhs = rule.split('*')[1]
    rhskey = re.search('\((.*?)=', rhs)[1]
    rhskey = (rhskey.replace('=','')).replace('(','')
    rhsval = re.search('=(.*?)\)', rhs)[1]
    rhsval = (rhsval.replace('=', '')).replace(')', '')

    tmp = list()
    for elem in res_sc:
        tmp.append(elem[0])

    cons1 = dict()
    for elem in tmp:
        if elem != rhskey and elem not in cons and elem != "uid" and elem != "geom" and elem != 'geopoint':
            lock.acquire()

            ps_conn = pool.getconn()
            cur = (ps_conn).cursor()
            cur.itersize = samplesiz
            cur.execute("select "+elem+" from data.nodes limit 1;")
            res = cur.fetchall()
            cur.close()
            pool.putconn(ps_conn)

            lock.release()
            cons1[elem] = res[0][0]

    cons1[rhskey] = 'NULL'

    lock.acquire()

    ps_conn = pool.getconn()
    cur = (ps_conn).cursor()
    cur.itersize = samplesiz

    cur.execute("select count(*) from data.nodes;")
    res=cur.fetchall()

    cur.close()
    pool.putconn(ps_conn)

    lock.release()

    cons1["uid"] = str(random.randint(int(res[0][0]), int(res[0][0])+int(res[0][0])))

    for key in cons:
        if ((str(cons[key])).strip()).isnumeric():
            tmpl = random.choice(string.digits)
            tmpr = random.choice(string.digits)
            cons1[key] = tmpl + cons[key] + tmpr
        if ((str(cons[key])).strip()).count('/') > 0 or ((str(cons[key])).strip()).count('-') > 0:
            tmpl = random.choice(string.digits)
            tmpr = random.choice(string.digits)
            cons1[key] = tmpl + cons[key] + tmpr
        else:
            tmpl = random.choice(string.ascii_lowercase)
            tmpr = random.choice(string.ascii_lowercase)
            cons1[key] = tmpl+cons[key]+tmpr

    if (str(cons[key])).count('=")') == 0 and (str(cons[key])).count(")") == 0 and (str(cons[key])).count("(") == 0:
        sqlupdatequery = """ insert into data.nodes ("""
        for key in cons1:
            sqlupdatequery += key + """, """
        sqlupdatequery += """geom, geopoint"""
        sqlupdatequery += """) values("""
        for key in cons1:
            sqlupdatequery += """ \'"""+cons1[key] + """\' , """
        sqlupdatequery +=  "st_setsrid(st_makepoint(random()*(180 - (-180)-1)+(-180), random()*(180 - (-180)-1)+(-180)),4326),"
        sqlupdatequery +=  "point(random()*(180 - (-180)-1)+(-180), random()*(180 - (-180)-1)+(-180))"
        sqlupdatequery += """);"""

        #print(sqlupdatequery)

        lock.acquire()

        ps_conn = pool.getconn()
        cur = (ps_conn).cursor()
        cur.itersize = samplesiz

        cur.execute(sqlupdatequery)
        ps_conn.commit()

        cur.close()
        pool.putconn(ps_conn)

        lock.release()

    repkey = list(cons.keys())[0]
    repval = cons[repkey]

    rulelhs = rule.split('*')[0]
    rulelhs = (rulelhs.replace(repkey,rhskey)).replace(repval,rhsval)

    rulelhs = re.sub('\"' + '{2,}', '\"', rulelhs)
    if (str(rulelhs)).count('=")') == 0 and (str(rulelhs)).count('")"') == 0 and (str(rulelhs)).count('"("') == 0:
        cons=getkeyvals(rulelhs)

        cons4 = dict()
        for key in cons:
            if key.count("\"") > 0:
                t = key.replace("\"",'')
                cons4[t] = cons[key]
            else:
                cons4[key] = cons[key]

        for key in cons4:
            if cons4[key].count("\"") > 0:
                t = cons4[key].replace("\"",'')
                rulelhs = rulelhs.replace(cons4[key],t)

        if rulelhs.count("and") > 0:
            lock.acquire()

            ps_conn = pool.getconn()
            cur = (ps_conn).cursor()
            cur.itersize = samplesiz
            cur.execute("select count(*) from data.queries;")
            res = cur.fetchall()
            cur.execute("insert into data.queries (qid,query) values (\'q"+str(res[0][0])+"\',\'"+rulelhs+"\');")
            ps_conn.commit()
            cur.close()
            pool.putconn(ps_conn)

            lock.release()

    pass

# function that is called by rule initialization and execution process

def computeRank(p,cons,q):

     #print("building summary ..")
     buildsummary(p,cons)
     #print("summary has been built .. ")
     #print("len of summary is .."+str(len(summar)))

     #cnt = 0
     #dcrules = selectdcrules("SER")

     #for rule in dcrules.selectdcrule(cur):
     rule = p
     tmp_x = ((rule.split("*"))[0].replace(')or(', ')#(')).replace(')and(', ')&(').split('&')
     tmp_y = []

     for idx, elem in enumerate(tmp_x):
         tmp_y.append(elem.split('#'))
     tmp_z = functools.reduce(operator.iconcat, tmp_y, [])
     cons = dict()

     for elem in tmp_z:
         if ('NULL' not in elem):
             strkey = re.search('\((.*?)=', elem)[1]
             strval = re.search('=(.*?)\)', elem)[1]
             strval = (strval.replace('="', '')).replace('")', '')
             strkey = (strkey.replace('=', '')).replace('(', '')
             cons[strkey] = strval

     cnttot = len(cons)
     cntcheck = 0

     for key in cons:
         if ((str(rule)).count(str(key))) > 0:
             tmp_y = summarychecktfd(key,summar.keys())
             if len(tmp_y) > 0:
                 for tmp_k in tmp_y:
                    if tmp_k in summar and tmp_k in cons:
                        if (summar[tmp_k]).has(cons[tmp_k]):
                            print("1.modified&imputed rule can be executed using tfd found after searching bloom filter.."+str(rule))
                            cntcheck += 1
                            syndata(rule)
                            q.put(rule)
             else:
                if key in summar:
                     if (summar[key]).has(cons[key]):
                         print("2.modified&imputed rule can be executed using fd found after searching bloom filter .."+str(rule))
                         syndata(rule)
                         q.put(rule)

         #print(str(cnttot)+"***"+str(cntcheck))
         #if cntcheck == cnttot:
             # print("found modified&imputed rule after searching bloom filters that can be executed.."+str(rule))
             #q.put(rule)
             #syndata(rule)

# ---------------------------------------------------------------------------------------

# function to initialize rule modification and execution

def run():

    #fd = val[0]
    cons = dict()
    threadlist = []
    q = queue.Queue()

    qcnt1 = QueryExec()

    for row in ruleimpute():
        t = threading.Thread(target=computeRank, args=(row, cons,q))
        threadlist.append(t)

    for thread in threadlist:
        thread.start()

    for thread in threadlist:
        thread.join()

    ind = False
    if q.empty() == False:
        for elem in q.queue:
            rhs = elem.split('*')[1]
            rhskey = re.search('\((.*?)=', rhs)[1]
            rhskey = (rhskey.replace('=', '')).replace('(', '')
            rhsval = re.search('=(.*?)\)', rhs)[1]
            rhsval = (rhsval.replace('=', '')).replace(')', '')
            lhs = elem.split('*')[0]
            lhs = (lhs.replace('=', ' ~ ')).replace('"', "\'")

            sqlquery = """ update data.nodes set """ + rhskey + """=\'""" + rhsval + """\'
                               where """ + lhs + """;"""
            # print(sqlquery)
            if sqlquery.count(')') == sqlquery.count('(') and (str(sqlquery)).count('=")') == 0:
                # print(sqlquery)

                lock.acquire()

                ps_conn = pool.getconn()
                cur = (ps_conn).cursor()
                cur.itersize = samplesiz

                cur.execute(sqlquery)

                ps_conn.commit()
                cur.close()
                pool.putconn(ps_conn)

                lock.release()

                sqlquery = """ select count(*) from data.nodes 
                                               where """ + lhs + """;"""

                lock.acquire()

                ps_conn = pool.getconn()
                cur = (ps_conn).cursor()
                cur.itersize = samplesiz

                cur.execute(sqlquery)
                res = cur.fetchall()

                cur.close()
                pool.putconn(ps_conn)

                lock.release()

                print(str(res[0][0]) + " number of records updated from the new rule " + str(elem))
                ind = True
            else:
                print("no records updated for these rules, try executing another time")

    if q.empty():
        print("no records updated, try executing another time")

    if ind:
        qcnt2 = QueryExec2()
        rk = (qcnt2 - qcnt1) / qcnt1
        print("---------------")
        print("overall rank .." + str(rk))
        print("---------------")

#print(list(run()))

# ---------------------------------------------------------------------------------------

# main function

if __name__ == '__main__':
    print("----------------")
    print("started display execution ..")
    moveAndDisplay()
    print("----------------")
    print("done with display and started rule correct, imputing and exec ..")
    run()
    print("process finished ..")
    print("----------------")