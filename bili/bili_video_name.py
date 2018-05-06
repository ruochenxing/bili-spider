#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading

import pymysql
import requests
import pyquery

from bili import headers, connect

conn = pymysql.connect(**connect)
cur = conn.cursor()

total = 1
lock = threading.Lock()


def get_video_aid(col):
    global cur
    sql = "select v_aid from bili_video where v_name is null or v_name = '' order by {} desc limit 100".format(col)
    cur.execute(sql)
    for _aid in cur.fetchall():
        yield _aid[0]


def get_video_name(aids):
    url = "https://www.bilibili.com/video/av{}"
    for i in aids:
        try:
            req = requests.get(url.format(i), headers=headers).text
            q = pyquery.PyQuery(req)
            yield {i: q("h1[title]").text()}
        except:
            pass


def update_db_video_name(names):
    global cur, conn
    sql = "update bili_video set v_name = %s where v_aid = %s"
    for row in names:
        for v_aid, v_name in row.items():
            try:
                cur.execute(sql, (v_name, v_aid))
            except:
                conn.rollback()
        conn.commit()

