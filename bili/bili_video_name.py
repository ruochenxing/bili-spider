#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading

import pymysql
import requests
import pyquery

from bili import headers, connect, logger
from concurrent import futures

ignore_ids = []


def get_video_aid(col):
    conn = pymysql.connect(**connect)
    cur = conn.cursor()
    sql = "select v_aid from bili_video where v_name is null or v_name = '' order by {} desc limit 1000".format(col)
    cur.execute(sql)
    for _aid in cur.fetchall():
        yield _aid[0]


def get_video_name(aids):
    url = "https://www.bilibili.com/video/av{}"
    for i in aids:
        if i in ignore_ids:
            continue
        try:
            req = requests.get(url.format(i), headers=headers).text
            q = pyquery.PyQuery(req)
            yield {i: q("h1[title]").text()}
        except StandardError:
            pass


def run(names):
    conn = pymysql.connect(**connect)
    cur = conn.cursor()
    sql = "update bili_video set v_name = %s where v_aid = %s"
    for v_aid, v_name in names:
        try:
            cur.execute(sql, (v_name, v_aid))
        except StandardError:
            conn.rollback()
    conn.commit()
    conn.close()


def update_db_video_name(names):
    names_list = []
    for row in names:
        for v_aid, v_name in row.items():
            if v_name is None or len(v_name) == 0:
                print "add ignore", v_aid
                logger.info(v_aid)
                continue
            else:
                names_list.append((v_aid, v_name))
    new_list = [names_list[i:i + 100] for i in range(0, len(names_list), 100)]
    with futures.ThreadPoolExecutor(10) as executor:
        executor.map(run, new_list)


def init_ignore_id():
    global ignore_ids
    for line in open("monitor.log"):
        ignore_ids.append(int(line.split("-")[5].strip()))
