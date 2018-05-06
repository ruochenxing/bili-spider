#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import time
import re
import pymysql
import requests
import json

from bili import headers, connect, logger

conn = pymysql.connect(**connect)
cur = conn.cursor()

total = 1
result = []
lock = threading.Lock()


def run(url):
	# 启动爬虫
	global total
	aid = get_revalue(url, r'aid=([0-9]+?)&', 'aid not found', 1)
	html = requests.get(url, headers=headers, timeout=6).content
	html = html.replace('jqueryCallback_bili_4(', '')
	html = html.replace('})', '}')
	time.sleep(0.5)  # 延迟，避免太快 ip 被封
	req = json.loads(html)
	data = req['data']
	try:
		if data is not None and len(data) > 0:
			for tag_data in data:
				tag_one = (aid, tag_data['tag_id'], tag_data['content'], tag_data['hated'], tag_data['hates'], tag_data['likes'], tag_data['liked'], tag_data['tag_name'], tag_data['type'])
				with lock:
					result.append(tag_one)
			if total % 100 == 0:
				print(total)
				logger.info(total)
			total += 1
	except StandardError, e:
		print e.message


def create_db():
	# 创建数据库
	global cur
	cur.execute("""create table if not exists bili_tag
(
`v_aid` int(11) NOT NULL,
`tag_id` int(11) DEFAULT NULL,
`tag_content` varchar(500) DEFAULT NULL,
`tag_hated` int(11) DEFAULT NULL,
`tag_hates` int(11) DEFAULT NULL,
`tag_likes` int(11) DEFAULT NULL,
`tag_liked` int(11) DEFAULT NULL,
`tag_name` varchar(100) DEFAULT NULL,
`tag_type` int(11) DEFAULT NULL
)""")


def save_db():
	# 将数据保存至本地
	global result, cur, conn, total
	sql = "insert into bili_tag values(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
	for row in result:
		try:
			cur.execute(sql, row)
		except StandardError, e:
			print e.message
			conn.rollback()
	conn.commit()
	result = []


def get_revalue(html, rex, er, ex):
	v = re.search(rex, html)
	if v is None:
		print er
		if ex:
			raise er
		return ''
	return v.group(1)


def get_video_aid(start, end):
	global cur
	sql = "select v_aid from bili_video where v_aid > %s and v_aid < %s limit 1000"
	cur.execute(sql, (start, end))
	for _aid in cur.fetchall():
		yield _aid[0]
