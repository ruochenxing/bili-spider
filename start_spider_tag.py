#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bili.bili_video_tag import *
from concurrent import futures
import time

if __name__ == "__main__":
	create_db()
	print("启动爬虫，开始爬取数据")
	for i in range(11, 2015):
		begin = 1000 * i
		v_aids = get_video_aid(begin, begin + 1000)
		urls = ["https://api.bilibili.com/x/tag/archive/tags?callback=jqueryCallback_bili_4&aid={}&jsonp=jsonp&_={}".format(j, int(round(time.time() * 1000))) for j in v_aids]
		with futures.ThreadPoolExecutor(64) as executor:
			executor.map(run, urls)
		save_db()
	print("爬虫结束")
	conn.close()
