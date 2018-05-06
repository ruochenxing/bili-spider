#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bili.bili_video_info import *
from concurrent import futures

if __name__ == "__main__":
    create_db()
    print("启动爬虫，开始爬取数据")
    for i in range(18, 2015):
        begin = 10000 * i
        urls = ["http://api.bilibili.com/archive_stat/stat?aid={}".format(j)
                for j in range(begin, begin + 10000)]
        with futures.ThreadPoolExecutor(64) as executor:
            executor.map(run, urls)
        save_db()
    print("爬虫结束，共为您爬取到 {} 条数据".format(total))
    conn.close()