#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bili import cols
from bili.bili_video_name import *

if __name__ == "__main__":
	for _col in cols[1: -1]:
		init_ignore_id()
		video_aids = get_video_aid(_col)
		_names = get_video_name(video_aids)
		update_db_video_name(_names)
		print(_col, "DONE!!!")
	# conn.close()
