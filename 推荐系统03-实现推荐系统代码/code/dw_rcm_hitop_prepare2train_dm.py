#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import codecs
import random
import math
import time
import datetime

if __name__ == "__main__":

    random.seed(time.time())
	
    for l in sys.stdin:
        d = l.strip().split('\t')
        if len(d) != 21:
            continue

        # Extract data from the line
        label = d.pop(0)
        hitop_id = d.pop(0)
        screen = d.pop(0)
        ch_name = d.pop(0)
        author = d.pop(0)
        sversion = d.pop(0)
        mnc = d.pop(0)
        interface = d.pop(0)
        designer = d.pop(0)
        icon_count = d.pop(0)
        update_date = d.pop(0)
        stars = d.pop(0)
        comment_num = d.pop(0)
        font = d.pop(0)
        price = d.pop(0)
        file_size = d.pop(0)
        ischarge = d.pop(0)
        dlnum = d.pop(0)
		#下载记录
        hitopids = d.pop(0)
        device_name = d.pop(0)
        pay_ability = d.pop(0)

        # Construct feature vector
        features = []
        features.append(("Item.id,%s" % hitop_id, 1))
        features.append(("Item.screen,%s" % screen, 1))
        features.append(("Item.name,%s" % ch_name, 1))
        features.append(("All,0",1))
        features.append(("Item.author,%s" % author, 1))
        features.append(("Item.sversion,%s" % sversion, 1))
        features.append(("Item.network,%s" % mnc, 1))
        features.append(("Item.dgner,%s" % designer, 1))
        features.append(("Item.icount,%s" % icon_count, 1))
        features.append(("Item.stars,%s" % stars, 1))
        features.append(("Item.comNum,%s" % comment_num,1))
        features.append(("Item.font,%s" % font,1))
        features.append(("Item.price,%s" % price,1))
        features.append(("Item.fsize,%s" % file_size,1))
        features.append(("Item.ischarge,%s" % ischarge,1))
        features.append(("Item.downNum,%s" % dlnum,1))

        ####User.Item and User.Item*Item
        idlist = hitopids.split(',')
        idCT = 0;
        for id in idlist:
			#为什么取前三个？  5个   全部取     效率 准确率
            features.append(("User.Item*Item,%s" % id +'*'+hitop_id, 1))
            idCT += 1
            if idCT >= 3:
                break;
				
				
		#组合特征
        features.append(("User.phone*Item,%s" % device_name + '*' + hitop_id,1))
		#组合特征  （1）根据经验  （2）随机组合
        features.append(("User.pay*Item.price,%s" % pay_ability + '*' + price,1))

        # Output
        output = "%s\t%s" % (label, ";".join([ "%s:%d" % (f, v) for f, v in features ]))
        print output
        
        
        
        
        