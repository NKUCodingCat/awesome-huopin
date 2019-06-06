import plyvel
import glob
import ujson as json
import string
import zhon.hanzi
import re
import utils2
import time

# ======== DO NOT TOUCH ==================

class Unbuffered(object):
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def writelines(self, datas):
       self.stream.writelines(datas)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

import sys
sys.stdout = Unbuffered(sys.stdout)

# ======== DO NOT TOUCH ==================

level_dan = plyvel.DB("./danju-gzipJson.ldb", create_if_missing=True, compression=None)
level_shuang = plyvel.DB("./shuangju-gzipJson.ldb", create_if_missing=True, compression=None)

Data = glob.glob("./json-zhcn/poet.song.*.json") + glob.glob("./json-zhcn/poet.tang.*.json")

#======= 先制造集合，再转成正则 ==========

Comma = ",，。、！!？?" # 分隔符，逗号/顿号/句号/感叹号/问号
NotComma = "".join(list(set(zhon.hanzi.punctuation+string.punctuation) - set(Comma)))

Comma = "[%s]"%(re.escape(Comma))
NotComma = "[%s]"%(re.escape(NotComma))

print("分割符 in regex:", Comma)
print("非分割符 in regex:", NotComma)

#=====================================

Data = list(enumerate(Data))
Sliced_Data = [Data[i:i+128] for i in range(0,len(Data),128)]

for d in Sliced_Data: 
    
    # <Key>: <json string of list>
    Cache_Dan = {}
    Cache_Shuang = {}

    for idx, i in d:

        print(time.ctime(), "正在处理：%d/%d "%(idx+1, len(Data)), i, end=' - ')
        z = json.loads(open(i, "rb").read())
        for j in z:
            sentences = j.get("paragraphs", [])
            
            All = "".join(sentences)
            washed_All = re.sub(NotComma, "", All)
            splited_All = re.split(Comma, washed_All)

            for sen in [[splited_All[s], splited_All[s+1]] for s in range(len(splited_All) - 1)] :

                # 单句部分
                for danju in sen:
                    danjuKeys = utils2.danju2Keys(danju)
                    for k in danjuKeys:
                        # print(k, danju)
                        if k not in Cache_Dan:
                            Cache_Dan[k] = set()
                        Cache_Dan[k].add(danju)
                
                shuangjuKeys = utils2.shuangju2Keys(*sen)
                shuangjuVal = "，".join(sen)

                for k in shuangjuKeys:
                    # print(k, shuangjuVal)
                    # shuangPipe.sadd(k, shuangjuVal)
                    if k not in Cache_Shuang:
                        Cache_Shuang[k] = set()
                    Cache_Shuang[k].add(shuangjuVal)
    
        print(len(Cache_Dan.keys()), len(Cache_Shuang.keys()))

    print(time.ctime(), "缓存 <-同步-> LevelDB idx =", idx+1)

    for k, v in Cache_Dan.items():
        level_dan.put(bytes(k, "utf8"), utils2.mergeValue(level_dan.get(bytes(k, "utf8")), v))

    for k, v in Cache_Shuang.items():
        level_shuang.put(bytes(k, "utf8"), utils2.mergeValue(level_shuang.get(bytes(k, "utf8")), v))
    
