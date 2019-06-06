import plyvel
import utils2
import types
import re
import time

import timeout_decorator


g = types.SimpleNamespace()


g.shuangju = plyvel.DB("./shuangju-gzipJson.ldb", compression='snappy')
g.danju =  plyvel.DB("./danju-gzipJson.ldb", compression='snappy')


def danshuangTransfer(startKeys, targetKeys, mode):
    # 全部key都应该是顺序无关的

    if mode == "SHUANGSTART":
        startShuang = {i:list() for i in startKeys}
        startDan = {}
    elif mode == "DANSTART":
        startShuang = {}
        startDan = {i:list() for i in startKeys}
    else :
        raise Exception("Unknown MODE: <%s>"%mode)

    targetDan = {i:list() for i in targetKeys}
    targetShuang = {i:list() for i in targetKeys}

    print(targetDan, targetShuang, startDan, startShuang, sep="\n")


    tmpTargetDan    = set(targetDan.keys())
    tmpTargetShuang = set(targetShuang.keys())

    tmpStartDan    = set(startDan.keys())
    tmpStartShuang = set(startShuang.keys())

    while True:

        NEW_tmpTargetShuang = set()
        NEW_tmpTargetDan    = set()

        NEW_tmpStartShuang = set()
        NEW_tmpStartDan    = set()

        print("========================== ROUND 0 ======================")
        # print(targetDan, targetShuang, startDan, startShuang, sep="\n")
        # print(tmpTargetDan, tmpTargetShuang, sep="\n")
        # print(NEW_tmpTargetDan, NEW_tmpTargetShuang, sep="\n")

        printed = set()
        for i in tmpTargetDan:

            sen  = utils2.parseRedisValue(g.shuangju.get(bytes(i       , "utf8")))
            sen += utils2.parseRedisValue(g.shuangju.get(bytes(i[::-1] , "utf8")))
            sen  = set(sen)

            for j in sen:

                newKeys = utils2.shuangju2Keys(*re.split("，", j))
                newKeys = list(map(utils2.orderInsensitiveKeysCheck, newKeys))

                for nKey in newKeys:
                    if nKey not in targetShuang:
                        targetShuang[ nKey ] = [(j, nKey), ] + targetDan[i]  # (当前句子，来源词)
                        NEW_tmpTargetShuang.add(nKey) # 更新一下tmp
                
                if (NEW_tmpTargetShuang & set(startDan)) - printed :
                    z = (NEW_tmpTargetShuang & set(startDan)) - printed
                    for c in z:
                        yield(startDan[c], targetShuang[c])
                        printed.add(c)
                    # return

        print("========================== ROUND 1A ======================")
        # print(targetDan, targetShuang, startDan, startShuang, sep="\n")
        # print(tmpTargetDan, tmpTargetShuang, tmpStartDan, tmpStartShuang, sep="\n")
        # print(NEW_tmpTargetDan, NEW_tmpTargetShuang, sep="\n")


        printed = set()
        for i in tmpStartShuang:

            sen = utils2.parseRedisValue(g.danju.get(bytes(i, "utf8")))
            for j in sen: 
                newKeys = utils2.danju2Keys(j)

                for nKey in newKeys:
                    if nKey not in startDan:
                        startDan[nKey] = startShuang[i] + [(j, i), ]
                        NEW_tmpStartDan.add(nKey)

                if (set(targetShuang) & NEW_tmpStartDan) - printed :
                    z = (set(targetShuang) & NEW_tmpStartDan) - printed
                    for c in z:
                        yield(startDan[c], targetShuang[c])
                        printed.add(c)
                    # return
        print("========================== ROUND 2A ======================")
        # print(targetDan, targetShuang, startDan, startShuang, sep="\n")
        # print(tmpTargetDan, tmpTargetShuang, tmpStartDan, tmpStartShuang, sep="\n")
        # print(NEW_tmpTargetDan, NEW_tmpTargetShuang, sep="\n")


        printed = set()
        for i in tmpTargetShuang:

            sen  = utils2.parseRedisValue(g.danju.get(bytes(i, "utf8")))
            for j in sen: 
                newKeys = utils2.danju2Keys(j)

                for nKey in newKeys:
                    if nKey not in targetDan:
                        targetDan[nKey] = [(j, nKey),] + targetShuang[i]
                        NEW_tmpTargetDan.add(nKey)
                
                if (NEW_tmpTargetDan & set(startShuang)) - printed :
                    z = (NEW_tmpTargetDan & set(startShuang)) - printed
                    for c in z:
                        yield(startShuang[c], targetDan[c])
                        printed.add(c)
                    # return

        print("========================== ROUND 1B ======================")
        # print(targetDan, targetShuang, startDan, startShuang, sep="\n")
        # print(tmpTargetDan, tmpTargetShuang, tmpStartDan, tmpStartShuang, sep="\n")
        # print(NEW_tmpTargetDan, NEW_tmpTargetShuang, sep="\n")
            

        printed = set()
        for i in tmpStartDan:

            sen  = utils2.parseRedisValue(g.shuangju.get(bytes(i       , "utf8")))
            sen += utils2.parseRedisValue(g.shuangju.get(bytes(i[::-1] , "utf8")))
            sen  = set(sen)

            for j in sen:

                newKeys = utils2.shuangju2Keys(*re.split("，", j))
                newKeys = list(map(utils2.orderInsensitiveKeysCheck, newKeys))

                for nKey in newKeys:
                    if nKey not in startShuang:
                        startShuang[nKey] = startDan[i] + [(j, i), ]
                        NEW_tmpStartShuang.add(nKey)
                
                if (set(targetDan) & NEW_tmpStartShuang) - printed :
                    z = (set(targetDan) & NEW_tmpStartShuang) - printed
                    for c in z:
                        yield(startShuang[c], targetDan[c])
                        printed.add(c)
                    # return

        print("========================== ROUND 2B ======================")
        # print(targetDan, targetShuang, startDan, startShuang, sep="\n")
        # print(tmpTargetDan, tmpTargetShuang, tmpStartDan, tmpStartShuang, sep="\n")
        # print(NEW_tmpTargetDan, NEW_tmpTargetShuang, sep="\n")


        tmpTargetDan, tmpTargetShuang = NEW_tmpTargetDan, NEW_tmpTargetShuang
        tmpStartDan, tmpStartShuang = NEW_tmpStartDan, NEW_tmpStartShuang

        # return


S = ['惜香', '漂行', '夜步', '微阑', '夜漂', '歌香']
T = ['膺橐', ]

S = list(map(utils2.orderInsensitiveKeysCheck, S))
T = list(map(utils2.orderInsensitiveKeysCheck, T))



def grabDanshuang(S, T, MODE):
    c = danshuangTransfer(S, T, MODE)
    Res = set()
    
    def F():
        count = 0
        for i in c:
            if count > 400 or len(Res) > 30:
                break
            count += 1
            
            Ans = "/".join(list(map( lambda y: "/".join(list(map(lambda z: " %s => %s "%(z[1], z[0]), y))) , i)))
            if Ans not in Res:
                print(Ans)
                Res.add(Ans)
    
    try:
        timeout_decorator.timeout(10)(F)()
    except timeout_decorator.TimeoutError:
        WARN = "【警告】- 执行超时，数据可能不足\n"
        WARN += "\n===================================================================\n"
    except:
        WARN = "【错误】- 发生其他错误，调用信息：\n"
        import traceback; WARN += traceback.format_exc()
        WARN += "\n===================================================================\n"
    else:
        WARN = ""
    return WARN, Res
    


print(grabDanshuang(S,T, "SHUANGSTART"))
print(grabDanshuang(S,T, "DANSTART"))

    
    