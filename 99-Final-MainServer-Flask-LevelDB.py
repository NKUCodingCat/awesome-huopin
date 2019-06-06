import sys; sys.path.append(".")
import plyvel
from flask import request, g, Flask, request
import bottle
import traceback
import utils2
import logging
import sys
import re
from functools import reduce
import timeout_decorator

# ===== HOW TO RUN ==========
# docker run -v "/Users/cjingtao/P_jobs/lq:/opt/files" -p 8080:8080 -e "PYTHONIOENCODING=utf-8" -w "/opt/files" nkucodingcat/doushitaolu-env:latest python3 "-u" "9-MainServer-Flask-LevelDB.py"
# PYTHONIOENCODING=utf-8 python3 9-MainServer-Flask-LevelDB.py
# ===========================

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

@app.route('/taolu', methods=['GET',])
def getTaolu():
    ensure_db()
    return g.taoluTemplate.render({"danju": "", "shuangju":"", "taolu": ""})

@app.route('/taolu', methods=['POST',])
def postTaolu():

    ensure_db()

    taolu = request.form.get("taolu", None)

    if taolu == None:
        return getTaolu()

    try:
        taolu = utils2.orderSensitiveKeysCheck(taolu)
    except:
        danju = shuangju = traceback.format_exc()
    else:
        danju = "\n".join(utils2.parseRedisValue(g.danju.get(bytes(utils2.orderInsensitiveKeysCheck(taolu), "utf8"))))
        shuangju = "\n".join(utils2.parseRedisValue(g.shuangju.get(bytes(taolu, "utf8"))))
    
    return g.taoluTemplate.render({"danju": danju, "shuangju":shuangju, "taolu": taolu})

@app.route('/guodu', methods=['GET',])
def getGuodu():
    ensure_db()
    return g.guoduTemplate.render({"qishi1": "", "qishi2":"", "mubiao":"", "guodu":"", "leixing": "zheng"})

@app.route('/guodu', methods=['POST',])
def postGuodu():
    ensure_db()

    qishi1 = request.form.get("qishi1", "")
    qishi2 = request.form.get("qishi2", "")
    mubiao = request.form.get("mubiao", "")
    leixing = request.form.get("leixing", "zheng")

    Ret = {"qishi1": qishi1, "qishi2":qishi2, "mubiao":mubiao, "guodu":"", "leixing": leixing}

    if (not qishi1 and not qishi2) or (not mubiao):
        guodu = "【错误】起始句应当至少填写一句，目标词必须填写"
    else:
        if not qishi1:
            # 单句输出的都是排序后的key，应扩展至所有可能
            StartKeys = utils2.danju2Keys(qishi2)
            StartKeys = list(set(StartKeys + map(lambda x: x[::-1], StartKeys)))
        elif not qishi2:
            # 单句输出的都是排序后的key，应扩展至所有可能
            StartKeys = utils2.danju2Keys(qishi1)
            StartKeys = list(set(StartKeys + map(lambda x: x[::-1], StartKeys)))
        
        else:
            StartKeys = utils2.shuangju2Keys(qishi1, qishi2)
        
        print(Ret, "StartKeys = ", StartKeys)

        res = transfer(StartKeys, [mubiao, ], (leixing != "zheng"))

        if not res:
            guodu = "无法找到对应的过渡 --- 不存在含有关键字的诗句"
        else:
            guodu = "\n -------------------------- \n".join(
                                                        map(
                                                            lambda x: "\n".join(
                                                                map(lambda y: "  => ".join(y[::-1]), x)
                                                            ),
                                                            res
                                                        ))


    Ret["guodu"] = guodu
    return g.guoduTemplate.render(Ret)

@app.route('/danshuang', methods=['GET',])
def getDanshuang():
    ensure_db()
    return g.danshuangTemplate.render({"qishi1": "", "qishi2":"", "mubiao":"", "guodu":""})


@app.route('/danshuang', methods=['POST',])
def postDanshuang():
    ensure_db()
    ensure_db()

    qishi1 = request.form.get("qishi1", "")
    qishi2 = request.form.get("qishi2", "")
    mubiao = request.form.get("mubiao", "")

    Ret = {"qishi1": qishi1, "qishi2":qishi2, "mubiao":mubiao, "guodu":""}

    MODE = None
    if (not qishi1 and not qishi2) or (not mubiao):
        guodu = "【错误】起始句应当至少填写一句，目标词必须填写"
    else:
        if not qishi1:
            # 单句输出的都是排序后的key，应扩展至所有可能
            StartKeys = utils2.danju2Keys(qishi2)
            MODE = "DANSTART"
        elif not qishi2:
            # 单句输出的都是排序后的key，应扩展至所有可能
            StartKeys = utils2.danju2Keys(qishi1)
            MODE = "DANSTART"
        else:
            # 反正下一句是单句，无所谓的啦
            StartKeys = utils2.shuangju2Keys(qishi1, qishi2)
            StartKeys = list(map(utils2.orderInsensitiveKeysCheck, StartKeys))
            MODE = "SHUANGSTART"

        # StartKeys: 全部是 orderInsensitive

        print(Ret, "StartKeys = ", StartKeys, "MODE=", MODE)
        targetKeys = [utils2.orderInsensitiveKeysCheck(mubiao), ]

        W, Res = grabDanshuang(StartKeys, targetKeys, MODE)
        guodu = W + "\n" + "\n -------------------------- \n".join(Res)

    return g.danshuangTemplate.render({"qishi1": qishi1, "qishi2":qishi2, "mubiao":mubiao, "guodu":guodu})

def grabDanshuang(S, T, MODE):
    c = danshuangTransfer(S, T, MODE)
    Res = set()
    
    def F():
        count = 0
        for i in c:
            if count > 400 or len(Res) > 30:
                break
            count += 1
            
            Ans = re.sub("(^\n)|(\n$)", "", "\n".join(list(map( lambda y: "\n".join(list(map(lambda z: " %s => %s "%(z[1], z[0]), y))) , i))))
            if Ans not in Res:
                print(Ans, end = "\n"+"-"*60+"\n")
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



def transfer(startKeys, targetKeys, reverse = False):

    KeyCovert = (lambda x : x ) if not reverse else (lambda x : x[::-1])

    # 反向的操作: src == 反向 ==> 查询句子 == 关键词 ==> 新的搜索列表
    # 另一个方向: 新的搜索列表<== 反向 == 查询句子 <== 关键词 == dst

    TopDownDict = {i:list() for i in startKeys}
    ButtomUpDict = {i:list() for i in targetKeys}
    # count = 1
    

    while True:

        isUpdated = False

        for i in list(ButtomUpDict.keys()):
            sen = utils2.parseRedisValue(g.shuangju.get(bytes(i , "utf8")))
            for j in sen:
                newKeys = utils2.shuangju2Keys(*re.split("，", j))
                for nKey in newKeys:
                    if KeyCovert(nKey) not in ButtomUpDict:
                        ButtomUpDict[ KeyCovert(nKey) ] = [(j, KeyCovert(nKey)), ] + ButtomUpDict[i]  # (当前句子，来源词)
                        isUpdated = True
            
            M = set(ButtomUpDict)&set(TopDownDict)
            if M:
                ret = [ TopDownDict[m] + ButtomUpDict[m] for m in M ]
                ret = { "|".join([ entry[0] for entry in r ]) : r for r in ret }
                if len(ret) > 15:
                    return ret.values()
        
        if not isUpdated:
            print( "BottomUP 搜索 - ", targetKeys, "无法完成搜索。。。。没有新增句子。。。。。")
            return []


        isUpdated = False
        
        for i in list(TopDownDict.keys()):
            sen = utils2.parseRedisValue(g.shuangju.get(bytes( KeyCovert(i) , "utf8")))
            for j in sen:
                newKeys = utils2.shuangju2Keys(*re.split("，", j))
                for nKey in newKeys:
                    if nKey not in TopDownDict:
                        TopDownDict[ nKey ] = TopDownDict[i] + [(j, i), ]  # (当前句子，来源词)
                        isUpdated = True
            
            M = set(ButtomUpDict)&set(TopDownDict)
            if M:
                ret = [ TopDownDict[m] + ButtomUpDict[m] for m in M ]
                ret = { "|".join([ entry[0] for entry in r ]) : r for r in ret } 
                if len(ret) > 15:
                    return ret.values()
        
        if not isUpdated:
            print("TopDown 搜索 - ",targetKeys, "无法完成搜索。。。。没有新增句子。。。。。")
            return []


def danshuangTransfer(startKeys, targetKeys, mode):
    ensure_db()
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

def ensure_db():
    if 'shuangju' not in g:
        g.shuangju = plyvel.DB("./shuangju-gzipJson.ldb", compression=None)
    if 'danju' not in g:
        g.danju = plyvel.DB("./danju-gzipJson.ldb", compression=None)
    if "taoluTemplate" not in g:
        g.taoluTemplate = bottle.SimpleTemplate('''
        <head>
            <style> 
                .div-a{ float:left;width:45%;padding:10px;margin:1%;border:1px solid #000;white-space: pre} 
                .div-b{ float:left;width:45%;padding:10px;margin:1%;border:1px solid #000;white-space: pre} 
            </style> 
        </head>
        <form action="/taolu" method="post">
            请输入关键字（两个字， e.g. “套路” ）<input name="taolu" value="{{taolu}}" type="text" />
            <input value="查询" type="submit" />
        </form>
        <div class="div-a">{{danju}}</div> 
        <div class="div-b">{{shuangju}}</div> 
        ''')
    if "guoduTemplate" not in g:
        g.guoduTemplate = bottle.SimpleTemplate('''
        <head>
            <style> 
                .div-a{ float:left;width:45%;padding:10px;margin:1%;border:1px solid #000;white-space: pre} 
                .div-b{ float:left;width:45%;padding:10px;margin:1%;border:1px solid #000;white-space: pre} 
            </style> 
        </head>
        <form action="/guodu" method="post" id="mainform">
            请输入起始诗句（单句/双句）
            <input name="qishi1" value="{{qishi1}}" type="text" />
            <input name="qishi2" value="{{qishi2}}" type="text" /> 
            请选择过渡模式
            <select name="leixing" form="mainform" >
                <option value="zheng" 
%if leixing == "zheng":
selected
%end
>正向过渡</option>
                <option value="fan"
%if leixing == "fan":
selected
%end
>反复横跳</option>
            </select>
            <br />
            目标关键字（两个字， e.g. “目标” ）
            <input name="mubiao" value="{{mubiao}}" type="text">
            <input value="查询" type="submit" />
        </form>
        <div class="div-a">{{guodu}}</div> 
        ''')

    if "danshuangTemplate" not in g:
        g.danshuangTemplate = bottle.SimpleTemplate('''
        <head>
            <style> 
                .div-a{ float:left;width:45%;padding:10px;margin:1%;border:1px solid #000;white-space: pre} 
            </style> 
        </head>
        <form action="/danshuang" method="post" id="mainform">
            请输入起始诗句（单句/双句）
            <input name="qishi1" value="{{qishi1}}" type="text" />
            <input name="qishi2" value="{{qishi2}}" type="text" /> 
            <br />
            目标关键字（两个字， e.g. “目标” ）
            <input name="mubiao" value="{{mubiao}}" type="text">
            <input value="单双匹配Go!" type="submit" />
        </form>
        <div class="div-a">{{guodu}}</div> 
        ''')

def run_server():
 
    from gevent.pywsgi import WSGIServer

    sys.stdout.write("\n=============== Starting Flask Server ===== @ 0.0.0.0:8080 ==============\n")
 
    server = WSGIServer(("0.0.0.0", 8080), app)
    server.serve_forever()
 
 
if __name__ == '__main__':
    run_server()

