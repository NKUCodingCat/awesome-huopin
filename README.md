# 一个神奇小游戏的无聊外挂

`danju-gzipJson.ldb / shuangju-gzipJson.ldb` : processed data source
DATA struct: `{<key, char[2]>: <val, json{<array> 诗句}>}`

`json`: data from https://github.com/chinese-poetry/chinese-poetry/tree/master/json
`json-zhcn`: data of `json/` in simplified chinese

HOW TO RUN:

```
# Init 
python3 -m pip install -r req.txt

# prepare data (zh_hant/zh_hans => zh_hans)
python3 1-trans.py

# Generate database 
python3 4-json2LevelDB.py
```

Daily usage:
```
python3 99-Final-MainServer-Flask-LevelDB.py

# Then check 127.0.0.1:8080
```

### It is a project base on bug-oriented programming, take care of it

NOTE: key in shuangju DB is order-sensitve, but it is order-insensitive in danju DB