#索引服务
import asyncio
import json
import tornado
import copy
import jieba
import datetime
from sqlalchemy import create_engine
from sqlalchemy import DateTime, Text, VARCHAR
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import sessionmaker

nginx = True
nginx_ip_key = "X-Real-IP"
dbpool = dict()
split_char = " "

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)

class Base(DeclarativeBase):
    pass

class KeyToId(Base):
    __tablename__ = "t_key_to_id"
    key: Mapped[str] = mapped_column(VARCHAR(50), primary_key=True)
    ids: Mapped[str] = mapped_column(Text())


class IdToKey(Base):
    __tablename__ = "t_id_to_key"
    id: Mapped[str] = mapped_column(primary_key=True)
    keys: Mapped[str] = mapped_column(Text())
    created_time: Mapped[DateTime] = mapped_column(
        DateTime(), default=datetime.datetime.now)
    modified_time: Mapped[DateTime] = mapped_column(
        DateTime(), default=datetime.datetime.now)
    ip: Mapped[str] = mapped_column(VARCHAR(30))

class FullText:
    def __init__(self, source: str):
        if dbpool.get(source):
            self.engine = dbpool[source]
            Base.metadata.create_all(self.engine)
        else:
            self.engine = create_engine("sqlite:///"+source+".db", echo=False)
            dbpool[source] = self.engine
            Base.metadata.create_all(self.engine)

    def __listostr(self, mylist: list):
        if mylist is None or len(mylist) == 0:
            return ''
        str = ''
        index = 0
        list = copy.deepcopy(mylist)
        while len(list) > 0:
            temp = list.pop(index)
            if len(list) == 0:
                str = str + temp
            else:
                str = str+temp+split_char
        return str

    def __merge(self, temp: list, list: list):
        result = []
        for id in list:
            if temp.count(id) > 0:
                result.append(id)
        return result

    def __isSymbol(self, s):
        for char in s:
            if not (char.isalpha() or char.isdigit() or ('\u4e00' <= char <= '\u9fff')):
                return False
        return True

    def __trset(self, list):
        keys = []
        for key in list:
            if key.strip() != "" and self.__isSymbol(key) == True:
                if keys.count(key) == 0:
                    keys.append(key)
        return keys

    def __createIndex(self, id: str, keys: list,  txt: str, ip: str):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        idtokey = session.query(IdToKey).filter_by(id=id).first()
        if idtokey is None:
            idtokey = IdToKey(id=id, keys=self.__listostr(
                keys), ip=ip)
            session.add(idtokey)
        else:
            idtokey.modified_time = datetime.datetime.now()
            keylist = idtokey.keys.split(split_char)
            idtokey.keys = self.__listostr(keys)
            keytoids = session.query(KeyToId).filter(
                KeyToId.key.in_(keylist)).all()
            if len(keytoids) > 0:
                for keytoid in keytoids:
                    myids = keytoid.ids.split(split_char)
                    myids.remove(id)
                    if len(myids) == 0:
                        session.delete(keytoid)
                    else:
                        keytoid.ids = self.__listostr(myids)

        keytoids = session.query(KeyToId).filter(
            KeyToId.key.in_(keys)).all()
        for keytoid in keytoids:
            if keys.count(keytoid.key) > 0:
                keys.remove(keytoid.key)
            list = keytoid.ids.split(',')
            if list.count(id) == 0:
                list.append(id)
                keytoid.ids = self.__listostr(list)
        if len(keys) > 0:
            for key in keys:
                keytoid = KeyToId(key=key, ids=id)
                session.add(keytoid)
        session.commit()

    def createIndex(self, id: str, txt: str, ip: str):
        cutkey = jieba.cut_for_search(txt)
        keys = self.__trset(cutkey)
        self.__createIndex(id, keys, txt, ip)

    def createIndex2(self, id: str, keys: list, txt: str, ip: str):
        keys = self.__trset(keys)
        self.__createIndex(id,  list(set(keys)), txt, ip)

    def deleteIndex(self, id: str):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        idtokey = session.query(IdToKey).filter_by(id=id).first()
        if idtokey is not None:
            session.delete(idtokey)
            keys = idtokey.keys.split(split_char)
            keytoids = session.query(KeyToId).filter(
                KeyToId.key.in_(keys)).all()
            for keytoid in keytoids:
                list = keytoid.ids.split(',')
                if list.count(id) > 0:
                    list.remove(id)
                    if len(list) > 0:
                        keytoid.ids = self.__listostr(list)
                    else:
                        session.delete(keytoid)
            session.commit()

    def query(self, keys: list):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        ids = None
        for key in keys:
            if key == "":
                continue
            keytoid = session.query(KeyToId).filter_by(key=key).first()
            if keytoid is None:
                return None
            else:
                temp = keytoid.ids.split(split_char)
                if ids == None:
                    ids = temp
                else:
                    ids = self.__merge(ids, temp)
        if len(ids) == 0:
            return None
        idtokeys = session.query(IdToKey).filter(IdToKey.id.in_(ids)).all()
        result = []
        for idtokey in idtokeys:
            showkeys = idtokey.keys.split(split_char)
            result.append(
                {"id": idtokey.id, "keys": showkeys})
        return result

    def clear(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        session.query(KeyToId).delete()
        session.query(IdToKey).delete()
        session.commit()


def responseOk(data=None):
    result = {}
    result["code"] = 0
    if data is not None:
        result["data"] = data
    return result

def responseErr(data: str, code=None):
    result = {}
    if code is None:
        result["code"] = -1
    else:
        result["code"] = code
    result["data"] = data
    return result

def containsKeys(obj, keys):
    if isinstance(obj, dict):
        return all(key in obj.keys() for key in keys)
    elif hasattr(type(obj), '__dict__'):
        return all(key in obj.__dict__ for key in keys)
    else:
        return False

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        target = self.get_query_argument("target", "")
        keys = self.get_query_argument("keys", "")
        if target == "":
            self.write(responseErr("target"))
            return
        if keys == "":
            self.write(responseErr("keys不能为空"))
            return
        fulltext = FullText(target)
        lkeys = keys.split(",")
        result = fulltext.query(lkeys)
        self.write(json.dumps(result, ensure_ascii=False, cls=BytesEncoder))

    def post(self):
        content = self.request.body
        target = self.get_query_argument("target", "")
        id = self.get_query_argument("id", "")
        keys = self.get_query_argument("keys", "")
        if target == "":
            self.write(responseErr("target不能为空"))
            return
        if id == "":
            self.write(responseErr("id不能为空"))
            return
        if content == "":
            self.write(responseErr("content不能为空"))
            return
            
        ip = ""
        if nginx == True:
            ip = self.request.headers.get(nginx_ip_key)
        else:
            ip = self.request.remote_ip

        fulltext = FullText(target)
        if len(keys) > 0:
            fulltext.createIndex2(
                id, keys, content, self.request.remote_ip)
        else:
            if content == "":
                fulltext.deleteIndex(id)
            else:
                fulltext.createIndex(
                    id, content, self.request.remote_ip)
        self.write(responseOk())



def make_app():
    return tornado.web.Application([
        (r"/index", IndexHandler),
    ])

async def main():
    port = 8888
    app = make_app()
    app.listen(port)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
