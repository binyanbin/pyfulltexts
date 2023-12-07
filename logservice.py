#日志服务
import asyncio
import datetime
import json
import tornado
import copy
from operator import and_
import jieba
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import DateTime, Text, VARCHAR
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import sessionmaker

dbpool = dict()
split_char = " "
nginx = False
nginx_ip_key = "X-Real-IP"

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)

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

class Base(DeclarativeBase):
    pass

class KeyToId(Base):
    __tablename__ = "t_key_to_id"
    key: Mapped[str] = mapped_column(VARCHAR(50), primary_key=True)
    ids: Mapped[str] = mapped_column(Text())


class IdToKey(Base):
    __tablename__ = "t_id_to_key"
    id: Mapped[str] = mapped_column(primary_key=True)
    content: Mapped[str] = mapped_column(VARCHAR(1000))
    createdTime: Mapped[DateTime] = mapped_column(
        DateTime(), default=datetime.now, index=True)
    traceId: Mapped[str] = mapped_column(VARCHAR(100), index=True)
    ip: Mapped[str] = mapped_column(VARCHAR(30))


class Count(Base):
    __tablename__ = "t_count"
    max_id: Mapped[int] = mapped_column(primary_key=True)


class LogDb:
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

    def __isSymbol(self,s):
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

    def write(self, traceId: str, txt: str, ip: str, createdTime: datetime):
        cutkey = jieba.cut_for_search(txt)
        keys = self.__trset(cutkey)
        Session = sessionmaker(bind=self.engine)
        session = Session()
        count = session.query(Count).first()
        id = 1
        if count == None:
            session.add(Count(max_id=id))
        else:
            count.max_id = count.max_id + 1
            id = count.max_id
        idtokey = IdToKey(id=id, content=txt, ip=ip,
                          traceId=traceId, createdTime=createdTime)
        session.add(idtokey)
        keytoids = session.query(KeyToId).filter(
            KeyToId.key.in_(keys)).all()
        for keytoid in keytoids:
            if keys.count(keytoid.key) > 0:
                keys.remove(keytoid.key)
            list = keytoid.ids.split(',')
            if list.count(str(id)) == 0:
                list.append(str(id))
                keytoid.ids = self.__listostr(list)
        if len(keys) > 0:
            for key in keys:
                keytoid = KeyToId(key=key, ids=id)
                session.add(keytoid)
        session.commit()

    def clearLog(self, id: str,createdTime:datetime):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        idtokeys = session.query(IdToKey).filter(IdToKey.createdTime<createdTime).all()
        if len(idtokeys)>0:
            for idtokey in idtokeys:
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

    def query(self, keys: str, begin: datetime, end: datetime, traceId=None):
        keys = self.__trset(keys.split(","))
        Session = sessionmaker(bind=self.engine)
        session = Session()
        ids = None
        if len(keys) > 0:
            keytoids = session.query(KeyToId).filter(
                KeyToId.key.in_(keys)).all()
        else:
            return None
        for keytoid in keytoids:
            temp = keytoid.ids.split(split_char)
            if ids == None:
                ids = temp
            else:
                ids = self.__merge(ids, temp)
        if ids == None or len(ids) == 0:
            return None
        if traceId == None:
            idtokeys = session.query(IdToKey).filter(
                and_(IdToKey.createdTime.between(begin, end), IdToKey.id.in_(ids))).all()
        else:
            idtokeys = session.query(IdToKey).filter(
                and_(IdToKey.createdTime.between(begin, end), IdToKey.id.in_(ids), IdToKey.traceId == traceId)).all()
        result = []
        for idtokey in idtokeys:
            result.append(
                {"traceId": idtokey.traceId, "createdTime": str(idtokey.createdTime), "content": idtokey.content})
        return result

    def clear(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        session.query(KeyToId).delete()
        session.query(IdToKey).delete()
        session.commit()


class LogHandler(tornado.web.RequestHandler):
    def get(self):
        beginTime = self.get_query_argument("beginTime", "")
        if beginTime == "":
            self.write(responseErr("beginTime不能为空"))
            return
        begin = datetime.strptime(beginTime, "%Y-%m-%d %H:%M:%S")

        endTime = self.get_query_argument("endTime", "")
        if endTime == "":
            self.write(responseErr("endTime不能为空"))
            return
        end = datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S")

        content = self.get_query_argument("content", "")
        if content == "":
            self.write(responseErr("content不能为空"))
            return
        branchId = self.get_query_argument("branchId", "")
        if branchId == "":
            branchId = "0"
        traceId = self.get_query_argument("traceId", "")
        if traceId == "":
            traceId = None

        log = LogDb("log"+branchId)
        result = log.query(content, begin, end, traceId)

        self.write(json.dumps(result, ensure_ascii=False, cls=BytesEncoder))

    def post(self):
        content = self.request.body
        branchId = self.get_query_argument("branchId", "")
        traceId = self.get_query_argument("traceId", "")
        createdTime = self.get_query_argument("createdTime", "")
        dt = datetime.strptime(createdTime, "%Y-%m-%d %H:%M:%S")
        if content == "":
            self.write(responseErr("content不能为空"))
            return
        if traceId == "":
            self.write(responseErr("traceId不能为空"))
            return
        if branchId == "":
            branchId = "0"
        ip = ""
        if nginx == True:
            ip = self.request.headers.get(nginx_ip_key)
        else:
            ip = self.request.remote_ip
        log = LogDb("log"+branchId)
        log.write(traceId=traceId, txt=content, ip=ip, createdTime=dt)
        self.write(responseOk())

def make_app():
    return tornado.web.Application([
        (r"/log", LogHandler),
    ])

async def main():
    port = 8888
    app = make_app()
    app.listen(port)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())

