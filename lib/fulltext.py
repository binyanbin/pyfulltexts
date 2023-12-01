import copy
import jieba
import datetime
from sqlalchemy import create_engine
from sqlalchemy import DateTime, Text, VARCHAR
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import sessionmaker


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
    content: Mapped[str] = mapped_column(VARCHAR(200), nullable=True)
    created_time: Mapped[DateTime] = mapped_column(
        DateTime(), default=datetime.datetime.now)
    modified_time: Mapped[DateTime] = mapped_column(
        DateTime(), default=datetime.datetime.now)


dbpool = dict()
split_char = " "


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

    def __trset(self, list):
        keys = []
        for key in list:
            if key.strip() != "":
                if keys.count(key) == 0:
                    keys.append(key)
        return keys

    def __createIndex(self, id: str, keys: list,  txt: str):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        idtokey = session.query(IdToKey).filter_by(id=id).first()
        if idtokey is None:
            idtokey = IdToKey(id=id, keys=self.__listostr(keys), content=txt)
            session.add(idtokey)
        else:
            idtokey.modified_time = datetime.datetime.now()
            keylist = idtokey.keys.split(split_char)
            idtokey.keys = self.__listostr(keys)
            idtokey.content = txt
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

    def createIndex(self, id: str, txt: str):
        cutkey = jieba.cut_for_search(txt)
        keys = self.__trset(cutkey)
        self.__createIndex(id, keys, txt)

    def createIndex2(self, id: str, keys: list, txt: str):
        keys = self.__trset(keys)
        self.__createIndex(id,  list(set(keys)), txt)

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
            result.append({"id": idtokey.id, "content": idtokey.content})
        return result

    def clear(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        session.query(KeyToId).delete()
        session.query(IdToKey).delete()
        session.commit()
