import jieba
import datetime
from sqlalchemy import and_, create_engine
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
    content: Mapped[str] = mapped_column(VARCHAR(200))
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

    def _listostr(self, mylist: list):
        if mylist is None:
            return ''
        str = ''
        index = 0
        while len(mylist) > 0:
            temp = mylist.pop(index)
            if len(mylist) == 0:
                str = str + temp
            else:
                str = str+temp+split_char
        return str

    def _merge(self, temp: list, list: list):
        result = []
        for id in list:
            if temp.count(id) > 0:
                result.append(id)
        return result

    def clear(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        session.query(KeyToId).delete()
        session.query(IdToKey).delete()
        session.commit()

    def createIndex(self, id: str, txt: str):
        cutkey = jieba.cut_for_search(txt)
        keys = ''
        lkeys = []
        for key in cutkey:
            if key.strip() != "":
                keys = keys + key.strip()+split_char
                if lkeys.count(key) == 0:
                    lkeys.append(key)
        Session = sessionmaker(bind=self.engine)
        session = Session()
        idtokey = session.query(IdToKey).filter_by(id=id).first()
        if idtokey is None:
            idtokey = IdToKey(id=id, keys=keys, content=txt)
            session.add(idtokey)
        else:
            idtokey.modified_time = datetime.datetime.now()
            keylist = idtokey.keys.split(split_char)
            idtokey.keys = keys
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
                        keytoid.ids = self._listostr(myids)

        keytoids = session.query(KeyToId).filter(
            KeyToId.key.in_(lkeys)).all()
        for keytoid in keytoids:
            if lkeys.count(keytoid.key) > 0:
                lkeys.remove(keytoid.key)
            list = keytoid.ids.split(',')
            if list.count(id) == 0:
                list.append(id)
                keytoid.ids = self._listostr(list)
        if len(lkeys) > 0:
            for key in lkeys:
                keytoid = KeyToId(key=key, ids=id)
                session.add(keytoid)
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
                    ids = self._merge(ids, temp)
                    if len(ids) == 0:
                        return None
        idtokeys = session.query(IdToKey).filter(IdToKey.id.in_(ids)).all()
        result = []
        for idtokey in idtokeys:
            result.append({"id": idtokey.id, "content": idtokey.content})
        return result
