
import asyncio
import tornado
from lib.fulltext import FullText


def responseOk(data=None):
    result = {}
    result["code"] = 0
    if data is not None:
        result["data"] = data
    return result


def responseErr(data=None):
    result = {}
    result["code"] = -1
    if data is not None:
        result["data"] = data
    return result


def containsKeys(obj, keys):
    if isinstance(obj, dict):
        return all(key in obj.keys() for key in keys)
    elif hasattr(type(obj), '__dict__'):
        return all(key in obj.__dict__ for key in keys)
    else:
        return False


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        source = self.get_query_argument("source", "")
        keys = self.get_query_argument("keys", "")
        if source == "":
            self.write(responseErr("source不能为空"))
            return
        if keys == "":
            self.write(responseErr("keys不能为空"))
            return
        fulltext = FullText(source)
        lkeys = keys.split(",")
        result = fulltext.query(lkeys)
        self.write(responseOk(result))

    def post(self):
        data = tornado.escape.json_decode(self.request.body)
        if containsKeys(data, ["source", "id", "content"]) != True:
            self.write(responseErr("source不能为空"))
            return
        if data["source"] == "":
            self.write(responseErr("source不能为空"))
            return
        if data["id"] == "":
            self.write(responseErr("id不能为空"))
            return
        if data["content"] == "":
            self.write(responseErr("content不能为空"))
            return
        fulltext = FullText(data["source"])
        if containsKeys(data, ["keys"]) and len(data["keys"]) > 0:
            fulltext.createIndex2(
                data["id"], data["keys"], data["content"])
        else:
            fulltext.createIndex(id=data["id"], txt=data["content"])
        self.write(responseOk())


port = 8888


def make_app():
    return tornado.web.Application([
        (r"/index", MainHandler),
    ])


async def main():
    app = make_app()
    app.listen(port)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
