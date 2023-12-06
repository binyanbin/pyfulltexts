
import asyncio
import tornado
from lib.fulltext import FullText
import pyttsx3

engine = pyttsx3.init()

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


class MainHandler(tornado.web.RequestHandler):
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
        self.write(responseOk(result))

    def post(self):
        data = tornado.escape.json_decode(self.request.body)
        if containsKeys(data, ["target", "id"]) != True:
            self.write(responseErr("数据格式不正确"))
            return
        if data["target"] == "":
            self.write(responseErr("target不能为空"))
            return
        if data["id"] == "":
            self.write(responseErr("id不能为空"))
            return
        content = ""
        if containsKeys(data, ["content"]) == True:
            content = data["content"]

        fulltext = FullText(data["target"])
        if containsKeys(data, ["keys"]):
            if len(data["keys"]) > 0:
                fulltext.createIndex2(data["id"], data["keys"], content)
            else:
                fulltext.deleteIndex(data["id"])
        else:
            fulltext.createIndex(data["id"], content)
        self.write(responseOk())


class VoiceHandler(tornado.web.RequestHandler):
    def __text2File(self, text, dstFile):
        engine.save_to_file(text, dstFile)
        engine.runAndWait()

    def get(self):
        text = self.get_query_argument("text").strip()
        if len(text) > 0:
            tmpFile = "1.mp3"
            rate = self.get_query_argument("rate", 100)
            if isinstance(rate, int) == False:
                rate = self.get_query_argument("rate").strip()
            engine.setProperty('rate', int(rate))
            self.__text2File(text, tmpFile)
            self.set_header('content-type', 'audio/mpeg')
            fbin = open(tmpFile, "rb").read()
            self.set_header('Content-Length', len(fbin))
            self.write(fbin)
            self.finish()
        else:
            self.write(responseErr("请输入文本"))

    def post(self):
        print("post")
        print(self.request.arguments)


port = 8888


def make_app():
    return tornado.web.Application([
        (r"/index", MainHandler),
        (r"/voice", VoiceHandler)
    ])


async def main():
    app = make_app()
    app.listen(port)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
