
from flask import Flask, app, request
from loguru import logger
import json

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


app = Flask(__name__)


@app.route('/index', methods=['POST'])
def createIndex():
    data = json.loads(request.get_data(as_text=True))
    if data["source"] is None or request.args.get("source") == "":
        return responseErr("source不能为空")
    if data["id"] is None or data["id"] == "":
        return responseErr("id不能为空")
    if data["content"] is None or data["content"] == "":
        return responseErr("content不能为空")
    fulltext = FullText(data["source"])
    fulltext.createIndex(data["id"], data["content"])
    return responseOk()


@app.route('/search', methods=['GET'])
def query():
    if request.args.get("source") is None or request.args.get("source") == "":
        return responseErr("source不能为空")
    if request.args.get("keys") is None or request.args.get("keys") == "":
        return responseErr("keys")
    fulltext = FullText(request.args.get("source"))
    keys = request.args.get("keys").split(",")
    result = fulltext.query(keys)
    return responseOk(result)


if __name__ == '__main__':
    try:
        logger.add("./http.log")
        app.run(host='0.0.0.0', port=8080)
    except Exception as err:
        logger.error("err %s: " % err)
