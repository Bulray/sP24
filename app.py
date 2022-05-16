import os
import re
import mypy
import argparse
from typing import Iterator
from flask import Flask, request, Response
from werkzeug.exceptions import BadRequest
import requests

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def limits(it: Iterator, limit: int) -> Iterator:
    i = 0
    for item in it:
        if i < limit:
            yield item
        else:
            break
        i += 1


def app_commandd(it: Iterator, cmd: str, value: str) -> Iterator:
    if cmd == "filter":
        return filter(lambda v: value in v, it)
    if cmd == "map":
        idx = int(value)
        return map(lambda v: v.split(" ")[idx], it)
    if cmd == "unique":
        return iter(set(it))
    if cmd == "sort":
        reverse = value == "desc"
        return iter(sorted(it, reverse=reverse))
    if cmd == "limit":
        arg = int(value)
        return limits(it, arg)

    if cmd == "regex":
        regex = re.compile(value)
        return filter(lambda v: regex.search(v), it)
    return it


def build_query(it: Iterator, cmd1: str, value1: str, cmd2: str, value2: str) -> Iterator:
    res: Iterator = map(lambda v: v.strip(), it)
    res = app_commandd(res, cmd1, value1)
    return app_commandd(res, cmd2, value2)


@app.post("/perform_query")
def perform_query() -> Response:
    try:
        file_name = request.form["file_name"]
        cmd1 = request.form["cmd1"]
        value1 = request.form["value1"]
        cmd2 = request.form["cmd2"]
        value2 = request.form["value2"]
    except KeyError:
        raise BadRequest

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise BadRequest(description=f"{file_name} was not found")

    with open(file_path) as fd:
        res = build_query(fd, cmd1, cmd2, value1, value2)
        content = '\n'.join(res)
    return app.response_class(content, content_type="text/plain")


if __name__ == '__main__':
    app.run(debug=True)
