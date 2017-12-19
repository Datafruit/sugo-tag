# coding: utf-8

import json
import cgi
import traceback
from urlparse import urlsplit, parse_qs
from BaseHTTPServer import BaseHTTPRequestHandler

from http.route import HttpRoute
from http.method import HttpMethod


def do_func(route, method, *args, **kwargs):
    if not HttpRoute.check_route(route):
        raise Exception("route [%s] not found" % route)
    func = HttpRoute.search_func(route)
    func_name = func.get_func_name()
    if not HttpMethod.check_method(func_name, method):
        raise Exception("method [%s] of function [%s] wrong" % (method, func_name))
    return func(*args, **kwargs)


class HttpHandler(BaseHTTPRequestHandler, object):

    def do_GET(self):
        split_res = urlsplit(self.path)
        route = split_res.path
        kwargs = parse_qs(split_res.query)
        try:
            ret = do_func(route, "GET", **kwargs)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(ret)
        except Exception as e:
            print traceback.format_exc()
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(e.message)

    def do_POST(self):
        split_res = urlsplit(self.path)
        route = split_res.path
        cgi.parse_header(self.headers["content-type"])
        length = int(self.headers['content-length'])
        kwargs = json.loads(self.rfile.read(length))
        for k, v in parse_qs(split_res.query).iteritems():
            kwargs[k] = v
        try:
            ret = do_func(route, "POST", **kwargs)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(ret)
        except Exception as e:
            print traceback.format_exc()
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(e.message)

    def do_DELETE(self):
        split_res = urlsplit(self.path)
        route = split_res.path
        kwargs = parse_qs(split_res.query)
        try:
            ret = do_func(route, "DELETE", **kwargs)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(ret)
        except Exception as e:
            print traceback.format_exc()
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(e.message)


if __name__ == "__main__":

    ## test
    import conf.env
    from http.register import http_register, http_route, http_method
    from BaseHTTPServer import HTTPServer

    @http_register
    class HttpTest1(object):
        @http_method("GET")
        @http_route("/sugo/tag1")
        def test(self, *args, **kwargs):
            return "hello world 1"

    @http_register
    class HttpTest2(object):
        @http_method("GET")
        @http_route("/sugo/tag2")
        def test(self, *args, **kwargs):
            return "hello world 2"

    @http_register
    class HttpTest3(HttpTest1):
        @http_method("GET")
        @http_route("/sugo/tag3")
        def test(self, *args, **kwargs):
            return "hello world 3"


    server = HTTPServer(("localhost", 8888), HttpHandler)
    server.serve_forever()
