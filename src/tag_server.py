# coding: utf-8

import conf.env
from conf.env import TAG_SERVER_HOST
from conf.env import TAG_SERVER_PORT
from BaseHTTPServer import HTTPServer
from http.server import HttpHandler
from http.igola.server_tag_sorting import IGola_TagSortingServer
from http.igola.server_tag_recommend import IGola_TagRecommendServer


if __name__ == "__main__":

    host = TAG_SERVER_HOST
    port = TAG_SERVER_PORT
    server_cls_list = [IGola_TagSortingServer, IGola_TagRecommendServer]
    for cls in server_cls_list:
        cls.get_all_task()
        cls.detection()
    server = HTTPServer((host, int(port)), HttpHandler)
    server.serve_forever()
