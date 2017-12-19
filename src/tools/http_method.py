# coding: utf-8

import urllib2


def get(url):
    req = urllib2.Request(url=url)
    req.get_method = lambda : 'GET'
    res = urllib2.urlopen(req)
    res_data = res.read()
    res_code = res.getcode()
    return int(res_code), res_data


def post(url, data, content="application/json; charset=utf-8"):
    req = urllib2.Request(url=url, data=data)
    req.add_header('Content-Type', content)
    res = urllib2.urlopen(req)
    res_data = res.read()
    res_code = res.getcode()
    return int(res_code), res_data


def delete(url):
    req = urllib2.Request(url=url)
    req.get_method = lambda : 'DELETE'
    res = urllib2.urlopen(req)
    res_data = res.read()
    res_code = res.getcode()
    return int(res_code), res_data