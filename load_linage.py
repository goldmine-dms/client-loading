#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys

sys.path.append("../client/python")

from cauth import client

s = client.study.new("Lineage", "A study of lineage")
study = s["id"]

t = client.type.new("Dummy", "-", "Dummy", "main", "float", None)
fakex = t["id"]
t = client.type.new("Dummy 2", "-", "Dummy", "main", "float", None)
fakey = t["id"]

def create(description, parent = None):
    c=client.dataset.new(study, fakex, [fakey], None, None, description, parent)
    client.dataset.close(c["id"])
    return c["id"]

a1 = create("Raw Data 1")
a2 = create("Raw Data 2")
a3 = create("Raw Data 3")

b = create("Processing", a1)
client.dataset.fork(a2, b)
client.dataset.fork(a3, b)

c1 = create("Cleaning - Right way", b)
c2 = create("Cleaning - Wrong way", b)

d = create("Intermediate", c1)

r = create("Additional Data")
e = create("Combined", d)
client.dataset.fork(r,e)

f = create("Published", e)

u = create("Unrelated")
u2 = create("Unrelated Child", u)
