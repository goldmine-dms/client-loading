#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
sys.path.append("../client/python")


if len(sys.argv) != 2:
    print "You must provide a .out file"
    exit()

import re

pattern = re.compile("\s*")

from pprint import pprint

study = "230d4190-f0f8-410d-85fd-a40821d20192"
x_type = "0978abc1-0467-450b-8e9a-45764be27d97"
dD_type = "1b5acdf1-1d3b-41bb-91ff-d7a1dfb4460e"
dD_err_type = "ee970753-4d67-4540-bda6-0fa4eed91215"
dO18_type = "64aa4624-8567-474c-b763-f4ba727f763f"
dO18_err_type = "a5216b6d-744c-4fc0-8e4a-6196a7cc9539"

filename = sys.argv[1]
f = file(filename)

groups = [[]]
i = 0 

for line in f:
    line = line.strip()
    if len(line) > 0:
        groups[i].append(line)
    else:
        groups.append([])
        i += 1

dataset = groups[-1]
metadata = []
for group in groups[:-1]:
    
    params = None
    annotation = ", ".join(group)
    
    if group[0][-1] == ":":
        
        params = {}
        annotation = group[0][:-1]

        for el in group[1:]:
            keys = el.split(": ")
            if len(keys) > 1:
                key = keys[0]
                key = key.replace(" ", "_").replace("-","_")
                values = map(float, keys[1].split(", "))
                for idx, value in enumerate(values):
                    params.update({key+"_"+str(idx): value}) 
            else:
                el = el.split(", ")
                for obj in el:
                    obj = obj.split("=")
                    key = obj[0]
                    value = obj[1]
                    err = value.split(" +/- ")

                    if len(err) == 2:
                        value = err[0]
                        err = float(err[1])
                    else:
                        err = None

                    key = key.replace(" ", "_").replace("-","_")
                    value = float(value)
                    params.update({key: value})

                    if err is not None:
                        params.update({key+"_err": value})
    print "-"*75
    print annotation
    if params:
        pprint(params)

    metadata.append({"annotation": annotation, "params": params})

x = []
dD = []
dD_err = []
dO18 = []
dO18_err = []

for data in dataset:
    data = re.split(pattern, data)
    x.append(int(data[0]))
    dD.append(float(data[1]))
    dD_err.append(float(data[2]))
    if len(data) > 3:
        dO18.append(float(data[3]))
        dO18_err.append(float(data[4]))

description = filename

print "="*75
print "Dataset description:", description
print "Ready to import", len(x), "samples"
print "="*75

from cauth import client

if len(data) > 3:
    dataset = client.dataset.new(
        study, x_type, [dD_type, dD_err_type, dO18_type, dO18_err_type], 
        "point", "center", description, None)
    conv = {}
    for p in dataset["params"]:
        conv[p["ytype"]["id"]] = p["id"]

    y = {conv[dD_type]: dD, conv[dD_err_type]: dD_err, 
         conv[dO18_type]: dO18, conv[dO18_err_type]: dO18_err}
else:
    dataset = client.dataset.new(
        study, x_type, [dD_type, dD_err_type], 
        "point", "center", description, None)
    conv = {}
    for p in dataset["params"]:
        conv[p["ytype"]["id"]] = p["id"]

    y = {conv[dD_type]: dD, conv[dD_err_type]: dD_err}

client.dataset.append(dataset["id"], x, y)

for m in metadata:
    client.dataset.add_metadata(dataset["id"], None, None, None, m)

client.dataset.close(dataset["id"])



