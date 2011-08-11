#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
sys.path.append("../client/python")


import csv
import pprint
import numpy
from scipy import stats   
   
datadir = 'old-db/exported/'

##################### HELPER FUNCTIONS ###############################

from itertools import islice, chain
def group(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

def transpose(lists):
    return [list(tup) for tup in zip(*lists)]

def dataconv(dataset):

    nans = []

    for (i, x) in enumerate(dataset):
        if x == "-1" or x == "":
            nans.append(i)
            dataset[i] = "0"
        
    try:
        dataset = map(int,dataset)
        storage = "int"
    except ValueError:    
        dataset = map(float,dataset)
        storage = "float"
        
    for n in nans:
        dataset[n] = None

    return (dataset, storage)
    
def annotate(x):
    x = x.strip()
    if len(x) > 0:
        x = {"annotation": x}
    else:
        x = None
    return x 

import re

asl = re.compile("(\d*)\s?m\s?a\.s\.l\.")
posn = re.compile("(\d*)\s(\d*(\.\d*)?)\'\s?((\d*(\.\d*)?)\'\')?\s?N")
posw = re.compile("(\d*)\s(\d*(\.\d*)?)\'\s?((\d*(\.\d*)?)\'\')?\s?W")


def coord_to_dec(deg, minute, seconds=0.0):
    if seconds is None: 
        seconds = 0.0        
    return float(deg) + float(minute)/60.0 + float(seconds)/3600.0

def get_location(txt):
    lat = None
    lon = None
    elevation = None
   
    aslmatch = re.search(asl, txt)
    posnmatch = re.search(posn, txt)
    poswmatch = re.search(posw, txt)
    
    if aslmatch is not None:
        elevation = int(aslmatch.group(1))
        
    if posnmatch is not None:
        lat = coord_to_dec(posnmatch.group(1), posnmatch.group(2), posnmatch.group(5))
    
    if poswmatch is not None:
        lon = -coord_to_dec(poswmatch.group(1), poswmatch.group(2), poswmatch.group(5))
         
    return (lat, lon, elevation)



############## BEGIN LOAD ###################

sites = {}
cores = {}
studies = {}
types = {}


print "Loading sites"
## load sites (which is called regions - and manually generated)
csvsites = csv.reader(
                    open(datadir + 'regions.csv', 'r'), 
                    delimiter="\t", quotechar='"')

for row in csvsites:
    if row[0] in sites:
        sites[row[0]].append(int(row[1]))
    else:
        sites[row[0]] = [int(row[1])]

print "Loading cores"
csvcores = csv.reader(
                    open(datadir + 'SiteT.txt', 'r'), 
                    delimiter="\t", quotechar='"')

for row in csvcores:
    if row[0] != "SiteId":
        cores[int(row[0])] = (row[1], row[2])
        
        
print "Loading datatypes"        
csvtypes = csv.reader(
                    open(datadir + 'DataType.txt', 'r'), 
                    delimiter="\t", quotechar='"')
        
for row in csvtypes:
    if row[0] != "DatatypeId":
    
        obj = {}
        obj["type"] = row[1]
        obj["unit"] = row[2]                                     
        types[int(row[0])] = obj
        
print "Loading studies"
csvstudies = csv.reader(
                    open(datadir + 'Measurements.txt', 'r'), 
                    delimiter="\t", quotechar='"')

for row in csvstudies:
    if row[0] != "Record no":
        core = int(row[1])
        datatype = int(row[2])
        tbl = row[3]
        name = tbl[len(row[1]):]
        comment = row[7]
        
        if comment == "No Text":
            comment = None
        
        if core not in studies:
            studies[core] = {}
            
        if name not in studies[core]:
            studies[core][name] = {}
            studies[core][name]["parameters"] = []
        
        studies[core][name]["parameters"].append(types[datatype].copy())
        studies[core][name]["comment"] = comment


print "Loading datasets"
for core in studies:
    for name in studies[core]:
        filename = str(core)+name+".txt"
        info = studies[core][name]
        # The filename that was loaded
        print "\t%s" % filename
        

        
        data = []
        try:
            csvds = csv.reader(
                        open(datadir + 'datasets/' + filename, 'r'), 
                        delimiter="\t", quotechar='"')
                        
            for row in csvds:
                data.append(row)  
                 
            info["filename"] = filename   
            
            # This transpose is slow
            # It is not done with a numerical library, rather python lists
            
            data = transpose(data)  
            
        except IOError:
            # Tried to load a non-existing dataset
            info["invalid"] = True
            print "CONSISTENCY ERROR: " + filename + " ####"
            continue
            
        for dataset in data:
            found = None
            dstype = dataset.pop(0) # From the CSV file, the first row is the type
            
            for pi, pv in enumerate(info["parameters"]):
                if info["parameters"][pi]["type"] == dstype:
                    found = pi
                    break
            
            if dstype == "Comment":
                comments = map(annotate, dataset)
                info["annotations"] = comments
                if found:
                    del info["parameters"][found]
                continue
            
            try:      
                (dataset, storage) = dataconv(dataset)
            except:
                comments = map(annotate, dataset)
                info["annotations"] = comments
                if found:
                    del info["parameters"][found]
                continue

             
            # find the span
            span = numpy.diff(numpy.array(dataset, dtype=numpy.dtype("float"))).tolist()       
            
            if found is not None:
                # The data was a "y" type - a parameter    
                classification = "main"

                if dstype.lower().find("timescale") > -1:
                    classification = "timescale"

                if dstype.lower().find("error") > -1:
                    classification = "error"
                 
                info["parameters"][found]["data"] = dataset
                info["parameters"][found]["classification"] = classification
                info["parameters"][found]["storage"] = storage    
            else:
                # The data was either a "y" support parameter or a "x" measurement
    
                if dstype == "Depth" or dstype == "DateYear":
                    # It was a "x" measurement
                    if dstype == "Depth":
                        unit = "m"
                    else:
                        unit = "Julian Year"
                        
                    param = {"type": dstype, 
                                 "unit": unit, 
                                 "classification": "main", 
                                 "measurement": True, 
                                 "data": dataset, 
                                 "storage": storage,
                                 "span": span}
                        
                    info["parameters"].append(param)
                else:
                    # It was a support parameter
                    param = {"type": dstype, 
                             "unit": "unknown", 
                             "classification": "support", 
                             "data": dataset, 
                             "storage": storage}
                
                    info["parameters"].append(param)          


print "Done building the data structure"

print "Log in to import into database"

from cauth import client        

print "Dropping indicies for speed"

client.admin.sqlquery("DROP INDEX index_measurement_dsid;");
client.admin.sqlquery("DROP INDEX index_datapoint_pid;");
client.admin.sqlquery("DROP INDEX index_datapoint_mid;");

print "Pre-creating types"

type_lookup = {}

#  this pre-allocates types in an expected empty database
for core in studies:
    for study in studies[core]:

        for ds in studies[core][study]["parameters"]:
        
            if "invalid" in ds:
                continue
       
            name = ds["type"]
            unit = ds["unit"]
            species = ds["type"]
            
            try:
                classification = ds["classification"]
                storage = ds["storage"]

                if name not in type_lookup:
                    created_type = client.type.new( 
                                        name, unit, 
                                        species, classification, 
                                        storage, None)
                                                    
                    type_lookup[name] = created_type["id"]

            except:
                print "Please fix types for:" 
                print core, study, ds["type"]
                

for (index, site) in enumerate(sorted(sites.keys())):

    print "Site %d of %d" % (index, len(sites))

    # setup the site
    site_data = client.site.new(site, None, None, None, None)

    for core in sites[site]:

        print "Importing Core #%s" % str(core)
        # setup the icecore
        (lat, lon, elev) = get_location(cores[core][1])
        core_data = client.core.new(
                        site_data["id"], cores[core][0], 
                        lat, lon, elev, cores[core][1])
        
        if core in studies:

            for study in studies[core]:
                
                info = studies[core][study]
                
                if "invalid" in info:
                    continue
                
                # setup the study
                study_data = client.study.new(study, info["comment"])
                
                # link study to icecore
                client.study.add_core(study_data["id"], core_data["id"])
                
                print "\t%s" % study
                
                # find the measurement column and populate ytype_ids
                xtype_id = None
                xdata = None
                xspan = None
                ytype_ids = []
                ydata = {}
                
                for ds in info["parameters"]:
                    if "measurement" in ds:
                        xtype_id = type_lookup[ds["type"]]
                        xdata = ds["data"]
                        xspan = ds["span"]
                    else:
                        ytype_ids.append(type_lookup[ds["type"]])
                        ydata[type_lookup[ds["type"]]] = ds["data"]
                        
                if xtype_id is None:
                    print "xtype was not found for", info["filename"]
                    print [s["type"] for s in info["parameters"]]
                    exit()
                    

                
                # setup the dataset (with differences between CFA and others)
                if study.find("CFA") >= 0:
                    ds_data = client.dataset.new(
                                    study_data["id"], xtype_id, ytype_ids,
                                    "point", "na", "Old-DB import", None)
                else:
                    ds_data = client.dataset.new(
                                    study_data["id"], xtype_id, ytype_ids,
                                    "span", "end", "Old-DB import", None)
                                    
                    # add the span to the dataset
                    if xspan is not None:
                        xspan.insert(0, xspan[0]) # duplicate first spanning point
                        
                        if "annotations" not in info:
                            xdata = zip(xdata, xspan)
                        else:
                            xdata = zip(xdata, xspan, info["annotations"])
                    elif "annotations" in info:
                        xdata = zip(xdata, None, info["annotations"])


                                            
                    
                

                # rebase from lookup by type to parameter id
                ydata_rebased = {}
                for ytype in ydata:
                    for yparam in ds_data["params"]:
                        if yparam["ytype"]["id"] == ytype:
                            key = yparam["id"]    
                    ydata_rebased[key] = ydata[ytype]
                    
                    
                # estimated batch size in number of data points
                gs = int(5e5) / (len(ydata_rebased) + 1)
                    
                if len(xdata) < gs:
                    client.dataset.append(ds_data["id"], xdata, ydata_rebased)
                else:
                    xiter = group(xdata, gs)
                    yiter = {}
                    
                    print "Splitting insert into in smaller groups"
                    
                    for key in ydata_rebased:
                        yiter[key] = group(ydata_rebased[key], gs)
                    
                    for idx, xxdata in enumerate(xiter):
                        yydata = {}
                        for key in yiter:
                            yydata[key] = list(yiter[key].next())
                    
                        print "Batch:", idx
                        
                        client.dataset.append(ds_data["id"], list(xxdata), yydata)
                        
                client.dataset.close(ds_data["id"])
                
            
client.admin.sqlquery("create index index_measurement_dsid on measurements(dataset_id);");
client.admin.sqlquery("create index index_datapoint_pid on datapoints(parameter_id);");
client.admin.sqlquery("create index index_datapoint_mid on datapoints(measurement_id);");

