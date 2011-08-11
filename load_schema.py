#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
sys.path.append("../client/python")

sql = file('server/schema.sql')
client.admin.sqlquery(sql.read())

