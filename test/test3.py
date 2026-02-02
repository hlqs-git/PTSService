#!/usr/bin/env python3
# -*- coding: utf-8 -*-

test1 = {
    "name": "test1",
    "type": "test",
    "value": "test1"
    }

test2 = {
    "name": "test1",
    "type": "test",
    "value": "test1",
    1:'1',
    2:'2',
    3:'3'
    }


test3 = test2.copy()

test2['name'] = 'test2'

print(test3)