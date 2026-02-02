#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# def test_1(a):
#     print(a)



# test_1(
#     'a'
# )

# test1 = {}

# test1['asd'] = 1

# print(test1)


merged_data = {
    'node_code': 'node_code',
    'pe_device_code': 'pe_device_code',
    'pe_device_main_interface_code': 'pe_physics_interface_code',
    'monitor_period': 5,
    '1':1,
    '2':2,
    '3':3,
    '4':4,
    '5':5,
    '6':6,
    '7':7,
    '8':8,
    '9':9,
    '10':10
}


test1 = list(merged_data.keys())
print(test1)

values_to_remove = ['node_code', 'pe_device_code', 'pe_device_main_interface_code', 'monitor_period']

filtered_numbers = [num for num in test1 if num not in values_to_remove]

print(filtered_numbers)

