#!/usr/bin/env python3


start_year = 2021
start_month = 1

end_year = 2028
end_month = 12

current_year = start_year
current_month = start_month

while True:
    table_name = f"TRUNCATE table pe_device_load_{current_year}{current_month:02};"
    print(table_name)

    if current_year == end_year and current_month == end_month:
        break

    current_month += 1
    if current_month > 12:
        current_month = 1
        current_year += 1