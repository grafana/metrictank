#!/usr/bin/env python                                                                                                                    
                                                                                                                                     
import math                                                                                                                          
from prettytable import PrettyTable                                                                                                  
                                                                                                                                     
def get_hours(start, end):                                                                                                           
    if start == end:                                                                                                                 
        return str(start)                                                                                                            
    return "{start} <= hours < {end}".format(start=start, end=end)                                                                            
                                                                                                                                     
def get_days(start, end):                                                                                                            
    start = float(start)/24                                                                                                          
    end = float(end)/24                                                                                                              
    if end < 1:                                                                                                                      
        return "<1"                                                                                                                  
    if start < 1 and end < 2:                                                                                                        
        return "~1"                                                                                                                  
    return "{start} - {end}".format(                                                                                                 
        start=int(math.floor(start)),                                                                                                
        end=int(math.ceil(end)),                                                                                                     
    )                                                                                                                                
                                                                                                                                     
def get_table_name(start):                                                                                                           
    return "metrics_{start}".format(start=start)                                                                                     
                                                                                                                                     
def get_window_size(start, factor):                                                                                                  
    return start / factor + 1                                                                                                        
                                                                                                                                     
def get_sstables(start, end, window_size):                                                                                           
    return "{start} - {end}".format(                                                                                                 
        start=int(math.ceil(float(start)/window_size)),                                                                                                     
        end=int(math.ceil(float(end)/window_size))+1,                                                                                                         
    )                                                                                                                                
                                                                                                                                     
def get_line(start, end, factor):                                                                                                    
    ttl_hours = get_hours(start,end)                                                                                                     
    table_name = get_table_name(start)                                                                                               
    window_size = get_window_size(start, factor)                                                                                     
    sstables = get_sstables(start, end, window_size)                                                                                 
    return (ttl_hours, table_name, window_size, sstables)                                                                           
                                                                                                                                     
                                                                                                                                     
ttl = 1                                                                                                                              
factor = 20                                                                                                                           
                                                                                                                                     
t = PrettyTable(["TTL hours", "table_name", "window_size (hours)", "sstables"])                                                           
t.add_row(get_line(0, 1, factor))                                                                                      

# generate up to 5 years                                                                                                              
while ttl < 5*365*24:                                                                                                                
    t.add_row(get_line(ttl, ttl*2, factor))                                                                                      
    ttl *= 2                                                                                                                         
                                                                                                                                     
t.align = 'r'                                                                                                                        
print(t)
