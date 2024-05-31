what I have changed is this

:- I have sharded the log tables

self.curr_log is the current log file im dealing with(the latest one i have created) 

self.lim in the store files describe the maximum limit of every sharded log table i have put it as 1 for testing but we can change it to anything we want

when i shard the log files, I had to add a column in the main table as well, so when i update an existing subject predicate tuple 
i look at the log column of this entry if its -1 this entry doesnt exist in any sharded log table, so  i just add it to the current log table
if not i delete this entry from log table it existed in 


while adding to the current log table if the current log table has already reached the self.lim size so i increment self.curr_log+=1 make another table with log{self.curr_log}


