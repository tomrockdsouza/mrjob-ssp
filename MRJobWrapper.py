#! /usr/bin/python3
# MRJobWrapper.py
import os
import sys
from CandidateItemGenerator import CandidateItemGenerator
import json
from itertools import combinations as cb
import time

k=int(sys.argv[2])
SUPPORT_THRESHOLD = float(sys.argv[1])  # 10% of transactions
args = sys.argv[3:6] # get the arguments from the command line

passx='0'
if args[1]=='hadoop':
     passx='1'

args.insert(3,'--koperation')
args.insert(4,'NUMBER')
args.append('--varfile')
args.append('varx.json')

with open('varx.json','w') as fx:
     fx.write('')

if k==0:
     exit()

for i in range(k):
     start=time.time()
     args[4]=str(i+1)
     if i>0:
          for x in range(len(args)):
               if args[x]=='--varfile':
                    args[x+1]='xfrequent'+str(i)+'.txt'
     job = CandidateItemGenerator(args = args)
     # job.set_up_logging()
     with job.make_runner() as runner:
          runner.run()
          filex={}
          if i==0:
               counters = runner.counters()
               transaction_count = counters[0]['association_rules']['transaction_count']
          for key, value in job.parse_output(runner.cat_output()):
               if value/transaction_count >= SUPPORT_THRESHOLD:
                    filex[key]=value
     print('Time taken for pass',str(i+1),':',round(time.time()-start,2),'s')
     if len(filex.keys())==0:
          end_round=i
          break
     with open('xfrequent'+str(i+1)+'.txt','w') as fh:
          fh.write(json.dumps(filex))
     end_round=i+1


print('No of round processed:',end_round,'| Rounds Skipped:',k-end_round)
arr_can=[]
for i in range(1,end_round+1):
     with open('xfrequent'+str(i)+'.txt','r') as f:
          arr_can.append(json.load(f))
     os.remove('xfrequent'+str(i)+'.txt')


arrx=0

for j in reversed(range(end_round)):
     for x in range(j-1,-1,-1):
          for key,val in arr_can[j].items():
               arr_curr=json.loads(key)
               for e in cb(arr_curr,x+1):
                    hashx=json.dumps(list(e))
                    if hashx in arr_can[x]:
                         conf=val/arr_can[x][hashx]
                         if conf>=float(sys.argv[6]):
                              w=set(arr_curr)-set(e)
                              print(set(e),'->',w,'Confidence:',round(conf,4),end=' ')
                              w=list(w)
                              w.sort()
                              hashy=json.dumps(w)
                              if hashy in arr_can[j-x-1]:
                                   valy=arr_can[j-x-1][hashy]
                                   print('Interest:',round(conf-valy/transaction_count,6))
                                   arrx+=1

print('Number of Association rules produced:',arrx)