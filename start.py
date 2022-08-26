import pandas as pd
import sys
from subprocess import check_call
import os
import time

start = time.time()

pvar='python3'
if sys.platform=='win32':
    pvar=pvar[:-1]


dfs_var='dfs'
hdfs_var='hdfs'

def clean(variable):
    variable=variable.replace("Wifi \\u2013 600 Mbps","Wifi")
    variable=variable.replace("Wifi \\u2013 100 Mbps","Wifi")
    variable=variable.replace("Paid street parking off premises","Paid Off")\
    .replace("Paid parking off premises","Paid Off")\
    .replace("Paid parking lot off premise","Paid Off")\
    .replace("Paid parking garage off premises","Paid Off")
    variable=variable.replace("Washer \\u2013\\u00a0In unit","Washer")
    variable=variable.replace("Dryer \\u2013\\u00a0In unit","Dryer")
    return variable

df=pd.read_csv('listings_detailed.csv')
sfx=pd.DataFrame()
sfx['amenities']=df['amenities'].apply(lambda x:clean(x))
sfx.to_csv('dataset.csv',index=False,header = False)


if sys.argv[1]=='hadoop':
    try:
        check_call([hdfs_var,dfs_var,'-rm','-r','-f','/tmp'])
        check_call([hdfs_var,dfs_var,'-rm','-r','-f','/user/hduser/tmp'])
        check_call([hdfs_var,dfs_var,'-mkdir','/tmp'])
        check_call([hdfs_var,dfs_var,'-mkdir','/tmp/tomrock'])
        check_call([hdfs_var,dfs_var,'-put','-f','dataset.csv','/tmp/tomrock/dataset.csv'])
        check_call([
            pvar,
            'MRJobWrapper.py',
            sys.argv[2],
            sys.argv[3],
            '-r',
            'hadoop',
            'hdfs://hadoop-master:54310/tmp/tomrock/dataset.csv',
            sys.argv[4]
        ])
    finally:
        check_call([hdfs_var,dfs_var,'-rm','-r','-f','/user/hduser/tmp'])
        check_call([hdfs_var,dfs_var,'-rm','-r','-f','/tmp'])
else:
    check_call([pvar,'MRJobWrapper.py',sys.argv[2],sys.argv[3],'-r','inline','dataset.csv',sys.argv[4]])

os.remove('varx.json')
os.remove('dataset.csv')

end = time.time()
print('Time Taken to execute script:',round(end-start,2),'s')


