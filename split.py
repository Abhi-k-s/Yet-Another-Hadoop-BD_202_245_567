import os
import glob
import shutil
import json
import threading
from functions import fileUpload  #after splitting files this takes care of placing those splits at right datanode

f = open("config_sample.json")
config = json.load(f)
block_size = config['block_size']
path_to_datanodes = config['path_to_datanodes']
path_to_namenodes = config['path_to_namenodes']
replication_factor = config['replication_factor']
num_datanodes = config['num_datanodes']
datanode_size = config['datanode_size']
sync_period = config['sync_period']
datanode_log_path = config['datanode_log_path']
namenode_log_path = config['namenode_log_path']
namenode_checkpoints = config['namenode_checkpoints']
fs_path = config['fs_path']
dfs_setup_config = config['dfs_setup_config']

setupfiledir = config['dfs_setup_config'][:-10]

def split(filePath):
    input=[]  #this is sent as input for nn.py
    try:
        shutil.rmtree('temp')
    except:
        pass
    os.mkdir('temp')

    #filePath="./US_ACCIDENT_DATA_5PERCENT.json"   #path of the file to be uploaded to hdfs

    fileName=filePath.split('/')[-1]

    #Code to split depending on the size of data
    os.system('split -b {}m '.format(block_size) + filePath + ' ./temp/block_')

    files = glob.glob('./temp/block*')
    files.reverse()  #since files list had splits in a reverse order

    input.append(fileName)
    input.append(len(files))

    #print(fileName)
    i=1
    for file in files:
        newFilePath='./temp/'+fileName.split('.')[0]+'${}'.format(i)+'.'+fileName.split('.')[1]
        os.rename(file, newFilePath)  #extension of file needed so last split() 
        splitPath=newFilePath
        i+=1
        
        input.append(splitPath)
        
    #print(input)
    message = fileUpload(input)
    return message
    # t1=threading.Thread(target=nn.fileUpload,args=(input,))
    # t1.start()
