import shutil #to move splits to datanodes
import json
import os
import glob

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

#after splitting a file into few blocks below data is sent to nn.py program
#input:
# {fileName:[3,locations where those 3 blocks are stored currently]}  
#initialy splits are stored in some location then moved to hdfs after checking for free blocks

def fileUpload(input):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	f1 = open(metaDataOfDatanodespath)
	f2 = open(metaDataOfInputFilespath)
	metaDataOfDatanodes = json.load(f1)
	metaDataOfInputFiles = json.load(f2)
	f1.close()
	f2.close()

	metaDataOfInputFiles[input[0]]=list()
	metaDataOfInputFiles[input[0]].append(input[1])

	splitNumberCount=1
	#check which datanode has free blocks then assign those blocks for storing abc.txt's splits 
	for i in range(1, num_datanodes + 1):
		if(len(metaDataOfDatanodes['datanode{}'.format(i)]['freeBlocks']) >= 1): #checking for atleast one freeblock
			freeBlockNumber = metaDataOfDatanodes['datanode{}'.format(i)]['freeBlocks'].pop(0) #pop based on hashing with split and block number
			metaDataOfDatanodes['datanode{}'.format(i)]['occupiedBlocks'][freeBlockNumber]=[input[0],splitNumberCount]
			
			#now move the corresponfing splits to datanode{i}'s  ie to hdfs previously it was stored in temporary folder
			
			source = input[splitNumberCount+1]
			destination = path_to_datanodes + "datanode{}".format(i)
			
			#print(source, destination)
			shutil.move(source, destination)
			os.remove(destination + '/block{}.txt'.format(freeBlockNumber))

			#metadata of files updation
			splitName=input[splitNumberCount+1].split('/')[-1].split('.')[0]
			splitInfo=list()
			splitInfo.append(splitNumberCount)
			splitInfo.append(splitName)   
			splitInfo.append('datanode{}'.format(i))  #datanode number where the split is stored      
			splitInfo.append(freeBlockNumber)
			metaDataOfInputFiles[input[0]].append(splitInfo)

			
		if(splitNumberCount==input[1]): 
			# all splits successfully placed in the free datanodes
			message = "Write file into HDFS successful"
			break
		splitNumberCount+=1

	f1 = open(metaDataOfDatanodespath, 'w')
	f2 = open(metaDataOfInputFilespath, 'w')
	f1.write(str(json.dumps(metaDataOfDatanodes, indent=4)))
	f2.write(str(json.dumps(metaDataOfInputFiles, indent=4)))
	f1.close()
	f2.close()
	return message
	#print(metaDataOfDatanodes)
	#print(metaDataOfInputFiles)

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

def cat(fileName):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	#print(fileName)
	f = open(metaDataOfInputFilespath)
	data=json.load(f)
	#print(data)
	#fileSplits=["./datanode1/hello.txt","./datanode2/hello2.txt"] #split's paths orderwise  this is how fileSplits looks like

	fileSplits=[]
	try:
		for i in range(1, data[fileName][0]+1):
			fileSplits.append(path_to_datanodes+data[fileName][i][2]+'/'+data[fileName][i][1]+'.'+fileName.split('.')[1]) #relativepaths of splits
	except:
		print("File not found in the DFS")

	for splits in fileSplits:
		file1 = open(splits,'r')

		lines = file1.readlines()
		for line in lines:
			print(line.strip())

		file1.close()