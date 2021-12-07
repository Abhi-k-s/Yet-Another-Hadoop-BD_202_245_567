import shutil #to move splits to datanodes
import json
import os
import glob

dfs_setup_config = "/users/vinaynaidu/DFS/setup.json"
setupfiledir = "/users/vinaynaidu/DFS/"

f = open(dfs_setup_config)
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

def fileUpload(input):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	metaDataOfReplicaspath = path_to_namenodes + 'metaDataofReplicas.json'
	f1 = open(metaDataOfDatanodespath)
	f2 = open(metaDataOfInputFilespath)
	f3 = open(metaDataOfReplicaspath)
	metaDataOfDatanodes = json.load(f1)
	metaDataOfInputFiles = json.load(f2)
	metaDataOfReplicas = json.load(f3)
	f1.close()
	f2.close()
	f3.close()

	metaDataOfInputFiles[input[0]]=list()
	metaDataOfInputFiles[input[0]].append(input[1])
	metaDataOfReplicas[input[0]] = {}
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
			# os.remove(destination + '/block{}.txt'.format(freeBlockNumber))

			#replication of newly added split the splits are replicated to the free blocks of the next  datanodes
			j = i+1
			replicaCount = 0
			if(j == num_datanodes + 1):
				j=1
			replicaCounter = 0 #number of replicas  produced in hdfs
			replicaInfo=[]  #will store locations of replicasplits later added to metadata of replicas
			
			while(j<= num_datanodes and replicaCount < replication_factor ): #this part does replication
				if(j==i): #full traversal done still no free blocks ie no space left for freeblocks no storing replica's in same datanode
					break 
				
				if(len(metaDataOfDatanodes['datanode{}'.format(j)]['freeBlocks'])>=1):
					
					replifreeBlockNumber=metaDataOfDatanodes['datanode{}'.format(j)]['freeBlocks'].pop(0)
					metaDataOfDatanodes['datanode{}'.format(j)]['occupiedBlocks'][replifreeBlockNumber]=[input[0],splitNumberCount]
					replicaCount += 1
					replicaDestiny= path_to_datanodes + "/datanode{}".format(j)
					splitFileName=source.split('/')[-1] 
					replicaSource=destination+'/'+ splitFileName  #this is the path where the split to be replicated resides
					try:
						shutil.copy(replicaSource,replicaDestiny)
						replicaCounter+=1
						replicaPath=replicaDestiny+'/'+splitFileName
						replicaInfo.append(replicaPath)
					except:
						pass #no copying splits to same datanode so moving same splits to same datanode is checked here
				j+=1
				if(j==num_datanodes+1):
					j=1
		
			#update the replica metadata	input[0] is filename second nested key is splitname of the input[0] ie filename
				
			replicaInfo.insert(0,replicaCounter)

			metaDataOfReplicas[input[0]][input[splitNumberCount+1].split('/')[-1]]=replicaInfo

			splitName=input[splitNumberCount+1].split('/')[-1].split('.')[0]
			splitInfo=list()
			splitInfo.append(splitNumberCount)
			splitInfo.append(splitName)   
			splitInfo.append('datanode{}'.format(i))  #datanode number where the split is stored      
			splitInfo.append(freeBlockNumber)		
			metaDataOfInputFiles[input[0]].append(splitInfo)

			
		if(splitNumberCount==input[1]): # all splits successfully placed in the free datanodes
			message = "Write file into HDFS successful"
			break
		splitNumberCount+=1

	f1 = open(metaDataOfDatanodespath, 'w')
	f2 = open(metaDataOfInputFilespath, 'w')
	f3 = open(metaDataOfReplicaspath, 'w')
	f3.write(str(json.dumps(metaDataOfReplicas, indent=4)))
	f1.write(str(json.dumps(metaDataOfDatanodes, indent=4)))
	f2.write(str(json.dumps(metaDataOfInputFiles, indent=4)))
	f1.close()
	f2.close()
	f3.close()
	return message


def split(filePath):
    input=[]  #this is sent as input for nn.py
    try:
        shutil.rmtree('temp')
    except:
        pass
    os.mkdir('temp')

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
        newFilePath='./temp/'+fileName.split('.')[0]+'*{}'.format(i)+'.'+fileName.split('.')[1]
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
	#fileSplits=["./datanode1/hello.txt","./datanode2/hello2.txt"] #split's paths orderwise  this is how fileSplits look like

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

def remove(fileName):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	f=open(metaDataOfInputFilespath)
	f3=open(metaDataOfDatanodespath)
	data=json.load(f)  #metadata of files
	metadataOfDatanodes=json.load(f3)  #metadata of datanodes

	f.close()
	f3.close()

	fileSplits=[]
	try:
		for i in range(1,data[fileName][0]+1):
			fileSplits.append(path_to_datanodes + data[fileName][i][2]+'/'+data[fileName][i][1]+'.'+fileName.split('.')[1]) #relativepaths of splits

	except:
		print("File does not exist") 


	i=1 #to keep track of splits in data{}
	for splits in fileSplits:
		freeDatanode= data[fileName][i][2]     #block from this datanode this removed
		freeBlockNumber=data[fileName][i][3]    #freed block
		#print(freeDatanode,freeBlockNumber)
		
		os.remove(splits)
		i=i + 1
		#update datanodemetafile since splits got deleted the blocks of datnode has become free
		metadataOfDatanodes[freeDatanode]["occupiedBlocks"].pop(str(freeBlockNumber))
		metadataOfDatanodes[freeDatanode]["freeBlocks"].insert(0,int(freeBlockNumber))
		
	try:
		data.pop(fileName) #file doesnt exits anymore so remove it off from the fileMetadata
		f2 = open(metaDataOfInputFilespath,'w')
		f2.write(str(json.dumps(data, indent=4)))
		f2.close()
	except:
		pass
		
	#updating datanode metadata
	f4=open(metaDataOfDatanodespath,'w')
	f4.write(str(json.dumps(metadataOfDatanodes, indent=4)))
	print("Remove file successful")

def listallfiles():
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	f = open(metaDataOfInputFilespath)
	data = json.load(f) 
	f.close()
	for key in data.keys():
		print(key)