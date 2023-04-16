import time
#import os, psutil
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
#THREADING
from threading import Thread
from threading import Event
import json

#DOCKER
import docker
#dockerClient = docker.from_env()
dockerClient = docker.DockerClient()
imageName = "tabular-comp"
containerStatsName = "containerStats.json"
#dataframeN = 1000000
calcN = 10000
cardinality = 5000
testTypes = ["spark","polars","pandas"]
dataframeNs = [2500,25000,250000,2500000]

def dockerLog(event,testType,dataframeN,cardinality):
    cpuLog = []
    memLog = []
    print("################################################### dockerLog started")
    startFlag = True
    while True:
        try:
            containers = getContainers()
            for container in containers:
                status = container.stats(decode=None, stream = False)
                try:
                    # Calculate the change for the cpu usage of the container in between readings
                    # Taking in to account the amount of cores the CPU has
                    cpuDelta = status["cpu_stats"]["cpu_usage"]["total_usage"] - status["precpu_stats"]["cpu_usage"]["total_usage"]
                    systemDelta = status["cpu_stats"]["system_cpu_usage"] - status["precpu_stats"]["system_cpu_usage"]
                    #print("systemDelta: "+str(systemDelta)+" cpuDelta: "+str(cpuDelta))
                    cpuPercent = (cpuDelta / systemDelta) * (status["cpu_stats"]["online_cpus"]) * 100
                    cpuPercent = int(cpuPercent)
                    #print("cpuPercent: "+str(cpuPercent)+"%")
                    #Fetch the memory consumption for the container
                    mem = status["memory_stats"]["usage"]
                    mem = int(mem/1000000)
                    if startFlag == True and cpuPercent == 0: #Not logging CPU increase during startup of container, before test code executes.
                        startFlag = False
                        startEpoch = int(time.time()*1000)
                        print("Startflag set to False - let's go!")
                    if startFlag == False:
                        cpuLog.append(cpuPercent)
                        memLog.append(mem)
                except Exception as e:
                    #print("Error: "+str(e))
                    #print(json.dumps(status["memory_stats"]))
                    #status = container.stats(decode=None, stream = False)
                    #print(json.dumps(status))
                    #print(json.dumps(status, indent=4, sort_keys=True))
                    break
        except Exception as e:
            print("Error: "+str(e))
            pass
        if event.is_set():
            break
    # Write the logging to a file
    endEpoch = int(time.time()*1000)
    with open("output/"+str(dataframeN)+"_"+testType+"_"+containerStatsName, "w") as f:
        json.dump({"mem":memLog,"cpu":cpuLog,"timeSpent":float((endEpoch-startEpoch)/1000)}, f)
    print("################################################### dockerLog ended")

##Visualize Mem & CPU
def visMemCpu():
    from os import listdir
    from os.path import isfile, join
    outputFiles = [f for f in listdir("output/") if isfile(join("output/", f))]
    combDict = {}
    for file in outputFiles:
        if "containerStats.json" in file:
            with open("output/"+file, "r") as fp:
                data = json.load(fp)
            fileSplit = file.split("_")
            rows = fileSplit[0]
            lib = fileSplit[1]
            try:
                combDict[rows][lib] = {}
                combDict[rows][lib]["mem"] = data["mem"]
                combDict[rows][lib]["cpu"] = data["cpu"]
                combDict[rows][lib]["timeSpent"] = int(data["timeSpent"])
            except:
                combDict[rows] = {}
                combDict[rows][lib] = {}
                combDict[rows][lib]["mem"] = data["mem"]
                combDict[rows][lib]["cpu"] = data["cpu"]
                combDict[rows][lib]["timeSpent"] = int(data["timeSpent"])
    with open("output/all.json", "w") as f:
        json.dump(combDict, f)

    for rows in combDict.keys():
        # Dictionary to store a color for each library
        colorDict = {}
        colorDict["pandas"] = "tab:blue"
        colorDict["polars"] = "tab:orange"
        colorDict["spark"] = "tab:green"
        textString = " | "
        plt.clf()
        for lib in combDict[rows].keys():
            plt.plot(combDict[rows][lib]["mem"], label=lib, color=colorDict[lib])
            textString += lib + " " + str(combDict[rows][lib]["timeSpent"]) + "s | "
        plt.xticks([], [])
        plt.ylabel("Memory MB")
        plt.title(textString)
        plt.xlabel("Cardinality : "+ str(cardinality) + " Dataframe rows : " + rows)
        plt.legend(framealpha=1, frameon=True)
        plt.savefig("output/"+"{}_mem.png".format(str(rows)), dpi=300)
        plt.clf()
        for lib in combDict[rows].keys():
            plt.plot(combDict[rows][lib]["cpu"], label=lib, color=colorDict[lib])
        plt.xticks([], [])
        plt.ylabel("CPU %")
        plt.xlabel("Cardinality : "+ str(cardinality) + " Dataframe rows : " + rows)
        plt.title(textString)
        plt.legend(framealpha=1, frameon=True)
        plt.savefig("output/"+"{}_cpu.png".format(str(rows)), dpi=300)
    plt.yticks([], [])

def getContainers():
    containersReturn = []
    containers = dockerClient.containers.list(all=True)
    for container in containers:
        if imageName in str(container.image):
            containersReturn.append(container)
    return containersReturn
    
def removeContainers():
    containers = getContainers()
    for container in containers:
        if imageName in str(container.image):
            print("################################################### Deleting old container {}".format(container.name))
            try:
                container.stop()
                container.remove()
            except Exception as e:
                print("################################################### Error deleting old container {}".format(container.name))
                print(e)

def runContainer(testType,dataframeN):
    images = dockerClient.images.list(all=True)
    tries = 0
    if imageName in ' '.join(map(str, images)):
        while True:
            try:
                print("################################################### Image exist, starting container..")
                dockerClient.containers.run(imageName+":latest", environment = {"TEST_TYPE":testType,"DATAFRAME_N":dataframeN,"CALC_N":calcN,"CARDINALITY":cardinality})
                break
            except Exception as e:
                print("################################################### Error starting container..")
                if tries == 3:
                    print(e)
                    break
                tries += 1
                time.sleep(1)
                continue
    else:
        while True:
            try:
                print("################################################### Image doesn't exist, need to create it!")
                dockerClient.images.build(path = "./", tag = imageName)
                dockerClient.containers.run(imageName+":latest", environment = {"TEST_TYPE":testType,"DATAFRAME_N":dataframeN,"CALC_N":calcN,"CARDINALITY":cardinality})
                break
            except Exception as e:
                print("################################################### Error starting container..")
                if tries == 3:
                    print(e)
                    break
                tries += 1
                time.sleep(1)
                continue

if __name__ == "__main__":
    # for testType in testTypes:
    #     for dataframeN in dataframeNs:
    #         removeContainers()
    #         event = Event()
    #         print("Starting {} {}".format(testType,dataframeN))
    #         dockerLogThread = Thread(target=dockerLog, args=(event,testType,dataframeN,cardinality,))
    #         dockerLogThread.start()
    #         startEpoch = int(time.time()*1000)
    #         runContainer(testType,dataframeN)
    #         endEpoch = int(time.time()*1000)
    #         timeSpent = float((endEpoch-startEpoch)/1000)
    #         print("Time spent : {}".format(timeSpent))
    #         event.set()
    #         dockerLogThread.join()
    #Visualize with matplotlib
    visMemCpu()