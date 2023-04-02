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

def dockerLog(event,testType,dataframeN):
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
                    cpuPercent = (cpuDelta / systemDelta) * len(status["cpu_stats"]["cpu_usage"]["percpu_usage"]) * 100
                    cpuPercent = int(cpuPercent)
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
                    break
        except Exception as e:
            pass
        if event.is_set():
            break
    # Write the logging to a file
    endEpoch = int(time.time()*1000)
    with open("output/"+str(dataframeN)+"_"+testType+"_"+containerStatsName, "w") as f:
        json.dump({"mem":memLog,"cpu":cpuLog,"timeSpent":float((endEpoch-startEpoch)/1000)}, f)
    print("################################################### dockerLog ended")

##Visualize Mem & CPU
def visMemCpu(testType,timeSpent,dataframeN,visAll=False):
    if visAll==True:
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
            textString = " | "
            plt.clf()
            for lib in combDict[rows].keys():
                plt.plot(combDict[rows][lib]["mem"], label=lib)
                textString += lib + " " + str(combDict[rows][lib]["timeSpent"]) + "s | "
            plt.xticks([], [])
            plt.ylabel("Memory MB")
            plt.title(textString)
            plt.xlabel("Dataframe rows : " + rows)
            plt.legend(framealpha=1, frameon=True)
            plt.savefig("output/"+"{}_mem.png".format(str(rows)), dpi=300)
            plt.clf()
            for lib in combDict[rows].keys():
                plt.plot(combDict[rows][lib]["cpu"], label=lib)
            plt.xticks([], [])
            plt.ylabel("CPU %")
            plt.xlabel("Dataframe rows : " + rows)
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
            container.remove()

def runContainer(testType,dataframeN):
    images = dockerClient.images.list(all=True)
    if imageName in ' '.join(map(str, images)):
        print("################################################### Image exist, starting container..")
        dockerClient.containers.run(imageName+":latest", environment = {"TEST_TYPE":testType,"DATAFRAME_N":dataframeN,"CALC_N":calcN})
    else:
        print("################################################### Image doesn't exist, need to create it!")
        dockerClient.images.build(path = "./", tag = imageName)
        dockerClient.containers.run(imageName+":latest", environment = {"TEST_TYPE":testType,"DATAFRAME_N":dataframeN,"CALC_N":calcN})

if __name__ == "__main__":
    testTypes = ["spark","polars","pandas"]
    dataframeNs = [2500,25000,250000,2500000]
    for testType in testTypes:
        for dataframeN in dataframeNs:
            removeContainers()
            event = Event()
            dockerLogThread = Thread(target=dockerLog, args=(event,testType,dataframeN,))
            dockerLogThread.start()
            startEpoch = int(time.time()*1000)
            runContainer(testType,dataframeN)
            endEpoch = int(time.time()*1000)
            timeSpent = float((endEpoch-startEpoch)/1000)
            print(timeSpent)
            event.set()
            dockerLogThread.join()

    visMemCpu("testType","timeSpent","dataframeN",True)