import asyncio
import csv
import json
import time

from fileAnalysis import createOfflineList, xmlexport, startXmlCreation
from extractor_FI import initializeExport
from pathlib import Path

async def following(client, connected, nodeList, offlineList):
    return await precfollow(client, connected, nodeList, False, offlineList)

async def preceding(client, connected, nodeList, offlineList):    
    return await precfollow(client, connected, nodeList, True, offlineList)
 
async def precfollow(client, connected, nodeList, direction, offlineList):
    # 1. ancestor-or-self of ndoeList
    # 2. siblings of all results
    # 3. results in tempSiblings
    tempSiblings = set()
    checkSet = set()
    checkSet.update(await ancestorOrSelf(client,connected,nodeList,offlineList,"i=84"))

    if(direction):# True if preceding
        siblings = await precedingSibling(client, connected, checkSet, offlineList)
    else: # if following
        siblings = await followingSibling(client, connected, checkSet, offlineList)
    tempSiblings.update(siblings)
    #rufe hier descendant or self auf allen elementen aus retrunSet auf
    returnSet = set()
    for i in tempSiblings:
        returnSet.update(await descendantOrSelf(client, connected, [i], offlineList, returnSet))
    return returnSet

async def precedingSibling(client, connected, nodeList, offlineList):
    return await siblingInit(client, connected, nodeList, offlineList, True)

async def followingSibling(client, connected, nodeList, offlineList):
    return await siblingInit(client, connected, nodeList, offlineList, False)

async def siblingInit(client, connected, nodeList, offlineList, preced):
    returnSet = set()
    for node in nodeList:
        siblings = await getSiblings(client, connected, node, offlineList)       
        returnSet.update(await precfollowSib(siblings, node, preced))

    return returnSet

async def getSiblings(client, connected, nodeList, offlineList):
    siblings = []
    for parentA in await parent(client, connected, [nodeList], offlineList):
        for sibls in await child(client, connected, [parentA], offlineList):
            siblings.append(sibls)
    return siblings

async def precfollowSib(siblings, initalnodeList, precOrFoll):
    # if precOrFoll == True -> preceding. if False-> following
    returnSet = set()
    for sib in siblings:
        if(precOrFoll): # preceding, every left sibling of initial node
            if(sib<initalnodeList):
                returnSet.add(sib)
        else: # following, every right sibling of initial node
            if(initalnodeList<sib):
                returnSet.add(sib)
    return returnSet


async def parent(client, connected, nodeList, offlineList):
    return await getChildParent(client, connected, nodeList, False, offlineList)

async def child(client, connected, nodeList, offlineList):
    return await getChildParent(client, connected, nodeList, True, offlineList)

async def getChildParent(client, connected, nodeList, direction, offlineList):
    nodeIdSet = set()
    if not connected:
        for nodes in nodeList:
            for i in offlineList:
                if(i[0]==nodes): # i[0] = NodeId
                    if(direction): # direction == True if Forward, == False if Inverse
                        nodeIdSet.update(set(i[1])) # i[1] = children of i[0]  
                        break
                    else:
                        nodeIdSet.update(set(i[2]))  # i[2] = parents of i[0]    
                        break
    else:
        
        for nodes in nodeList:
            for i in await client.get_node(nodes).get_references():
                if(i.IsForward == direction): # direction == True if Forward, == False if Inverse
                    if not str(i.ReferenceTypeId) == "i=40": # dont allow HasTypeDef references
                        nodeIdSet.add(str(i.NodeId))
                    
        intersecSet = set()
        for nodes in nodeList:
            for i in offlineList:
                if(i[0]==nodes): # i[0] = NodeId  
                    if(direction): # direction == True if Forward, == False if Inverse
                        intersecSet.update(set(i[1])) # i[1] = children of i[0]  
                        break
                    else:
                        intersecSet.update(set(i[2]))  # i[2] = parents of i[0]    
                        break

        nodeIdSet=nodeIdSet.intersection(intersecSet)

    return nodeIdSet   

async def descendant(client, connected, nodeList, offlineList):
    return await getDescendant(client, connected, nodeList, False, offlineList)

async def descendantOrSelf(client, connected, nodeList, offlineList, precfoll=set()):
    return await getDescendant(client, connected, nodeList, True, offlineList, precfoll)

async def ancestor(client, connected, nodeList, offlineList, rootnode):
    return await getAncestor(client, connected, nodeList, False, offlineList, rootnode)

async def ancestorOrSelf(client, connected, nodeList, offlineList, rootnode):
    return await getAncestor(client, connected, nodeList, True, offlineList, rootnode)


async def getDescendant(client, connected, nodeList, orSelf, offlineList, precfoll=set()):
    returnSet = set() # returned set
    checkSet = set() # nodeIds to check
    ancNodes = set() # ancestor nodes
    ancNodes.update(set(await ancestor(client,False,nodeList, offlineList,"i=84")))
    checkSet.update(nodeList) # initialize checkSet with nodeList


    if(len(precfoll)>0):
        returnSet.update(precfoll)

    # repeat until checkSet == 0
    while (len(checkSet) > 0): 
        # extract all children of each node in checkSet
        checkSet.update(await child(client, connected, checkSet, offlineList)) 
        # if flag==True, delete self node in the first cycle
        if(not orSelf): 
            for i in nodeList:
                checkSet.remove(i)
            orSelf = True
        # checkSet = all elements in checkSet without the elements in ancNodes
        checkSet = checkSet.difference(ancNodes)
        # a = all elemente of checkset without the elements in the interstection of checkSet and returnSet
        # x1 ^ x2 returns the set of all elements which are not in the intersection of x1 and x2 (symmetric difference)
        a = checkSet^(returnSet.intersection(checkSet)) 
        returnSet.update(checkSet)
        checkSet = a
    returnSet.update(await child(client, connected, returnSet, offlineList))
    return returnSet


    
async def getAncestor(client, connected, start, orSelf, offlineList, rootnode):
    # start is a list of nodes
    # endnode is always the rootnode
    # This method calculates all valid paths from the startnode
    # to the rootnode and returns a set with all traversed nodes
    pathnodes = set()
    for nodes in start:
        nodes=str(nodes)
        pathList = [[nodes]]
        while len(pathList)>0:
            path = pathList.pop(0)
            vertex = path[-1]
            if vertex == rootnode:
                pathnodes.update(path)
            for parentA in (await parent(client, connected, [vertex], offlineList)):
                parentA = str(parentA)
                if parentA not in path:
                    pathList.append(path + [parentA])
        if(not orSelf):
            if pathnodes:
                pathnodes.remove(nodes)
    return pathnodes

async def getAttributes(client, connected, nodeList, attrList, path, precalculatedExport = []):
    nodeAttributes = []
    if connected:
        for node in nodeList:
            # NodeId=1, NodeClass=2, DisplayName=4, BrowseName=3, Description=5, Value=13, DataType=14, UnitURI=100, UnitID=101, UnitRangeMin=102, UnitRangeMax=103
            attrs = await client.get_node(node).read_attributes(attrList)
            tempAttrs = []
            for i in attrList:
                try:
                    #NodeClass
                    if i==2: 
                        attribute = attrs[attrList.index(i)]
                        tempAttrs.append(attribute.Value.Value.name)
                    #NodeId
                    if i==1:
                        attribute = attrs[attrList.index(i)]
                        tempAttrs.append(str(attribute.Value.Value))
                    #DisplayName
                    if i==4:
                        attribute = attrs[attrList.index(i)]
                        tempAttrs.append(attribute.Value.Value.Text)
                    #BrowseName
                    if i==3:
                        attribute = attrs[attrList.index(i)]
                        tempAttrs.append(attribute.Value.Value.Name)
                    #Description
                    if i==5:
                        attribute = attrs[attrList.index(i)]
                        tempAttrs.append(attribute.Value.Value.Text)
                    #Value
                    if i==13:
                        attribute = attrs[attrList.index(i)]
                        val = attribute.Value.Value
                        if type(val) == int or type(val) == bool or type(val) == float or type(val) == str:
                            tempAttrs.append(val)
                        else:
                            tempAttrs.append("None")
                    #DataType
                    if i==14:
                        attribute = attrs[attrList.index(i)]
                        dataType = attribute.Value.Value # NodeId of DataType
                        dataTypeString = (await client.get_node(dataType).read_attributes([4]))[0].Value.Value.Text 
                        tempAttrs.append(dataTypeString)
                    
                except:
                    tempAttrs.append("None")
                try:
                    # UnitURI
                    if i==100:
                        if ((await client.get_node(node).read_attributes([14]))[0]).Value.Value.Identifier == 887:
                            tempAttrs.append(((await client.get_node(node).read_attributes([13]))[0]).Value.Value.NamespaceUri)
                        else: tempAttrs.append("None")
                except:
                    tempAttrs.append("None")
                try:
                    # UnitID
                    if i==101:
                        if ((await client.get_node(node).read_attributes([14]))[0]).Value.Value.Identifier == 887:
                            tempAttrs.append(((await client.get_node(node).read_attributes([13]))[0]).Value.Value.UnitId)
                        else: tempAttrs.append("None")
                except:
                    tempAttrs.append("None")
                try:
                    # UnitRangeMin, UnitRangeMax
                    if i==102 or i==103:
                        if ((await client.get_node(node).read_attributes([14]))[0]).Value.Value.Identifier == 884:
                            vals = ((await client.get_node(node).read_attributes([13]))[0])
                            if i==102: tempAttrs.append(vals.Value.Value.Low)
                            else: tempAttrs.append(vals.Value.Value.High)
                        else:
                            tempAttrs.append("None")
                except:
                    tempAttrs.append("None")

            nodeAttributes.append(tempAttrs)
    else:
        # NodeId=1, NodeClass=2, DisplayName=4, BrowseName=3, Description=5, Value=13, DataType=14, UnitURI=100, UnitID=101, UnitRangeMin=102, UnitRangeMax=103
        attrOfNodeList = []
        if(precalculatedExport):
            export = precalculatedExport
        else:
            export = await xmlexport(1, path)
        for node in nodeList:
            for nodeAttrs in export:
                if node==nodeAttrs[0]:
                    attrOfNodeList.append(nodeAttrs)
                    break
        for node in attrOfNodeList:
            tempAttrs = []
            for AtrNumber in attrList:
                if AtrNumber==1: tempAttrs.append(node[0])# NodeId
                elif AtrNumber==2: tempAttrs.append(node[1])# NodeClass
                elif AtrNumber==4: tempAttrs.append(node[2])# DisplayName
                elif AtrNumber==3: tempAttrs.append(node[3])# BrowseName
                elif AtrNumber==5: tempAttrs.append(node[4])# Description
                elif AtrNumber==13: tempAttrs.append(node[5])# Value
                elif AtrNumber==14: tempAttrs.append(node[6])# DataType
                elif AtrNumber==100: tempAttrs.append(node[7])# UnitURI
                elif AtrNumber==101: tempAttrs.append(node[8])# UnitID
                elif AtrNumber==102: tempAttrs.append(node[9])# UnitRangeMin
                elif AtrNumber==103: tempAttrs.append(node[10])# UnitRangeMax
            nodeAttributes.append(tempAttrs)
    return nodeAttributes


async def xPathExpression(uaExportPath, dictionary, client, connected):
    """The NodeIds of the passed dictionary are read and then "unitId" and 
    "range" are read from the associated nodeids with a transformed XPath 
    expression from the created export or server."""

    offlineList = await createOfflineList("i=84", uaExportPath) 
    parsedNodeIds = dictionary # get all nodeIds from "dictionary"

    returnData = [] # contains a list of dictionarys wiht the following keys: 'nodeId', 'range', 'unitId'
    for node in parsedNodeIds: # iterate through each nodeId (from the dictionary)
        nodeData = {"nodeId":str(node)}
        siblings = await child(client, connected, [node], offlineList) # get all siblings from "node"
        attrs = await getAttributes(client,connected,siblings,[14,101,102,103],uaExportPath) # get the following attributes from each sibling: 14=DataType, 101=UnitID, 102=UnitRangeMin, 103=UnitRangeMax
        
        for attributeType in attrs:
            if attributeType[0] == "EUInformation":
                nodeData.update({"unitId": str(attributeType[1])})
            elif attributeType[0] == "Range":
                nodeData.update({"range": [str(attributeType[2]),str(attributeType[3])]})
        returnData.append(nodeData)
    return returnData

async def getUnitDescription(csvList, unitId):
    # csvList contains the following entries: ['UNECECode', 'UnitId', 'DisplayName', 'Description']
    # if the extracted unitid and that of the csvList are identical, return its description
    for row in csvList:
        if(row[1]==str(unitId)):
            return row[3]

async def expandDictionary(dictionary, path):
    with open(Path(path / "additionalFiles"/'UNECE_to_OPCUA.csv'), 'r', encoding='ISO-8859-1') as unitIdToDescription:
        csvList =  list(csv.reader(unitIdToDescription))
        for i in dictionary:
            if "unitId" in i:
                unitIdNumber = i["unitId"]
                # unitIdNumber = 5066068 # example for unitid 5066068 -> millimetre
                if (unitIdNumber != "-1"): # unitDescription is added 
                    i.update({"unitDescription":await getUnitDescription(csvList, unitIdNumber)})
    return dictionary

async def getNodeAttributes(uaExportPath, client, connected, offlineList, precalculateExportList, sibAttrDict, singleSiblings, siblDisplayName, siblValue):
    sibAttrDict = {}
    if(siblDisplayName == "Update_Rate"):
        updateInfo = list(await child(client, connected, [singleSiblings], offlineList))
        for information in updateInfo:
                        # DataType=14, UnitID=101, UnitRangeMin=102, UnitRangeMax=103
            attrsOfInfo = (await getAttributes(client, connected, [information], [14, 101, 102, 103], uaExportPath, precalculateExportList))[0]
            if(attrsOfInfo[0] == "EUInformation"):
                sibAttrDict.update({"Update_Rate_UnitId": attrsOfInfo[1]})
            elif(attrsOfInfo[0] == "Range"):
                sibAttrDict.update({"Update_Rate_Range": [attrsOfInfo[2], attrsOfInfo[3]]})
    sibAttrDict.update({siblDisplayName: siblValue})
    return sibAttrDict

async def retrofitCSToJson(uaExportPath, client, connected, machineName, path):
    a1 = time.perf_counter()
    """The following expression searches the entire OPC UA Server or the Server's export for all sensor nodes and extracts the associated data such as units and measuring ranges"""
    """retruns list of list with [[ParentNodeId, ChildNodeId, ChildDataType, ChildUnitID, ChildUnitRangeMin, ChildUnitRangeMax], ...]"""
    offlineList = await createOfflineList("i=84", uaExportPath)

    descendants = await descendantOrSelf(client, connected, ["i=85"], offlineList)

    attributeList = await getAttributes(client, connected, descendants, [1, 14], uaExportPath)
    precalculateExportList = await xmlexport(1, uaExportPath)
    
    nodes=set()
    
    for attribute in attributeList:
        if (attribute[1] == "EUInformation"): # normal expression
            # the RetrofitCS has a structure where the third parent of the EUinformation node is the sensor node
            # thus we call three times the parent function
            firstParent = await parent(client, connected, [attribute[0]], offlineList)
            secondParent = await parent(client, connected, [firstParent.pop()], offlineList)
            thirdParent = await parent(client, connected, [secondParent.pop()], offlineList)
            nodes.add(thirdParent.pop())

    nodesChecked = set()
    for i in nodes:
        if(".Aggregation") in i:
            continue
        nodesChecked.add(i)
    
    nodesList = sorted(list(nodesChecked))
    sensorNodeDict = {}
    for parentX in nodesList:
        a11 = time.perf_counter()
        firstChildNodeDict = {}
        siblingsOfParentNode = list(await child(client, connected, [parentX], offlineList))
        for i in siblingsOfParentNode[::-1]:
            if("ns=5;" in i):
                siblingsOfParentNode.remove(i)
        dispNameList = await getAttributes(client, connected, siblingsOfParentNode, [4], uaExportPath)
        for sibling,dispName in zip(siblingsOfParentNode,dispNameList):
            if(len(dispName)<1):
                continue
            dispName = dispName[0]#

            siblingsOfSiblingNode = list(await child(client, connected, [sibling], offlineList))
            siblingAttributes = await getAttributes(client, connected, siblingsOfSiblingNode, [4, 13], uaExportPath, precalculateExportList)
            
            sibAttrDict = {}
            for singleSiblings, singleSiblingAttribute in zip(siblingsOfSiblingNode,siblingAttributes):
                siblDisplayName = singleSiblingAttribute[0]
                siblValue = singleSiblingAttribute[1]

                if(dispName == "Aggregation"):
                    aggrSibAttrDict = {}
                    childrenOfAggregation = list(await child(client, connected, siblingsOfSiblingNode, offlineList))
                    childAttributes = await getAttributes(client, connected, childrenOfAggregation, [4, 13], uaExportPath, precalculateExportList)

                    for aggrChild, singleAggrChildAttribute in zip(childrenOfAggregation,childAttributes):
                        childDisplayName = singleAggrChildAttribute[0]
                        childValue = singleAggrChildAttribute[1]
                        nodeData = await getNodeAttributes(uaExportPath, client, connected, offlineList, precalculateExportList, aggrSibAttrDict, aggrChild, childDisplayName, childValue)
                        aggrSibAttrDict.update(nodeData)
                    
                    sibAttrDict.update({siblDisplayName:aggrSibAttrDict})
                else:
                    nodeData = await getNodeAttributes(uaExportPath, client, connected, offlineList, precalculateExportList, sibAttrDict, singleSiblings, siblDisplayName, siblValue)
                    sibAttrDict.update(nodeData)

            firstChildNodeDict.update({dispName: sibAttrDict})

        sensorNodeDict.update({parentX: firstChildNodeDict})
        a22 = time.perf_counter()
        print(len(sensorNodeDict),"\ttime per Sensornode\t",a22-a11)
    a2 = time.perf_counter()
    print("\ntotal time\t", a2-a1)
    with open(Path(path / (machineName+"Analyzed.json")), "w") as final:
        json.dump(sensorNodeDict, final, indent=2)


    
async def allSensorNodes(uaExportPath, client, connected, machineName):
    """The following expression searches the entire OPC UA Server or the Server's export for all sensor nodes and extracts the associated data such as units and measuring ranges"""
    """retruns list of list with [[ParentNodeId, ChildNodeId, ChildDataType, ChildUnitID, ChildUnitRangeMin, ChildUnitRangeMax], ...]"""

    offlineList = await createOfflineList("i=84", uaExportPath)
    precalculateExportList = await xmlexport(1, uaExportPath)

    descendants = await descendantOrSelf(client, connected, ["i=85"], offlineList)
    attributeList = await getAttributes(client, connected, descendants, [1, 14], uaExportPath, precalculateExportList)
    nodes=set()
   
    for attribute in attributeList:
        if(machineName != "HPDC" and machineName != "RetrofitCS"):
            if (attribute[1] == "EUInformation"): # normal expression
                nodes.add(attribute[0])
        elif(machineName == "HPDC"):
            # only for HPDC because it doesnt use datatypes like "EUInformation". We check if the last three letters of the nodeid are "min" or "max" and treat it like "EUInformation"
            lastThreeLetters = (attribute[0][-3:]).lower()
            if((lastThreeLetters == "min") or (lastThreeLetters=="max")):
                nodes.add(attribute[0])

    if(machineName != "RetrofitCS"):
        parentNodes = list(await parent(client, connected, nodes, offlineList)) # regular expression
    else:
        # the RetrofitCS has a 
        firstParentNodes = list(await parent(client, connected, nodes, offlineList)) #set only for RetrofitCS
        parentNodes = list(await parent(client, connected, firstParentNodes, offlineList)) #set only for RetrofitCS
    siblings = [] # list of lists-> [[sibl1Parent1,sibl2Parent1,...][sibl1Parent2,sibl2Parent2,...][sibl1Parent3,sibl2Parent3,...][]...]
    for i in parentNodes:
        siblings.append(list(await child(client, connected, [i], offlineList)))

    # NodeId=1, NodeClass=2, DisplayName=4, BrowseName=3, Description=5, Value=13, DataType=14, UnitURI=100, UnitID=101, UnitRangeMin=102, UnitRangeMax=103
    siblAttrs = [] #like siblings but this list contains instead of the single nodeid of a sibling a list of its attributes (nodeid,datatype,unitid,....) -> [[attrList],[],[],[]]
    counter = 0
    for eachParent in siblings:
        singleParent = await getAttributes(client, connected, eachParent, [1, 14, 101, 102, 103, 4, 13], uaExportPath, precalculateExportList)
        siblAttrs.append(singleParent)
        counter +=1
        if(counter%100==0):
            print(len(siblAttrs) ,"/",len(siblings))

    returnData = []
    for singleParent, siblingList in zip(parentNodes,siblAttrs):
        nodeData = {"nodeId": singleParent} # nodeid of parent/sensornode
        for singleSibling in siblingList:
            datatype = singleSibling[1]
            if((datatype == "EUInformation") or (datatype == "Range")):
                if(datatype == "EUInformation"):
                    nodeData.update({"unitId": singleSibling[2]})
                else:
                    nodeData.update({"range": [singleSibling[3], singleSibling[4]]})
            else:
                displayname = singleSibling[5]
                value = singleSibling[6]
                if((displayname != "None") and (value != "None")):
                    nodeData.update({displayname: value})
        returnData.append(nodeData)

    return returnData

async def returnDataToJson(returnData, path, ExportFileName):
    # sort the entries and create a json file to store them
    returnData2 = []
    for elemDict in returnData:
        tempDict = {"nodeId": elemDict["nodeId"]}
        keyList = []

        for key in elemDict:
            if(key != "nodeId"):
                keyList.append(key)       
        for key in sorted(keyList):
            tempDict.update({key: elemDict[key]})

        returnData2.append(tempDict)

    with open(Path(path / (ExportFileName+"Analyzed.json")), "w") as final:
        json.dump(returnData2, final, indent=2)


async def startTranslation(loop, machinesToParse, directoryPath):
    path = directoryPath   

    createXMLexport = False # set to True if an XML image should be created from the export
   
    for machineName in machinesToParse:
        # create export 
        await initializeExport(path, machineName)
        uaExportPath = Path(path / "exports" / (machineName + "Export.json")) # path of the export
        alanyzedFiles = Path(path / "semantics") 
        # create an XML image of the export of the OPC UA Server
        if createXMLexport: 
            print("Generating XML document") 
            uaXmlPath = Path(path / "exports" / (machineName + "XML.xml")) # path of the xml image
            await startXmlCreation(uaXmlPath, uaExportPath) 
        #dataAfterExpression = await xPathExpression(uaExportPath, listOfNodeIdsToParse, client, connected) # parse only specific nodes
        print("executing semanitc expression on: " + machineName)
        if(machineName == "RetrofitCS"):
            await retrofitCSToJson(uaExportPath, None, False, machineName, alanyzedFiles)
        else:
            dataAfterExpression = await allSensorNodes(uaExportPath, None, False, machineName) # search server for sensornode information
            # Expand the input dictionary by the new fields.
            returnData = await expandDictionary(dataAfterExpression, path)
            await returnDataToJson(returnData, alanyzedFiles, machineName)
        print("--------------")


def initializer(machinesToParse, directoryPath):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(startTranslation(loop,machinesToParse, directoryPath))
