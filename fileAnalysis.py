import time
import itertools
import json

from lxml import etree as ET
async def openPath(path):
    jsonFile = json.load(open(path))
    # removing the entries we dont need
    del jsonFile["namespaces"]
    del jsonFile["extractionInfo"]
    return jsonFile

async def xmlexport(children, path):
    # children = 0: NodeId list with startnode and all children [[i=84,i=61,i=85,i=86,i=87]]
    # children = 1: Attribute list of each node
    # children = 2: RefType and Typedef list with [[predecessorNodeID,NodeId,RefType,TypeDef],...]
    export = await openPath(path)

    # create list containing all NodeIds of the Server
    allNodeIds = [nodeId for nodeId in export]

    returnList = []
    if(children == 0):
        # generates a list looking like: [[NodeId1,child1,child2,...,childN],[NodeId2,child1,...]]
        # first entry is the context node, all following are its children
        for eachNode in allNodeIds:
            childList = [eachNode]
            for childInfo in export[eachNode][1]["children"]:
                childList.append(childInfo["NodeId"])
            if(len(childList) > 1):
                returnList.append(childList)
            
    elif(children == 1):
        # generates a list looking like: [[NodeId,NodeClass,Displayname,Browsename,Description,Value,DataType, UnitURI...],[NodeId,NodeClass,...]]
        # eg.: attributeList = ['i=84', 'Object', 'Root', 'Root', 'None', 'None', 'None', 'None', 'None', 'None', 'None']
        # eg.: returnList = [['i=84', 'Object', 'Root', 'Root', 'None', 'None', 'None', 'None', 'None', 'None', 'None'],...]
        for eachNode in allNodeIds:
            attributeList = []
            attrsToExtract = ["NodeId","NodeClass","DisplayName","BrowseName","Description","Value","DataType","UnitURI","UnitID","UnitRangeMin","UnitRangeMax"]
            nodeAttributesDict = export[eachNode][0]["attributes"]
            for attribute in attrsToExtract:
                attributeList.append(nodeAttributesDict[attribute])
            returnList.append(attributeList)

    elif(children == 2):
        # creates list with RefType and Typedef like [[predecessorNodeID,NodeId,RefType,TypeDef],...]
        for eachNode in allNodeIds:
            for refPerNode in export[eachNode][1]["children"]:
                returnList.append([eachNode, refPerNode["NodeId"], refPerNode["ReferenceTypeId"], refPerNode["typeDefinition"]])

    if(children == 3):
        # generates a list looking like: [[NodeId1,child1,child2,...,childN],[NodeId2,child1,...]]
        # first entry is the context node, all following are its children
        for eachNode in allNodeIds:
            childList = [eachNode]
            for nodeid in export[eachNode][1]["children"]:
                childList.append(nodeid["NodeId"])
            returnList.append(childList)
    return returnList


async def pathFinder(start,childList,offlineList, path):
    # return all paths (from the startnode) until:
    # path has no more child nodes or last node already exists in path (loop)
    time1 = time.perf_counter()
    pathList = [[start]] # [[['Object', 'ns=4;i=1001']]]
    result = []
    f = 0
    childList = []
    for i in offlineList:
        temp = [i[0]]
        for child in i[1]:
            temp.append(child)
        childList.append(temp)
    checkList = []
    dontvisit = set()
    # returns list like: [['i=84', 'i=61', 'i=40', 'i=0'],....] first element is the NodeId of browse node, second NodeId of target node, third reftypeId, fourth typedef
    # if the third element is i=40 (HasTypeDefinition), add the second node to blockedNodes
    typeDefnodes = await xmlexport(2, path) 
    for node in typeDefnodes:
        if node[2]=="i=40":
            dontvisit.add(node[1])
    # i=2274 Server Diagnostics node. Useless Information -> block
    dontvisit.add("i=2274")
    
    # add all unique nodes to checkList
    for i in childList:
        checkList.append(i[0])
    while len(pathList)>0:
        path = pathList.pop(0)
        vertex = path[-1]
        childrenOfVertex = []

        for child in childList:
            if child[0] == vertex:
                childrenOfVertex = child[1:]
                break

        if not childrenOfVertex:
            if vertex in dontvisit:
                result.append(path[:-1])
            else: result.append(path)

        for child in childrenOfVertex:
            if vertex in dontvisit:
                # exclude last element
                result.append(path[:-1])
                continue
            if child not in path:
                pathList.append(path + [child])
            else:
                result.append(path + [child])

        f += 1
        if(f%100000==0):
            pathss = set()
            for childrenOfVertex in result:
                for i in childrenOfVertex:
                    pathss.add(i)
            pathssc = set()
            for childrenOfVertex in pathList:
                for i in childrenOfVertex:
                    pathssc.add(i)

            # cycles x1000         # stack            # unique nodes in stack    # result length(number of paths)  # unique nodes in result
            print(str(f/1000)+ " "+ str(len(pathList)) + " "+ str(len(pathssc))+ " "+ str(len(result))+ " "+ str(len(pathss))+ " "+ str(("{:.3f}".format(time.perf_counter()-time1))))
    time2 = time.perf_counter()
    print("all paths calculated in: ", str(time2 - time1) + " seconds")

    result.sort()
    uniquePaths=list(result for result,_ in itertools.groupby(result))

    cycleList = []
    for i in uniquePaths:
        if i.count(i[-1])>1:
            if(not i in cycleList):
                cycleList.append(i)
    print("Number of cycles: ",len(cycleList))
    return uniquePaths

async def createXML(startnode, offlineList, path):

    paths = await pathFinder(startnode, await xmlexport(0, path), offlineList, path)
    attrList = await xmlexport(1, path)
    refTypeList = await xmlexport(2, path)
    rootnode = []
    for i in attrList:
        if i[0] == startnode:
            rootnode = i
            break
        
    root = ET.Element(rootnode[1])
    root.attrib['NodeId']=rootnode[0]
    root.attrib['ReferenceTypeId']=""
    root.attrib['TypeDefinition']=""
    root.attrib['DisplayName']=rootnode[2]
    root.attrib['BrowseName']=rootnode[3]
    root.attrib['Description']=rootnode[4]
    root.attrib['Value']=rootnode[5]
    root.attrib['DataType']=rootnode[6]

    root.attrib['UnitURI']=rootnode[7]
    root.attrib['UnitID']=rootnode[8]
    root.attrib['UnitRangeMin']=rootnode[9]
    root.attrib['UnitRangeMax']=rootnode[10]

    for pfad in paths:
        node = root
        for currentNode in pfad:
            # get attributes of current node
            attribute = []
            for attr in attrList:
                if attr[0] == currentNode:
                    attribute = attr
                    break

            if len(attribute)==0:
                attribute.append(currentNode)
                for i in range(10):
                    attribute.append("error")
            if not currentNode == root.get("NodeId"):
                # get refType and typeDef according to parent node
                refTypeId = ""
                typeDef = ""
                for i in refTypeList:
                    if i[1] == currentNode and i[0] == node.get("NodeId"):
                        refTypeId = i[2]
                        typeDef = i[3]
                        break
                
                # check if the node has children. If not, create node with its parameters,
                # else check if the NodeId of one of the cildren is equal to the next node
                # in pfad. Repeat until pfad is empty or node child NodeId is unequal to node in pfad 
                if len(list(node)): # list(node) returns all children of node
                    nodeExists = 0
                    # Check if pfad[currentNode] is an attribute of a child of node
                    for element in list(node):
                        #if element.get("NodeId") == pfad[currentNode][1]:
                        if element.get("NodeId") == currentNode:
                            nodeExists=element

                    # if pfad[currentNode] already exists, update node to pfad[currentNode]
                    if not nodeExists == 0:
                        node = nodeExists
                    
                    else:
                        eleme = ET.SubElement(node,attribute[1])
                        eleme.attrib['NodeId']=currentNode
                        eleme.attrib['ReferenceTypeId']=refTypeId
                        eleme.attrib['TypeDefinition']=typeDef
                        eleme.attrib['DisplayName']=attribute[2]
                        eleme.attrib['BrowseName']=attribute[3]
                        eleme.attrib['Description']=attribute[4]
                        eleme.attrib['Value']=attribute[5]
                        eleme.attrib['DataType']=attribute[6]

                        eleme.attrib['UnitURI']=attribute[7]
                        eleme.attrib['UnitID']=attribute[8]
                        eleme.attrib['UnitRangeMin']=attribute[9]
                        eleme.attrib['UnitRangeMax']=attribute[10]

                        # sort the nodes by their NodeId
                        prevNodeId = ""
                        for element in list(node):
                            # if currentNode smaller than the first node of all children, add currentNode left/previous to the first node
                            if not prevNodeId:
                                prevNodeId = element.get("NodeId")
                            else:
                                # run until currentNode is smaller than element. If found, add currentNode left/previous to the first node 
                                if currentNode < prevNodeId:
                                    element.addprevious(eleme)
                                    break
                                else:
                                    if currentNode < element.get("NodeId"):
                                        element.addprevious(eleme)
                                        break
                        node = eleme
                else:
                    eleme = ET.SubElement(node,attribute[1])
                    eleme.attrib['NodeId']=currentNode
                    eleme.attrib['ReferenceTypeId']=refTypeId
                    eleme.attrib['TypeDefinition']=typeDef
                    eleme.attrib['DisplayName']=attribute[2]
                    eleme.attrib['BrowseName']=attribute[3]
                    eleme.attrib['Description']=attribute[4]
                    eleme.attrib['Value']=attribute[5]
                    eleme.attrib['DataType']=attribute[6]

                    eleme.attrib['UnitURI']=attribute[7]
                    eleme.attrib['UnitID']=attribute[8]
                    eleme.attrib['UnitRangeMin']=attribute[9]
                    eleme.attrib['UnitRangeMax']=attribute[10]
                    node = eleme
    print("Number of Nodes: ",str(len(root.xpath(".//*"))))
    return ET.tostring(root, pretty_print=True).decode()


async def createOfflineList(startNode, path):
    # create an array looking like: [[NodeId,[NodeIdChild1,...,NodeIdChildN],[NodeIdParent1,...,NodeIdParentN]],....]
    # this array contains all NodeIds with their cildren and parents
    # This enables the calculation of XQuery expressions without being connected to the OPC UA server.

    export = await openPath(path)
    returnList = []
    for eachNode in export:
        # create a list containing all nodes with thier children and parents
        # eg for the list with the nodeid of the starting node: [['i=84', ['i=61', 'i=85', 'i=86', 'i=87'], []], ...]
        # "i=84" is the starting node, ['i=61', 'i=85', 'i=86', 'i=87'] are its children and [] are its parents (empty because its the starting node)
        childList = []
        for nodeid in export[eachNode][1]["children"]:
            childList.append(nodeid["NodeId"])
        parentList = []
        for nodeid in export[eachNode][2]["parents"]:
            parentList.append(nodeid["NodeId"])
        returnList.append([eachNode, childList, parentList])

    # block all Nodes receiving a HasTypeDefinition reference
    blockedNodes = set()
    # returns list like: [['i=84', 'i=61', 'i=40', 'i=0'],....] first element is the NodeId of browse node, second NodeId of target node, third reftypeId, fourth typedef
    # if the third element is i=40 (HasTypeDefinition), add the second node to blockedNodes
    typeDefnodes = await xmlexport(2, path) 
    for node in typeDefnodes:
        if node[2]=="i=40":
            blockedNodes.add(node[1])
    blockedNodes.add("i=2274") # Server Diagnostics
    # subsetOfNodes are all descendant nodes of the given node. this are all nodes that are in the xml tree
    subsetOfNodes = await getDescendantX([startNode],returnList,blockedNodes)
    allNodes = await getDescendantX(["i=84"],returnList,[])
    # all nodes that are in allNodes but not in subsetOfNodes are invalid nodes
    invalidNodes = allNodes^subsetOfNodes
    wrongNS = []
    for i in invalidNodes:
        if(i[0:2] != "i="):
            wrongNS.append(i)
    for i in wrongNS:
        invalidNodes.remove(i)
    ret = []
    for i in returnList:
        if i[0] in invalidNodes:
            continue
        children=[]
        for child in i[1]:
            if not child in invalidNodes:
                children.append(child)
        parents=[]
        for parent in i[2]:
            if not parent in invalidNodes:
                parents.append(parent)
        ret.append([str(i[0]),children,parents])
    return ret

async def getDescendantX(nodeList, offlineList, blockedNodes):
    returnSet = set() # this set will be returned
    checkSet = set() # NodeIds to check
    notToCheck = set()
    notToCheck.update(blockedNodes)
    checkSet.update(nodeList) # initialize checkSet with nodeList
    # repeat until checkSet == 0
    while (len(checkSet) > 0):
        # extract all children from each node in checkSet
        checkSet.update(await childX(checkSet, offlineList)) 
        # checkSet = all elements in checkSet without the elements in notToCheck
        checkSet = checkSet.difference(notToCheck)
        # a = all elements that are not in the intersection of checkSet and returnSet
        # x1 ^ x2 return the set of all elements in either x1 or x2, but not both (symmetric difference)
        a = checkSet^(returnSet.intersection(checkSet)) 
        returnSet.update(checkSet)
        checkSet = a

    return returnSet
async def parentX(nodeList, offlineList):
    return await getChildParentX(nodeList, False, offlineList)

async def childX(nodeList, offlineList):
    return await getChildParentX(nodeList, True, offlineList)

async def getChildParentX(nodeList, direction, offlineList):
    nodeIdSet = set()
    for nodes in nodeList:
        for i in offlineList:
            if(i[0]==nodes): # i[0] = NodeId
                if(direction): # direction == True if Forward, == False if Inverse
                    nodeIdSet.update(set(i[1])) # i[1] = children of i[0]
                    break
                else:
                    nodeIdSet.update(set(i[2]))  # i[2] = parents of i[0]    
                    break
    
    return nodeIdSet 

async def startExportToXML(path):
    startNode = "i=84"
    offlineList = await createOfflineList(startNode, path)
    createdXML = await createXML(startNode, offlineList, path)
    return createdXML

async def startXmlCreation(fileLocation, exportPath):
    time1 = time.perf_counter()

    file_object = open(fileLocation, 'w')
    for i in await startExportToXML(exportPath):
        file_object.write(str(i))

    time2 = time.perf_counter()
    print("Duration: ", str(time2 - time1) + " seconds")
