from asyncua import Client
from pathlib import Path
import asyncio
import time
import logging
import json

async def getChildParent(clienta, nodea, nsarr):
    children = []
    parent = []
    nodeAttrs = []

    # NodeClass=2, NodeId=1, DisplayName=4, BrowseName=3, Description=5, Value=13, DataType=14
    attrs = []
    attributeIdsToExtract = [2,1,4,3,5,13,14]
    for attributeId in attributeIdsToExtract:
        try:
            attrs.append((await clienta.get_node(nodea).read_attributes([attributeId]))[0])
        except:
            attrs.append("None")
        
    for i in attrs:
        try:
            #NodeClass
            if attrs.index(i)==0: 
                nodeAttrs.append(i.Value.Value.name)
            #NodeId
            if attrs.index(i)==1:
                nodeAttrs.append(str(i.Value.Value))
            #DisplayName
            if attrs.index(i)==2: 
                nodeAttrs.append(i.Value.Value.Text)
            #BrowseName
            if attrs.index(i)==3: 
                nodeAttrs.append(i.Value.Value.Name)
            #Description
            if attrs.index(i)==4: 
                descString = i.Value.Value.Text
                if("\n" in descString):
                    # newlines (\n) in the export produce errors with further processing, replace them by "___"
                    descString = descString.replace("\n","___")
                nodeAttrs.append(descString)
                
            #Value
            if attrs.index(i)==5: 
                val = i.Value.Value
                if type(val) == int or type(val) == bool or type(val) == float or type(val) == str:
                    nodeAttrs.append(val)
                else:
                    nodeAttrs.append("None")
            #DataType
            if attrs.index(i)==6:
                dataType = i.Value.Value # NodeId of DataType
                # DataType String
                dataTypeString = (await clienta.get_node(dataType).read_attributes([4]))[0].Value.Value.Text 
                nodeAttrs.append(dataTypeString)
        except:
            nodeAttrs.append("None")
    # add UnitURI, UnitID, UnitRangeMin, UnitRangeMax
    try:
        # check if Identifier(attrs[6]) of Datatype == EUInformation(id=887) and append NamespaceURI        
        if (attrs[6]).Value.Value.Identifier == 887:
            nodeAttrs.append((attrs[5]).Value.Value.NamespaceUri)
            nodeAttrs.append((attrs[5]).Value.Value.UnitId)
        else:
            for i in range(2):
                nodeAttrs.append("None")
    except:
        for i in range(2):
                nodeAttrs.append("None")
    try:            
        # check if Identifier(attrs[6]) of Datatype == Range(id=884) and append Low Value
        if (attrs[6]).Value.Value.Identifier == 884:
            nodeAttrs.append((attrs[5]).Value.Value.Low)
            nodeAttrs.append((attrs[5]).Value.Value.High)
        else:
            for i in range(2):
                nodeAttrs.append("None")
    except:
        for i in range(2):
                nodeAttrs.append("None")

    # NodeClass, NodeId, ReferenceTypeId, TypeDefinition, DisplayName, BrowseName
    for i in await clienta.get_node(nodea).get_references():
        nodeidy = str(i.NodeId)
        if ";nsu=http://opcfoundation.org/UA/" in nodeidy:
            nodeidy = nodeidy.partition(";")[0]
        #print("---",nodeidy.partition(";")[0])
        try:
            reftypeid = str(i.ReferenceTypeId)
            typedef = str(i.TypeDefinition)
        except:
            reftypeid = "None"
            typedef = "None"

        if(i.IsForward == True):
            children.append([nodeidy,reftypeid,typedef])
        else:
            parent.append([nodeidy,reftypeid,typedef])
    export = [nodeAttrs,children,parent]
    return export

async def getDescendant(clienta, nodea):
    time1total = time.perf_counter()

    nsarr = await clienta.get_namespace_array()
    returnSet = set() # this set is returned
    checkSet = set() # NodeIds to check
    checkSet.update(nodea) # initialize checkSet with nodea
    totalDictOfDicts = {}
    inSet = set()
    counter = 1
    
    attributesToExtract = ["NodeClass","NodeId","DisplayName","BrowseName","Description","Value","DataType","UnitURI","UnitID","UnitRangeMin","UnitRangeMax"]
    attributesOfChildParent = ["NodeId", "ReferenceTypeId", "typeDefinition"]
    while (len(checkSet) > 0): # repeat until checkSet == 0
        childrenOfNode = set()
        for currentNode in checkSet:
            attributesOfCurrentNode = {}
            childListOfCurrentNode = [] # contains for each child a dictionary
            parentListOfCurrentNode = [] # contains for each parent a dictionary

            children = await getChildParent(clienta, currentNode, nsarr)
            if(counter%500==0):
                print("Nodes passed: ",str(counter))
            counter += 1
            # get nodeid
            if len(children[1]):
                for i in children[1]:
                    childrenOfNode.add(i[0])   
            if not currentNode in inSet:
                # add all attributes to file
                for keyX, valueX in zip(attributesToExtract, children[0]):
                    # create a dictionary of all extracted attributes of the current node:
                    attributesOfCurrentNode.update({keyX:str(valueX)})
                
                for child in children[1]:
                    childOfCurrentNode = {}
                    for keyX, valueX in zip(attributesOfChildParent, child):
                        # create a dictionary of the nodeid, RefTypeId and TypeDef of all children of the current node
                        childOfCurrentNode.update({keyX:valueX})
                    # chrea a list of dictionarys containing all children of the curren node
                    childListOfCurrentNode.append(childOfCurrentNode)

                for parent in children[2]:
                    parentOfCurrentNode = {}
                    # same as before with the parents instead of the children
                    for keyX, valueX in zip(attributesOfChildParent, parent):
                        parentOfCurrentNode.update({keyX:valueX})
                    parentListOfCurrentNode.append(parentOfCurrentNode)

                totalDictOfDicts.update({currentNode:[{"attributes":attributesOfCurrentNode}, {"children":childListOfCurrentNode}, {"parents":parentListOfCurrentNode}]})
            inSet.add(currentNode)
        checkSet.update(childrenOfNode)
        # a = all elements that are not in the intersection of checkSet and returnSet
        # x1 ^ x2 return the set of all elements in either x1 or x2, but not both
        a = checkSet^(returnSet.intersection(checkSet)) 
        returnSet.update(checkSet)
        checkSet = a
    
    time2total = time.perf_counter()
    totalDictOfDicts.update({"namespaces":nsarr})
    totalDictOfDicts.update({"extractionInfo":[{"totalExtractionTime":str("{:.4f}".format(time2total - time1total)), "extractedNodes":counter}]})

    return totalDictOfDicts

async def createExport(client, path, machineName):
    descOutput = await getDescendant(client,["i=84"])
    with open(Path(path / "exports" / (machineName+"Export.json")), "w") as outputFile:
                json.dump(descOutput, outputFile, indent=2)

async def initializeExport(path, specificMachine):
    machineDicts = [{"machineName": "InjectionMolding", "url": "opc.tcp://127.0.0.1:4840/InjectionMolding", "username": "username", "userpassword": "password"},\
                    {"machineName": "DosingFurnace", "url": "opc.tcp://127.0.0.1:4840/DosingFurnace", "username": "username", "userpassword": "password"},\
                    {"machineName": "Electronics", "url": "opc.tcp://127.0.0.1:4840/RealTimeMS"},\
                    {"machineName": "HPDC", "url": "opc.tcp://127.0.0.1:4840/HPDC", "username": "username", "userpassword": "password", "pathToCertAndKey": Path(path / "additionalFiles"), "security": "SignAndEncrypt"},\
                    {"machineName": "SprayHead", "url": "opc.tcp://127.0.0.1:4840/SprayHead", "pathToCertAndKey": Path(path / "additionalFiles"), "security": "Sign"},\
                    {"machineName": "RetrofitCS", "url": "opc.tcp://127.0.0.1:4840/RetrofitCS", "username": "username", "userpassword": "password"},\
                    {"machineName": "ProsysExServer", "url": "opc.tcp://MaxAcer.mshome.net:53535/AcerServer"}]

    for machine in machineDicts:
        machineName = machine["machineName"]
        if(specificMachine != machineName):
            continue
        print("creating export of ",machineName)
        
        # establish connection to OPC UA Server
        client=Client(url=machine["url"], timeout=30)
        if((machineName != "RealTimeMS") and (machineName != "SprayHead") and (machineName != "ProsysExServer")):
            try:
                client.set_user(machine["username"])
                client.set_password(machine["userpassword"]) 
            except:
                print("wrong username or password")
        if((machineName == "HPDC") or (machineName == "SprayHead")):
            client.application_uri = "urn:freeopcua:client"
            try:
                await client.set_security_string("Basic256Sha256," + machine["security"] + "," + str(Path(machine["pathToCertAndKey"] / "certificate.pem,")) + str(Path(machine["pathToCertAndKey"]/"key.pem")))
            except:
                print("a problem occured while setting the security string")
        try:
            await client.connect() # connect to opc ua server
            await createExport(client, path, machineName)
            await client.disconnect()
        except:
            print("an error occured while connecting to the " + machineName + " server")
