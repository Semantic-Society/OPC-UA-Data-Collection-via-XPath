## Overview
This repository contains the application of the OPC UA export and lifting scripts at the [Foundry Institute](https://www.gi.rwth-aachen.de/en/) at RWTH Aachen University.
Our goal is to capture all semantic information (e.g. unit, range, accuracy, etc.) from all sensor nodes using a suitable XPath expression.

To achieve we use the results of the project [Transforming-XPath-to-OPC-UA](https://github.com/JohannesLipp/Transforming-XPath-to-OPC-UA).
The basic idea behind their project is to simplify data access in OPC UA by transforming XPath expressions to OPC UA.
To achieve this they:
- created a local export of the OPC UA server
- created an XML image based on the export
- created XPath expressions
- transformed these XPath expressions to OPC UA to extract data

**Since we cannot publish the OPC UA server data of the machines we use, we utilize the Prosys sample server for demonstration.**


## Requirements
- [python 3.7](https://www.python.org/downloads/)
- [opcua-asyncio](https://github.com/FreeOpcUa/opcua-asyncio) Version [0.9.0](https://pypi.org/project/asyncua/0.9.0/) (e.g., run `pip install asyncua==0.9.0`)
- [OPC UA Server/Prosys Example Server](https://www.prosysopc.com/products/opc-ua-simulation-server/)

## Usage
The program consists of the following four scripts:
- extractor_FI.py
- fileAnalysis.py
- OPCtranslator.py
- initializer.py
- RESTexporter.py

The three folders ```additionalFiles```, ```exports``` and ```semantics``` are created automatically if they do not exist, but the files such as the certificates for the servers must be added manually in the ```additionalFiles``` folder.
To execute the scripts the file ``initializer.py`` must be executed.
The machines to be extracted can be defined as arguments in the command line call to the ``initializer.py`` file.
(open the command line in the directory where the files are located, at least one machine name must be specified)
For example:
- If we want to run the scripts on all machines
    ```console
    python .\initializer.py InjectionMolding DosingFurnace RealTimeMS HPDC SprayHead RetrofitCS
    ```
- If we want to run the scripts only on the Dosing Furnace and Spray Head
    ```console
    python .\initializer.py DosingFurnace SprayHead
    ```
At the FI Institute the execution of the initializer.py is done automatically via a cronjob with the command:
```console
30 1 * * 0 /usr/bin/python3.7 initializer.py InjectionMolding DosingFurnace RealTimeMS HPDC SprayHead RetrofitCS
```
The cronjob is executed every Sunday at 01:30.

The extracted and processed json files are made available via a REST API server. For this we execute the file ```RESTexporter.py``` from the command line. 
For example:
- In general
    ```console
    python .\RESTexporter.py
    ```
- At the FI Institute 
    ```console
    /usr/bin/python3.7 RESTexporter.py
    ```

The data of the individual machines can be accessed via the url in the form ```http://ip:port/machinename```:
| Machine           | url                                       |
|-------------------|-------------------------------------------|
| **Injection Molding Unit**        |     ```http://127.0.0.1:8080/InjectionMolding```    |
| **Dosing Furnace**        |     ```http://127.0.0.1:8080/DosingFurnace```    |
| **Real Time Measurement System**   |  ```http://127.0.0.1:8080/RealTimeMS```  |
| **Spray Head**        |     ```http://127.0.0.1:8080/SprayHead```    |
| **HPDC Machine**       |    ```http://127.0.0.1:8080/HPDC```    |
| **Retrofit Cell Sensors** | ```http://127.0.0.1:8080/RetrofitCS``` |
| **Prosys Server** | ```http://127.0.0.1:8080/ProsysExServer``` |


## Specs
The scripts run at FI Institute on a [Raspberry Pi 4](https://www.raspberrypi.com/products/raspberry-pi-4-model-b/specifications/) with the following specifications:
 - OS:  Raspbian Buster x64 (Debian 10)
 - CPU: Broadcom BCM2711, Quad core Cortex-A72 (ARM v8) 64-bit SoC @ 1.5GHz
 - RAM: 8 GB
 - Directly connected to the machines via Ethernet

The computer on which we tested the scripts has the following specifications:
 - OS:  Windows 10 Education  64 bit
 - CPU: Intel Core i5-4210U @ 1.70GHz - 2.4GHz
 - RAM: 16 GB

## Requirements to connect to the machines
|                   | nothing | username | password | certifiate | private key |
|-------------------|---------|----------|----------|------------|-------------|
| **Injection Molding Unit**        |         |      X    |     X    |            |             |
| **Dosing Furnace**        |         |     X    |     X    |            |             |
| **Real Time Measurement System**   |    X    |          |          |            |             |
| **Spray Head**        |         |          |          |      X     |     (X)     |
| **HPDC Machine**       |         |     X    |     X    |      X     |      X      |
| **Retrofit Cell Sensors** |         |     X    |     X    |            |             |
| **Prosys Server** |    X    |          |          |            |             |

We use the same certificate for both the Spray Head and the HPDC Machine. 
The certificate is created as described [here](https://github.com/FreeOpcUa/opcua-asyncio/blob/master/examples/generate_certificate.sh).
The certificate and the private key are located in the [additionalFiles](https://git.rwth-aachen.de/semantic-toolbox/opc-ua-export-and-lifting/-/tree/5-using-the-opc-ua-exportand-and-lifiting-scripts-on-the-machines-of-the-foundry-institute-at-rwth/Foundry_Institute/additionalFiles) folder.
In general, the Spray Head does not require a private key, but since the [set_security_string](https://python-opcua.readthedocs.io/en/latest/opcua.client.html?highlight=security#opcua.client.client.Client.set_security_string) method of FreeOpcUa requires one, we need to specify it.


## additionalFiles
This folder contains the necessary certificate and private key for exporting from the respective machines as well as the UNECE_to_OPCUA.csv file to translate the extracted UnitIds to a human readable format.

## exports
This folder contains the exports and calculated XML images from the different machines.
To get the desired information we search the server for all nodes with the data type "EUInformation". 
Then we get the corresponding sensor node via the "parent" expression and get all children resp. sensor metadata from it via the "child" expression. The results of this are also located as `.json` files in this folder.

Since the HPDC Machine does not use datatypes like `EUInformation` and `EURange`, no results are found. If we search the server instead for nodeids ending with `...min` or `...max` we get 14 sensor nodes.

|                   | runtime of  export creation on Raspberri Pi [sec] | number of extracted nodes | runtime of xml  image calculation [sec] | number of nodes in xml image | number of cycles detected while calculation the xml image | runtime of semantic expression on Raspberry Pi[sec] | number of sensor nodes  in semantic expression |
|-------------------|---------------------------------------------------|---------------------------|-----------------------------------------|------------------------------|-----------------------------------------------------------|-----------------------------------------------------|------------------------------------------------|
| **Injection Molding Unit**        |                        121                        |           4,161           |                    11                   |             4,016            | 124                                                       | 12                                                  | 25                                             |
| **Dosing Furnace**        |                        105                        |           2,035           |                    3                    |             1,363            | 2                                                         | 1                                                   | 0                                              |
| **Real Time Measurement Systems**   |                         52                        |           1,940           |                    2                    |             1,357            | 0                                                         | 2                                                   | 191                                            |
| **Spray Head**        |                        144                        |           1,875           |                                   2      |               1,414               |                               0                            | 1                                                   | 0                                              |
| **HPDC Machine**       |                         88                        |           2,190           |                    3                    |             1,902            | 1                                                         | 1                                                   | 167                                              |
| **Retrofit Cell Sensors** |                       1,142                       |           35,257          |             563                            |            34,643            | 0                                                         | 5,645                                               | 135                                            |
| **Prosys Server** |                       118                       |           4,331          |             7                            |            3,064            | 0                                                         | 1                                               | 21                                            |




## semantics
This folder contains the results of the semantic expression.
The results are saved as a json file in the format ```'Machinename'Analyzed.json```.
For example:
- InjectionMoldingAnalyzed.json
- RetrofitCSAnalyzed.json
