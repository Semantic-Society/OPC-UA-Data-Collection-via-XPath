from flask import Flask
from pathlib import Path
import json

print("running flask server...")
currentDirectory = Path(__file__).parent.absolute()
app = Flask(__name__)

@app.route("/InjectionMolding")
def get_injectionMolding():
    with open(Path(currentDirectory / "semantics" / "InjectionMoldingAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString
@app.route("/DosingFurnace")
def get_dosingFurnace():
    with open(Path(currentDirectory / "semantics" / "DosingFurnaceAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString
@app.route("/RealTimeMS")
def get_realTimeMS():
    with open(Path(currentDirectory / "semantics" / "RealTimeMSAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString
@app.route("/HPDC")
def get_hpdc():
    with open(Path(currentDirectory / "semantics" / "HPDCAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString
@app.route("/SprayHead")
def get_sprayHead():
    with open(Path(currentDirectory / "semantics" / "SprayHeadAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString
@app.route("/RetrofitCS")
def get_retrofitCS():
    with open(Path(currentDirectory / "semantics" / "RetrofitCSAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString
@app.route("/ProsysExServer")
def get_prosysExServer():
    with open(Path(currentDirectory / "semantics" / "ProsysExServerAnalyzed.json"), 'r') as jsonFile: # locate json file
        loadedJson = json.load(jsonFile) # load it (Type: list)
        jsonString = json.dumps(loadedJson) # convert it to string
        return jsonString

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="127.0.0.1", port=8080) # runnng on http://127.0.0.1:8080/NAME
