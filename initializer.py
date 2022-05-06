from OPCtranslator import initializer
from pathlib import Path
import sys

def main():
    # get all arguments from the third one (including)
    machinesToParse = sys.argv[1:]
    currentDirectory = Path(__file__).parent.absolute()
    
    
    existingMachines = ["InjectionMolding","DosingFurnace","RealTimeMS","HPDC","SprayHead","RetrofitCS","ProsysExServer"]
    # check if passed machines exist
    for machine in machinesToParse:
        if( not(machine in existingMachines)):
            # eg if passed machine name is wrong, we throw an error
            raise SystemExit("The machine \""+machine+"\" is not in the list of valid machines.\nThe only valid machines are: "+ ', '.join(existingMachines))

    # check if required directories exist, if not create them
    reqDirectories = ["semantics", "additionalFiles", "exports"]
    for singleDirectory in reqDirectories:
        dirPath = Path(currentDirectory / singleDirectory)
        if(not dirPath.is_dir()):
            dirPath.mkdir(parents=True, exist_ok=True)

    initializer(machinesToParse, currentDirectory) # pass inputDictionary to initializer  

main()
