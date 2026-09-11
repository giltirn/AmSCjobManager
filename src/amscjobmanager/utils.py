from . import globals
import pathlib

#Default user comms methods
def cmdlinePrint(*args, **kwargs):
    print(*args, *kwargs)
def cmdlineInput(query):
    return input(query + " : ").strip()


input_func = cmdlineInput

def userInput(query):
    """Make a query to the user and return their response"""
    global input_func
    return input_func(query)

def queryYesNo(query: str, body="")->bool:
    result = ""
    while(result not in ["y","n"]):
        if result == "":
            result = userInput(query + " [y/n]" + body)
        else:        
            result = userInput(f"You must answer with either 'y' or 'n'. {query}{body}")            
    return True if result == "y" else False

    
def checkSafePath(machine: str, path: str):
    machine = machine.lower()
    if globals.remote_workdir == None:
        raise Exception("setupWorkflowAgent has not been called")
    if machine not in globals.remote_workdir:
        raise Exception("Unknown machine")
    safe_p = pathlib.Path(globals.remote_workdir[machine]).resolve()
    path_p = pathlib.Path(path).resolve()
    tmp_p = pathlib.Path("/tmp")
    return path_p.is_relative_to(safe_p) or path_p.is_relative_to(tmp_p)
