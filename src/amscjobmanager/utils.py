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

    

