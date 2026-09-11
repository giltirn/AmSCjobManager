##Default IO functions for cmdline tool
from .utils import cmdlinePrint

#Control over the function used for workflow manager logging
wfman_log_func = cmdlinePrint

def wfmanLog(*args, **kwargs):
    """
    Function employed by the workflow manager to output log information
    Default to cmdline but can be overridden
    """
    wfman_log_func(*args, *kwargs)

#Control over the function used for workflow API logging
api_log_func = cmdlinePrint

def wfapiLog(*args, **kwargs):
    """
    Function employed by the workflow api to output log information
    Default to cmdline but can be overridden
    """
    api_log_func(*args, *kwargs)


def defaultAPIuserQueryFunc(title, query):
    if len(title):
        print(title)
    return input(query).strip()
            
api_query_user_func = defaultAPIuserQueryFunc

def wfapiUserQuery(title, query):
    """
    Ask a query to the user, e.g. for obtaining login credentials
    """
    return api_query_user_func(title,query)
