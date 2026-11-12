##Default IO functions for cmdline tool
from .utils import cmdlinePrint
from .workers import checkForThreadCurrentWorkerInterruption

#Control over the function used for workflow manager logging
wfman_log_func = cmdlinePrint

def wfmanLog(*args, **kwargs):
    """
    Function employed by the workflow manager to output log information
    Default to cmdline but can be overridden
    """
    wfman_log_func(*args, **kwargs)

#Control over the function used for workflow API logging
api_log_func = cmdlinePrint

def wfapiLog(*args, **kwargs):
    """
    Function employed by the workflow api to output log information
    Default to cmdline but can be overridden
    """
    api_log_func(*args, **kwargs)


def defaultAPIuserQueryFunc(title: str, query: str) -> str:
    """Print title (str), ask query (str) in the terminal and return a stripped answer.

    This is the default api_query_user_func hook for direct Python use. Server
    frontends replace that hook with their query queue's ask function.
    """
    if len(title):
        print(title)
    return input(query).strip()
            
api_query_user_func = defaultAPIuserQueryFunc


def wfapiUserQuery(title: str, query: str) -> str:
    """Ask through the configured hook and return its response string.

    title (str) describes the interaction; query (str) is the question or login
    URL/prompt. Always call api_query_user_func: its default implementation asks
    in the terminal; the server utility installs its queue's ask function instead.
    Authentication workers retain their execution state while this call waits;
    no completion callback or condition ID is passed through the query interface.
    """
    checkForThreadCurrentWorkerInterruption()
    response = api_query_user_func(title, query)
    checkForThreadCurrentWorkerInterruption()
    return response
