from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT
import httpx
from .globals import checkSafePath, addSandboxDirs
import json
from typing import Literal, Union, List, Optional, Tuple
import time
import pathlib
import io
import os
import stat
from pathlib import Path
import globus_sdk
from globus_sdk.exc import GlobusAPIError
from globus_sdk.scopes import TransferScopes
from .utils import queryYesNo
from .logging import wfapiLog, wfapiUserQuery
from . import local_api

known_machines = {  "perlmutter" :
                    { "iriapi_base" : "https://api.iri.nersc.gov/api/v1",
                      "iriapi_group" : "perlmutter",
                      "globus_endpoint" : "6bdc7956-fc0f-4ad2-989c-7aa5ee643a79", 
                      "queues" : [ ("debug", "max time 0.5 hours, max nodes 8"), ("regular", "use for standard, production jobs or those too large for debug") ]
                     },
                     "amsc_transfer_server" :  #Universal server for AmSC data transfer API
                     {
                      "iriapi_transfer_base" : "https://amsc-data-api.nersc.gov"
                     }

                    }

#These endpoints require special data access permissions
#We also use this to bake in nicknames
#TODO: figure out how to deal with those that also require a special domain
#TODO: user config should store endpoints for which scopes are required, and token should be regenerated if list is changed (right now requires deletion)
special_globus_endpoints = { "dtn" :  "9d6d994a-6d04-11e5-ba46-22000b92c6ec", "perlmutter" : known_machines["perlmutter"]["globus_endpoint"],
                            "bnl": "12782fb1-a599-4f18-b0fb-2e849681e214"  }

def replaceSpecialGlobusEndpoint(endpoint : str):
    """Replace a special globus endpoint tag with the actual UUID"""
    endpoint = endpoint.lower()
    if endpoint in special_globus_endpoints:
        return special_globus_endpoints[endpoint]
    else:
        return endpoint


def listSpecialGlobusEndpoints():
    return special_globus_endpoints.keys()

    
tokens = { "iriapi_base" : None, "iriapi_transfer_base" : None }  #index tokens by their base path



#IRI main and transfer APIs currently have different servers and tokens

iri_api_client = httpx.Client()
               
def parse_scope_string(scope_string: str) -> set[str]:
    return set(scope_string.split()) if scope_string else set()

def ensure_private_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)

def load_tokens(token_file: Path) -> dict | None:
    if not token_file.exists():
        return None
    with token_file.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_tokens(token_file: Path, tokens: dict) -> None:
    ensure_private_parent_dir(token_file)
    tmp = token_file.with_suffix(".tmp")
    with os.fdopen(
        os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(tokens, f, indent=2)
    os.replace(tmp, token_file)
    os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)

def refresh_tokens(client: globus_sdk.NativeAppAuthClient, refresh_token: str, server: str) -> dict | None:
    try:
        token_response = client.oauth2_refresh_token(refresh_token)
        return token_response.by_resource_server[server]
    except GlobusAPIError as exc:
        wfapiLog(
            f"IRI token refresh failed ({exc.http_status}); switching to interactive login."
        )
        return None
    except Exception as exc:
        wfapiLog(
            f"IRI token refresh failed other error (Exception: {exc}, Server response: {token_response}, Resource server: {server}); switching to interactive login."
        )
        return None

############################################################
####IRI API SETUP
#############################################################

GLOBUS_CLIENT_ID = "eb18f0bb-4c76-43b5-88f1-750782be30ad" #Femtomeas, ckelly@bnl.gov

IRI_RESOURCE_SERVER="ed3e577d-f7f3-4639-b96e-ff5a8445d699"
RESOURCE_SERVER = "auth.globus.org"
REQUIRED_SCOPES = {
    f"https://auth.globus.org/scopes/{IRI_RESOURCE_SERVER}/iri_api"
}       

def interactive_login(client: globus_sdk.NativeAppAuthClient) -> dict:
    client.oauth2_start_flow(
        requested_scopes=" ".join(sorted(REQUIRED_SCOPES)),
        refresh_tokens=True,
    )
    query = f"""Open this URL, login, and consent:
    { client.oauth2_get_authorize_url(query_params={"prompt": "login"}) }

    Enter authorization code: """
    
    accept = False
    while(not accept):
        try:
            code = wfapiUserQuery("IRI API login", query)
            token_response = client.oauth2_exchange_code_for_tokens(code)
            accept = True
        except Exception as e:
            continue

    assert IRI_RESOURCE_SERVER in token_response.by_resource_server.keys()
        
    return token_response.by_resource_server[IRI_RESOURCE_SERVER]

def setupIRIapiCompute(key_path):
    client =  globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)

    wfapiLog("setupIRIapi checking stored tokens at",key_path)
    stored = load_tokens(Path(key_path))
    auth_data = None
    if stored and stored.get("refresh_token"):
        auth_data = refresh_tokens(client, stored["refresh_token"], IRI_RESOURCE_SERVER)

    if auth_data == None:
        auth_data = interactive_login(client)

    granted = parse_scope_string(auth_data.get("scope", ""))
    missing = REQUIRED_SCOPES - granted
    if missing:
        raise RuntimeError(f"Missing required scopes: {sorted(missing)}")

    save_tokens(Path(key_path), auth_data)

    expires_at = auth_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nAccess token valid for ~{max(ttl, 0)} seconds.")

    wfapiLog(f"Saved token data to {key_path}")
    wfapiLog(f"Granted scopes: {auth_data.get('scope', '')}")

    tokens['iriapi_base'] = auth_data['access_token']
    

#############################
#### IRI TRANSFER API
#############################

IRI_TRANSFER_RESOURCE_SERVER = "08da012c-6998-46f9-9375-a6985ebe3f2b"
IRI_TRANSFER_DEFAULT_SCOPE = f"https://auth.globus.org/scopes/{IRI_TRANSFER_RESOURCE_SERVER}/transfer"

def interactive_login_transfer(client: globus_sdk.NativeAppAuthClient) -> dict:
    scope = globus_sdk.Scope(IRI_TRANSFER_DEFAULT_SCOPE)
    mapped_collections = [v for k,v in special_globus_endpoints.items()]
    
    data_access = [globus_sdk.scopes.GCSCollectionScopes(mc).data_access for mc in mapped_collections] #data access scope required for mapped collections
    transfer_scope = TransferScopes.all.with_dependencies(data_access)
    scope = scope.with_dependency(transfer_scope)
    print(f"Logging in with scope: {scope}")

    client.oauth2_start_flow(
        requested_scopes=scope,                 #{IRI_TRANSFER_RESOURCE_SERVER: scope},
        refresh_tokens=True,
    )
    query = f"""Open this URL, login, and consent:
    { client.oauth2_get_authorize_url(query_params={"prompt": "login"}) }

    Enter authorization code: """

    accept = False
    while(not accept):
        try:
            code = wfapiUserQuery("IRI Transfer API login", query)
            token_response = client.oauth2_exchange_code_for_tokens(code)
            accept = True
        except Exception as e:
            continue

    assert IRI_TRANSFER_RESOURCE_SERVER in token_response.by_resource_server.keys()
        
    return token_response.by_resource_server[IRI_TRANSFER_RESOURCE_SERVER]

def setupIRIapiTransfer(key_path):
    client =  globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)

    wfapiLog("setupIRIapiTransfer checking stored tokens at",key_path)
    stored = load_tokens(Path(key_path))
    auth_data = None
    if stored and stored.get("refresh_token"):
        auth_data = refresh_tokens(client, stored["refresh_token"], IRI_TRANSFER_RESOURCE_SERVER)

    if auth_data == None:
        auth_data = interactive_login_transfer(client)

    save_tokens(Path(key_path), auth_data)

    expires_at = auth_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nAccess token valid for ~{max(ttl, 0)} seconds.")

    wfapiLog(f"Saved token data to {key_path}")
    wfapiLog(f"Granted scopes: {auth_data.get('scope', '')}")

    tokens['iriapi_transfer_base'] = auth_data['access_token']

    
################################################
##### Public facing API
################################################
        
def setupWorkflowAgent(iriapi_key_path : str, iriapi_transfer_key_path : str, work_dir : dict):
    """
    Setup the workflow agent
    Args:
       iriapi_key_path: The full path to the IRI API key file. This will be generated automatically if it doesn't currently exist.
       iriapi_transfer_key_path: The full path to the IRI transfer API key file. This will be generated automatically if it doesn't currently exist.
       work_dir: The remote work directories, by machine as a dict, e.g. { "perlmutter" : "/path/to/dir" }. Use a list of directories if more than one. The API is only allowed to modify the contents of files within this directory or its children
    """    
    setupIRIapiCompute(iriapi_key_path)
    setupIRIapiTransfer(iriapi_transfer_key_path)
    addSandboxDirs(work_dir) #needs the API to be set up
        

def get(machine, suburl, params = None, base='iriapi_base'):
    machine = machine.lower()
    assert iri_api_client != None
    assert machine in known_machines
    assert base in known_machines[machine]
    assert base in tokens

    token = tokens[base]
    base_path = known_machines[machine][base]
    resp = iri_api_client.get(base_path + '/' + suburl, headers={ "accept" : "application/json", "Authorization" : f"Bearer {token}" }, params=params, timeout=300  )
    if resp.status_code == 200:
        j = json.loads(resp.text)
        return j
    else:
        raise Exception(f"Get operation failed with code {resp.status_code} and text {resp.text} (full response: {resp})")
        
iri_api_project_map = {}

def getUserProjectIDmap(machine):
    """
    Obtain the mapping between project name and id
    Return:
       dict name -> id
    """
    global iri_api_project_map
    machine = machine.lower()

    if machine not in iri_api_project_map:
        j = get(machine, "account/projects")
        pmap = dict()
        for acct in j:
            pmap[acct['name']] = acct['id']
        iri_api_project_map[machine] = pmap
    
    return iri_api_project_map[machine]

def getKnownMachines():
    return list(known_machines.keys()) + ["local"]

def getMachineQueues(machine)->List[ Tuple[str,str] ]:
    """
    Provide a list of queues and associated information for a given machine
    
    Return: a list of string tuples, with the first tuple entry being the queue name and the second relevant information about the queue
    """
    machine = machine.lower()
    if machine not in getKnownMachines():
        raise Exception(f"Invalid machine: {machine}")

    return known_machines[machine]["queues"]

def getUserAccountProjects(machine):
    machine = machine.lower()
    if machine not in getKnownMachines():
        raise Exception(f"Invalid machine: {machine}")
    
    out = list(getUserProjectIDmap(machine).keys())
    if machine == "perlmutter": #_g is required for GPU nodes
        for i in range(len(out)):
            out[i] += "_g"
    return out
    



iri_api_resource_map = {}

def getResourceID(machine, rtype="compute"):
    """
    Get the resource ID associated with the resource
    rtype: "compute" or "login"
    """
    machine = machine.lower()
    global iri_api_resource_map
    if rtype not in ["compute","login"]:
        raise Exception("Invalid resource type")
    
    if machine not in iri_api_resource_map:
        wfapiLog("Obtaining resource information for machine",machine)
        j = get(machine, "status/resources", params={"group" : known_machines[machine]['iriapi_group'], "resource_type" : "compute"})
        
        iri_api_resource_map[machine] = dict()
            
        #The API does not distinguish between login and compute nodes; both are "compute" resources, but the login node does not have any capabilities listed (unclear if this will change)
        #For now, use the capabilities to distinguish as the names are likely arbitrary
        compute = None
        login=None
        for r in j:
            cpu = False
            gpu =False
            for cap in r['capability_uris']:
                if 'capabilities/cpu' in cap:
                    cpu = True
                if 'capabilities/gpu' in cap:
                    gpu = True
            if cpu and gpu:
                wfapiLog(f"Identified resource {json.dumps(r,indent=2)} as compute backend")
                iri_api_resource_map[machine]["compute"] = r["id"]
            elif not cpu and not gpu:
                wfapiLog(f"Identified resource {json.dumps(r,indent=2)} as login frontend")
                iri_api_resource_map[machine]["login"] = r["id"]
            else:
                wfapiLog(f"Warning: unidentified resource {json.dumps(r,indent=2)}")
        
    
    return iri_api_resource_map[machine][rtype]


def queryMachineStatus(machine: str, rtype="compute")-> bool:
    """
    Query the status of a machine
    Args:
       machine - The name of the machine
       rtype - The resource type (compute/login)
    Return:
       A bool indicating whether the machine up (True) or down (False)
    """
    machine = machine.lower()
    if machine == "local":
        return True
    rid = getResourceID(machine, rtype)
    j = get(machine, f"status/resources/{rid}")
    wfapiLog(f"Query status of machine {machine} returned {j['current_status']}")
    return j['current_status'] == 'up'
    

def waitTask(machine, task_id, poll_freq=4):
    machine = machine.lower()
    j = get(machine, f"task/{task_id}")
    
    while(j['status'] == "active"):
        time.sleep(poll_freq)
        j = get(machine, f"task/{task_id}")

    if j['status'] != "completed":
        raise Exception(f"Task not completed, response {json.dumps(j,indent=2)}")

    return j

def remoteLs(machine: str, path: str)-> List[str]:
    """
    Query the contents of a path on a given machine
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       path - The absolute path
    Return:
       A list of files in the directory

    TODO: Explore behavior of trailing slashes, and the fact that the directory name itself seems to be listed among the directory content; should we unify the behavior with SFAPI?
    """
    machine = machine.lower()
    assert iri_api_client != None
    assert machine in known_machines

    wfapiLog(f"Listing contents of directory {machine}:{path}")
    
    rid = getResourceID(machine, rtype="login")

    j = get(machine, f"filesystem/ls/{rid}", params={"path" : path})
    tid = j['task_id']
    j = waitTask(machine, tid)

    files = [ f['name'] for f in j['result']['output'] ]
    return files


def put(machine, suburl, data = None, params=None):
    machine = machine.lower()
    assert iri_api_client != None
    assert machine in known_machines
    assert 'iriapi_base' in tokens
    
    token = tokens['iriapi_base']   
    base_path = known_machines[machine]['iriapi_base']
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {token}" }
    
    resp = iri_api_client.put(base_path + '/' + suburl, headers=headers, json=data, params=params, timeout=300)
    return json.loads(resp.text), resp.status_code



def remoteChmod(machine: str, path : str, mode : str, allow_unsafe = False):
    machine = machine.lower()
    wfapiLog(f"Changing permissions of file {machine}:{path} to {mode}")
    
    if not allow_unsafe and not checkSafePath(machine, path):
        raise Exception("Path is not a subdirectory of the sandbox path")

    if not pathlib.Path(path).is_absolute():
        raise Exception("Path must be absolute")

    rid = getResourceID(machine, rtype="login")
    j, status = put(machine, f"filesystem/chmod/{rid}", data={"path" : path, "mode" : mode})
    tid = j['task_id']
    j = waitTask(machine, tid)
    
    if j["status"] != "completed":        
        raise Exception(f"Permission change failed: {json.dumps(j,indent=2)}")        
    

def post(machine, suburl, data = None, params=None, files=None, base='iriapi_base', data_is_json=True):
    machine = machine.lower()
    assert iri_api_client != None
    assert machine in known_machines
    assert base in known_machines[machine]
    assert base in tokens

    token = tokens[base]      
    base_path = known_machines[machine][base]
    headers={ "accept" : "application/json", "Authorization" : f"Bearer {token}" }
    
    resp = iri_api_client.post(base_path + '/' + suburl, headers=headers, json=data if data_is_json else None, data=data if not data_is_json else None, params=params, files=files, timeout=300 )
    return json.loads(resp.text), resp.status_code
    
def remoteMkdir(machine: str, path: str, create_parents = True, allow_unsafe = False):
    """
    Create a directory on the remote machine. This is an unsafe action as it is not confined to the sandbox directory, and thus should not be exposed as a tool without safeguards
    Args:
           allow_unsafe - Allow uploading to directories other than within the sandbox
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.localMkdir(path, create_parents, allow_unsafe)


    wfapiLog(f"Creating directory {machine}:{path}")
    
    if not allow_unsafe and not checkSafePath(machine, path):
        raise Exception(f"Path {path} is not a subdirectory of the sandbox path")

    if not pathlib.Path(path).is_absolute():
        raise Exception("Path must be absolute")

    rid = getResourceID(machine, rtype="login")
        
    j, status = post(machine, f"filesystem/mkdir/{rid}", data={"path" : path, "parent" : create_parents})
    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] != "completed":
        raise Exception(f"Directory creation failed, status: {status},  response: { json.dumps(j,indent=2) }")        


def pathStat(machine: str, remote_path: str, dereference=True)->str:
    """
    Run stat on the provided path
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
    """
    machine = machine.lower()    

    if machine == "local":
        raise NotImplementedError()
       
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="login")

    j = get(machine, f"filesystem/stat/{rid}", params={'path' : remote_path, 'dereference' : dereference})
    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        


def pathType(machine: str, remote_path: str)->str:
    """
    Run 'file' on the provided path, identifying whether a file or directory
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
    """
    machine = machine.lower()    

    if machine == "local":
        return local_api.localPathType(remote_path)

    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="login")

    j = get(machine, f"filesystem/file/{rid}", params={'path' : remote_path})
    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        

    


def uploadBytes(machine: str, remote_path: str, content: io.BytesIO, allow_unsafe = False):
    """
    Upload file contents as bytes to a remote path (max 5242880 Bytes)
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path to the resulting file on the remote machine
       content - The file contents as binary
       allow_unsafe - Allow uploading to directories other than within the sandbox
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.writeBytes(remote_path, content, allow_unsafe)

    wfapiLog(f"Uploading binary data to {machine}:{remote_path}")
    
    if not allow_unsafe and not checkSafePath(machine, remote_path):
        raise Exception("Path is not below the privileged directory")
    
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
   
    rid = getResourceID(machine, rtype="login")

    j, status = post(machine, f"filesystem/upload/{rid}", params={'path' : remote_path}, files={'file': content})

    tid = j['task_id']
    j = waitTask(machine, tid)

    if j["status"] != "completed":
        raise Exception(f"Upload failed, status: {status},  response: {json.dumps(j,indent=2)}")        


def downloadFile(machine: str, remote_path: str)->str:
    """
    Download a (small) remote file. Returns the file contents as a string
    Args:
       machine - The name of the machine. Valid values are 'Perlmutter'
       remote_path - The absolute path on the remote machine
    """
    machine = machine.lower()

    wfapiLog(f"Downloading file {machine}:{remote_path}")
       
    if not pathlib.Path(remote_path).is_absolute():
        raise Exception("Path must be absolute")
    
    rid = getResourceID(machine, rtype="login")

    j = get(machine, f"filesystem/download/{rid}", params={'path' : remote_path})
    tid = j['task_id']
    j = waitTask(machine, tid)
    
    if j["status"] == "completed":
        return j["result"]["output"]
    else:
        raise Exception(f"Download failed, response: { json.dumps(j,indent=2)}")        


    
    
def executeBatchJobCompat(machine: str, script_body: str,
                    nodes : int, ranks_per_node : int, gpus_per_rank : int,
                    time : str, queue : str, account : str,
                    job_run_dir : str, exclusive=True, allow_unsafe=False) -> str:
    """
    Execute batch script on the machine
    script_body: The content of the batch script. If you are executing an existing remote script, use "source /path/to/script"    
    Note that any SLURM/PBS headers will be ignored; ensure that SLURM headers that are usually passed to srun are manually passed instead

    time: the job duration. Currently it seems to only accept integers, which my testing indicates is in *seconds*

    NOTE: This relies on hacking the API to bypass the automatic srun command in favor of the (more flexible) script contents. Hopefully they will allow arbitrary batch script submission as part of the API eventually!
    """
    machine = machine.lower()
    if machine == "local":
        return local_api.executeJobScript(script_body, job_run_dir, allow_unsafe)

    wfapiLog(f"Executing batch job on machine {machine} with nodes:{nodes}, ranks/node:{ranks_per_node}, gpus/rank:{gpus_per_rank}, time:{time}, queue:{queue}, account:{account}")
    
    if not allow_unsafe and not checkSafePath(machine, job_run_dir):
        raise Exception("Path is not below the privileged directory")

    resources = {
        "node_count" : nodes,
        "processes_per_node" : ranks_per_node,
        "exclusive_node_use": exclusive,
        "gpu_cores_per_process" : gpus_per_rank
        }       
        
    attributes = {
        "duration" : time,
        "account": account,
        "queue_name": queue
        }
    spec = {
        "executable": "date", 
        "directory" : job_run_dir,
        "inherit_environment": True,
        "stdin_path" : None,
        "stdout_path": f"{job_run_dir}/run.log",
        "stderr_path": f"{job_run_dir}/err.log",
        "resources" : resources,
        "attributes" : attributes,
        "launcher" : "srun",
        "pre_launch" : f"cd {job_run_dir}\n" + script_body,  #hijack the pre_launch to run the actual batch script content to avoid the jobspec restrictions
        "post_launch" : None
        }
    
    rid = getResourceID(machine, rtype="compute")

    j, status = post(machine, f"compute/job/{rid}", data=spec)
    if status == 200:
        return j['id']
    else:        
        raise Exception(f"Job submission failed, status: {status}, reason: { json.dumps(j,indent=2)}")
    
def getJobState(machine: str, jobid: str) -> str:
    machine = machine.lower()
    if machine == "local":
        return local_api.getJobState(jobid)

    rid = getResourceID(machine, rtype="compute")
    j = get(machine, f"compute/status/{rid}/{jobid}", params = { "historical" : True })
    wfapiLog(f"Queried job state {machine}:{jobid}, got {j['status']['state']}")
    return j['status']['state']
    
    
def delete(machine, suburl, params = None):
    machine = machine.lower()

    assert iri_api_client != None
    assert machine in known_machines
    assert 'iriapi_base' in tokens
    
    token = tokens['iriapi_base']         
    base_path = known_machines[machine]['iriapi_base']
    resp = iri_api_client.delete(base_path + '/' + suburl, headers={ "accept" : "*/*", "Authorization" : f"Bearer {token}" }, params=params, timeout=300  )
    return {} if resp.text == "" else resp.json(), resp.status_code

def cancelJob(machine: str, jobid: str):
    machine = machine.lower()

    wfapiLog(f"Canceling job {machine}:{jobid}")
    rid = getResourceID(machine, rtype="compute")
    j, status = delete(machine, f"compute/cancel/{rid}/{jobid}")
    if status != 204:
        raise Exception("Job cancellation failed:",json.dumps(j))


def globusTransferStatus(transfer_id)-> str:
    """
    Query the status of a Globus transfer with the provided transfer_id
    Returns the status from the following (cf. https://docs.globus.org/api/transfer/task/):
    "ACTIVE"  The task is in progress.
    "INACTIVE" The task has been suspended and will not continue without intervention. Currently, only credential expiration will cause this state.
    "SUCCEEDED"  The task completed successfully.
    "FAILED"  The task or one of its subtasks failed, expired, or was canceled.
    """
    j = get("amsc_transfer_server", f"movement/transfer/globus/{transfer_id}", base='iriapi_transfer_base')
    return j["status"]


def _globusCopy(source_endpoint, dest_endpoint, source_path, dest_path, block_until_complete=False):
    trans_args = { "source_uuid" : source_endpoint, "source_path": source_path,
                   "destination_uuid" : dest_endpoint, "destination_path" : dest_path,
                   "label" : "FemtoMeas transfer" }
    
    j, status=post("amsc_transfer_server", "movement/transfer/globus", data=trans_args, base='iriapi_transfer_base', data_is_json=True)
    if status == 200:
        tid = j["transfer_uuid"]

        if block_until_complete:
            while (status := globusTransferStatus(tid)) == "ACTIVE":
                print(".", end="")
                time.sleep(20)
            print(status)

        return tid
    else:
        raise Exception("Globus transfer failed, response content: " + json.dumps(j))

def _checkSafePathTagOrUUID(machine_or_uuid: str, path: str)->bool:
    #First check if machine_or_uuid is a machine in globals.remote_workdir
    if machine_or_uuid in globals.remote_workdir.keys():
        return checkSafePath(machine_or_uuid, path)

    #See if machine_or_uuid is a UUID corresponding to a named machine
    machine=None
    for m, uuid in special_globus_endpoints.items():
        if uuid == machine_or_uuid:
            machine = m
            break

    #If we have a remote_workdir assigned for this machine we can go ahead
    if machine is not None and machine in globals.remote_workdir.keys():
        return checkSafePath(machine, path)

    #Otherwise we have to ask the user
    query = f"Do you give permission to write to path {path} on UUID {machine_or_uuid}"
    if machine is not None:
        query += f" ({machine})"
    return queryYesNo(query)

def globusCopy(dest_uuid: str, dest_path: str,
               source_uuid: str, source_path: str,
               allow_unsafe=False,
               block_until_complete=False)-> str: 
    """
    Perform a Globus transfer between two endpoints
    Args:
       dest_uuid: The destination UUID (or a known machine name/tag)
       dest_path: The destination path on that machine (directory)
       source_uuid: The source UUID (or a known machine name/tag)
       source_path : The path on that endpoint
       allow_unsafe : Allow movement to paths outside of the sandbox or user permissions
       block_until_complete : Poll the transfer status every 20s until the transfer is complete before returning
    Return:
       The transfer ID as a string

    Notes:
       If source_path is a filename, only that file will be copied. If it is a directory name only the contents of that directory will be copied, not the directory itself (even if there is no trailing /)
    """
       
    wfapiLog(f"Initiating globus copy from {source_uuid}:{source_path} to {dest_uuid}:{dest_path}")    

    if not allow_unsafe and not _checkSafePathTagOrUUID(dest_uuid, dest_path):
        raise Exception(f"Attempting to copy data to a disallowed location on {dest_uuid}")

    #Transform tags into actual endpoints
    if dest_uuid in special_globus_endpoints:
        wfapiLog(f"Destination tag {dest_uuid} replaced by UUID {special_globus_endpoints[dest_uuid]}")
        dest_uuid = special_globus_endpoints[dest_uuid]
    if source_uuid in special_globus_endpoints:
        wfapiLog(f"Source tag {source_uuid} replaced by UUID {special_globus_endpoints[source_uuid]}")
        source_uuid = special_globus_endpoints[source_uuid]        

    return _globusCopy(source_endpoint=source_uuid, dest_endpoint=dest_uuid, source_path=source_path, dest_path=dest_path, block_until_complete=block_until_complete)
