"""Public IRI API.

Authentication and lifecycle helpers remain in their owning modules rather
than being re-exported here. Setup supplies configuration; subsequent
operations retrieve the state they need internally.
"""

from .config import (
    IRI_IMPLEMENTATIONS, MACHINE_IMPLEMENTATIONS,
    getImplementationAPIVersion, registerImplementation,
)
from .state import (
    getConfiguredImplementations, getImplementationClient,
    getMachineImplementation, getPersonalStagingConfiguration,
    listSpecialGlobusEndpoints, replaceSpecialGlobusEndpoint, setupWorkflowAgent,
    setupWorkflowAgentFromConfig,
)
from .discovery import (
    discoverImplementation, loadCachedImplementation, loadCachedImplementations,
)
from .rest import delete, get, post, put, submitTask, waitTask
from .resources import (
    getKnownMachines, getMachineQueues, getProjectAllocation, getResource,
    getResourceID, getResources, getUserAccountProjects, getUserProjectIDmap,
    queryMachineStatus,
)
from .filesystem import (
    downloadFile, downloadFileContents, getMountedFilesystems,
    getStorageResourcePaths, pathStat, pathType, remoteChmod, remoteLs,
    remoteMkdir, uploadBytes,
)
from .globus import (
    createGlobusStagingDirectory, downloadFileGlobus, findGlobusTransferByLabel,
    findGlobusTransfersByLabel, getRemoteGlobusLocation, globusCopy,
    globusTransferStatus, uploadFileGlobus,
)
from .compute import (
    cancelJob, executeBatchJobCompat, executeJob, findJobByName, getJobState,
)

__all__ = [
    "IRI_IMPLEMENTATIONS", "MACHINE_IMPLEMENTATIONS",
    "getImplementationAPIVersion", "registerImplementation",
    "setupWorkflowAgent", "setupWorkflowAgentFromConfig", "getConfiguredImplementations",
    "getImplementationClient", "getMachineImplementation",
    "getPersonalStagingConfiguration", "listSpecialGlobusEndpoints",
    "replaceSpecialGlobusEndpoint", "discoverImplementation",
    "loadCachedImplementation", "loadCachedImplementations",
    "get", "put", "post", "delete", "submitTask", "waitTask",
    "getKnownMachines", "getMachineQueues", "getProjectAllocation",
    "getResource", "getResourceID", "getResources", "getUserAccountProjects",
    "getUserProjectIDmap", "queryMachineStatus", "downloadFile",
    "downloadFileContents", "getMountedFilesystems", "getStorageResourcePaths",
    "pathStat", "pathType", "remoteChmod", "remoteLs", "remoteMkdir",
    "uploadBytes", "createGlobusStagingDirectory", "downloadFileGlobus",
    "findGlobusTransferByLabel", "findGlobusTransfersByLabel",
    "getRemoteGlobusLocation", "globusCopy", "globusTransferStatus",
    "uploadFileGlobus", "cancelJob", "executeBatchJobCompat", "executeJob",
    "findJobByName", "getJobState",
]
