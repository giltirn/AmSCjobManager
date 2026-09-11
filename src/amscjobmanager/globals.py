import os
remote_workdir = None ##Dictionary mapping machine to a sandbox directory
api_impl = os.getenv("AMSC_JOB_MANAGER_API_IMPL", "IRI") #Control which API implementation is used
