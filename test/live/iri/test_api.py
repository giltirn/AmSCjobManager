import sys
from amscjobmanager.manager_config import readManagerConfigFile, setupManager
from amscjobmanager.manager_config_models import ManagerConfig
from amscjobmanager.iri_api import pathType
from amscjobmanager.api_general import downloadFile, uploadBytes, globusCopy, remoteMkdir, executeJob, getUserAccountProjects, getProjectAllocation, getUsername,remoteLs
import amscjobmanager.iri_api.filesystem as filesystem
from io import BytesIO
import base64

if len(sys.argv) != 2:
    template_path = ManagerConfig.write_template_json()
    raise SystemExit(
        "Usage: python3 test/live/iri/test_api.py <manager-config.json>\n"
        f"Configuration template available at {template_path}"
    )

config = readManagerConfigFile(sys.argv[1])
print(type(config))
setupManager(config)

implementation="esnet"
#implementation="nersc"
un = getUsername(implementation=implementation)
#un="ckelly"
machine="esnet_low_priority_compute"
#machine="perlmutter"
print(filesystem.getStorageResourcePaths(machine))
#print(remoteLs(machine, f"/data/home"))
print(remoteLs(machine, f"/data/home/{un}"))
#accounts = getUserAccountProjects("esnet_low_priority_compute")
#print("Accounts: ", accounts)

if 0:
    accounts = getUserAccountProjects("aurora")
    print("Accounts: ", accounts)

    for a in ["LatticeFlavor"]:
        print(f"Getting allocation for project: {a}")
        print(getProjectAllocation("aurora", a))
if 0:
    accounts = getUserAccountProjects("perlmutter")
    print("Accounts: ", accounts)
    
    for a in ["amsc013_g"]:
        print(f"Getting allocation for project: {a}")
        print(getProjectAllocation("perlmutter", a))

#remoteMkdir("aurora","/home/ckelly/agent_safe_dir/amsc-job-manager-iri-compute/311523b1732548f7887a90989cdf5c77/execute-job")


#globusCopy("dtn", "/global/cfs/cdirs/mp13/ckelly/agent_safe_dir/", "81cc3abd-c31a-11f1-a702-02ffe792127d", "/C/Users/gilti/Documents/AmSC_staging/test.txt", allow_unsafe=True, block_until_complete=True)
        
#remoteMkdir("aurora", "/lus/flare/projects/LatticeFlavor/ckelly/agent_safe_dir/test")        

#jid = executeJob("aurora", "/bin/bash", ["-lc", r'echo "HELLO"; echo $PALS_RANKID'], nodes=1, ranks_per_node=1, gpus_per_rank=1, time=300, queue="debug", account="LatticeFlavor::dwf", job_run_dir="/lus/flare/projects/LatticeFlavor/ckelly/agent_safe_dir")
#print(jid)

# script_body=r"""
# cat << 'EOF' > script.sh
# #!/bin/bash
# echo "Hello from rank $PALS_RANKID"
# sleep 10
# echo "Rank $PALS_RANKID is done"
# EOF

# chmod u+x script.sh

# mpiexec -np 8 ./script.sh
# """

# #executeJob("aurora", "/bin/bash", ["-lc", script_body], nodes=1, ranks_per_node=1, gpus_per_rank=1, time=300, queue="debug", account="LatticeFlavor::dwf", job_run_dir="/lus/flare/projects/LatticeFlavor/ckelly/agent_safe_dir")

# executable = "/bin/bash"
# job_run_dir="/lus/flare/projects/LatticeFlavor/ckelly/agent_safe_dir"
# encoded = base64.b64encode(script_body.encode()).decode()
# arguments = [
#     "-lc",
#     f"cd {job_run_dir}; echo {encoded} | base64 -d | /bin/bash",
# ]
# executeJob("aurora", executable, arguments, nodes=1, ranks_per_node=1, gpus_per_rank=1, time=300, queue="debug", account="LatticeFlavor::dwf", job_run_dir=job_run_dir)

#j = pathType("perlmutter", "/global/cfs/cdirs/mp13/ckelly/AmSC_lightshow/test_file.pooh")
#print(j)
#j = pathType("perlmutter", "/global/cfs/cdirs/mp13/ckelly/AmSC_lightshow")
#print(j)
#j = pathType("local", "/home/ckelly/src/AmSCjobManager")
#print(j)

#j = pathType("perlmutter", "/global/cfs/cdirs/mp13/ckelly/AmSC_lightshow/oidosjlkd")
#print(j)

#uploadBytes("perlmutter", "/global/homes/c/ckelly/agent_safe_dir/test_upload.txt", BytesIO(b"AmSC job manager IRI API smoke test\n"), allow_unsafe=True)



#downloadFile("perlmutter", ".", "/global/cfs/cdirs/mp13/ckelly/AmSC_lightshow/SCF.tar.gz")
#downloadFile("perlmutter", ".", "/global/cfs/cdirs/mp13/ckelly/AmSC_lightshow/test_file")

#downloadFile("perlmutter", "test_file", "/global/cfs/cdirs/mp13/ckelly/AmSC_lightshow/myfile.bin")

#downloadFile("local", "/home/ckelly/src/AmSCjobManager/pooh_out", "/home/ckelly/src/AmSCjobManager/pooh_in")
