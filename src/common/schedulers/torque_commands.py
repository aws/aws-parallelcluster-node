# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re
import subprocess
from xml.etree import ElementTree  # nosec nosemgrep

from common.schedulers.converters import ComparableObject, from_xml_to_obj
from common.utils import check_command_output, run_command

TORQUE_NODE_ERROR_STATES = ("down", "offline", "unknown")
TORQUE_NODE_STATES = (
    "free",
    "offline",
    "down",
    "reserve",
    "job-exclusive",
    "job-sharing",
    "busy",
    "time-shared",
    "state-unknown",
)
TORQUE_PENDING_JOB_STATE = "Q"
TORQUE_SUSPENDED_JOB_STATE = "S"
TORQUE_RUNNING_JOB_STATE = "R"

TORQUE_BIN_DIR = "/opt/torque/bin/"


def _qmgr_manage_nodes(operation, hosts, error_messages_to_ignore, additional_qmgr_args=""):
    if not hosts:
        return set()

    hostnames = ",".join(hosts)
    command = TORQUE_BIN_DIR + 'qmgr -c "{operation} node {hostnames} {additional_args}"'.format(
        operation=operation, hostnames=hostnames, additional_args=additional_qmgr_args
    )
    try:
        output = check_command_output(command, log_error=False)
    except subprocess.CalledProcessError as e:
        if not hasattr(e, "output") or not e.output or e.output == "":
            logging.error("Failed when executing operation %s on nodes %s with error %s", operation, hostnames, e)
            return set()
        else:
            output = e.output
    except Exception as e:
        logging.error("Failed when executing operation %s on nodes %s with error %s", operation, hostnames, e)
        return set()

    return _qmgr_process_command_output(operation, hosts, error_messages_to_ignore, output)


def _qmgr_process_command_output(operation, hosts, error_messages_to_ignore, output):
    succeeded_hosts = set(hosts)
    # analyze command output to understand if failure can be ignored (e.g. already existing node)
    for error_message in output.splitlines():
        match = re.match(r"qmgr obj=(?P<host>.*) svr=default: (?P<error>.*)", error_message)
        if not match:
            # assume unexpected error and mark all as failed
            logging.error(
                "Failed when executing operation %s on nodes %s with error %s", operation, ",".join(hosts), output
            )
            return set()

        host, error = match.groups()
        if any(error.strip() == message_to_ignore for message_to_ignore in error_messages_to_ignore):
            logging.warning(
                "Marking %s operation on node %s as succeeded because of ignored error message %s",
                operation,
                host,
                error,
            )
            continue

        try:
            logging.error("Failed when executing operation %s on node %s with error %s", operation, host, error_message)
            succeeded_hosts.remove(host)
        except Exception as e:
            logging.error(
                "Failed to extract host from error message while adding nodes. Mark all as failed. Output was %s.\n"
                "Exception was %s",
                output,
                e,
            )

    return succeeded_hosts


def add_nodes(hosts, slots):
    return _qmgr_manage_nodes(
        operation="create",
        hosts=hosts,
        error_messages_to_ignore=["Node name already exists"],
        additional_qmgr_args="np={0}".format(slots),
    )


def delete_nodes(hosts):
    # Setting nodes to offline before deleting to workaround issue with pbs_mom unable to
    # rerun the job.
    if hosts:
        run_command(TORQUE_BIN_DIR + "pbsnodes -o {0}".format(" ".join(hosts)), raise_on_error=False, log_error=False)
    # Process at most 20 concurrent deletions at a time since the required time linearly depends
    # on the number of nodes that we try to remove
    succeeded_hosts = set()
    chunk_size = 20
    for i in range(0, len(hosts), chunk_size):
        succeeded_hosts.update(
            _qmgr_manage_nodes(
                operation="delete",
                hosts=hosts[i : i + chunk_size],  # noqa E203: incompatible with black
                error_messages_to_ignore=[
                    "Unknown node",
                    "The server was unable to communicate with the MOM to requeue or delete the job."
                    " The node has been deleted and all jobs on the node have been purged.",
                ],
            )
        )

    return succeeded_hosts


def update_cluster_limits(max_nodes, node_slots):
    try:
        logging.info("Updating cluster limits: max_nodes=%d, node_slots=%d", max_nodes, node_slots)
        # resources_available.nodect enforces the max number of usable slots. Setting this to max_queue_size causes
        # torque not to use all slots available to the cluster and the scheduler erroneously limits the number of
        # concurrent running jobs so that the total number of required nodes is equal to max_queue_size. This is not
        # correct because a node can be shared across multiple jobs if free slots are available.
        run_command(
            TORQUE_BIN_DIR + 'qmgr -c "set queue batch resources_available.nodect={0}"'.format(max_nodes * node_slots)
        )
        run_command(
            TORQUE_BIN_DIR + 'qmgr -c "set server resources_available.nodect={0}"'.format(max_nodes * node_slots)
        )
        # resources_max.nodect enforces the max size of the cluster
        run_command(TORQUE_BIN_DIR + 'qmgr -c "set queue batch resources_max.nodect={0}"'.format(max_nodes))
        run_command(TORQUE_BIN_DIR + 'qmgr -c "set server resources_max.nodect={0}"'.format(max_nodes))
        run_command(TORQUE_BIN_DIR + 'qmgr -c "set queue batch resources_max.ncpus={0}"'.format(node_slots))
        _update_head_node_np(max_nodes, node_slots)
    except Exception as e:
        logging.error("Failed when updating cluster limits with exception %s.", e)


def _update_head_node_np(max_nodes, node_slots):
    """Head node np is dynamically based on the number of compute nodes that join the cluster."""
    current_nodes_count = len(check_command_output("cat /var/spool/torque/server_priv/nodes").strip().splitlines()) - 1
    # If cluster is at max size set the head node np to 1 since 0 is not allowed.
    head_node_np = max(1, (max_nodes - current_nodes_count) * node_slots)
    head_node_hostname = check_command_output("hostname")
    logging.info("Setting head node np to: %d", head_node_np)
    run_command(
        TORQUE_BIN_DIR
        + 'qmgr -c "set node {hostname} np = {slots}"'.format(hostname=head_node_hostname, slots=head_node_np)
    )


def get_compute_nodes_info(hostname_filter=None):
    command = TORQUE_BIN_DIR + "pbsnodes -x"
    if hostname_filter:
        command += " {0}".format(" ".join(hostname_filter))

    output = check_command_output(command, raise_on_error=False)
    if output.startswith("<Data>"):
        root = ElementTree.fromstring(output)  # nosec
        nodes = root.findall("./Node")
        nodes_list = [TorqueHost.from_xml(ElementTree.tostring(node)) for node in nodes]
        return dict((node.name, node) for node in nodes_list if node.note != "MasterServer")
    else:
        if output != "":
            logging.warning("Failed when running command %s with error %s", command, output)
        return dict()


def wakeup_scheduler():
    # Trigger a scheduling cycle. This is necessary when compute nodes are added to speed up jobs allocation.
    # This is also necessary when the first compute node gets added to the scheduler otherwise the jobs are never
    # started.
    logging.info("Triggering a scheduling cycle.")
    run_command(TORQUE_BIN_DIR + 'qmgr -c "set server scheduling=true"', raise_on_error=False)


def get_jobs_info(filter_by_states=None, filter_by_exec_hosts=None):
    command = TORQUE_BIN_DIR + "qstat -t -x"
    output = check_command_output(command)
    if not output:
        return []

    root = ElementTree.fromstring(output)  # nosec
    jobs = root.findall("./Job")
    jobs_list = []
    for job in jobs:
        parsed_job = TorqueJob.from_xml(ElementTree.tostring(job))
        if filter_by_states and parsed_job.state not in filter_by_states:
            continue
        if filter_by_exec_hosts:
            if any(host in parsed_job.exec_hosts for host in filter_by_exec_hosts):
                jobs_list.append(parsed_job)
        else:
            jobs_list.append(parsed_job)

    return jobs_list


def get_pending_jobs_info(max_slots_filter=None, log_pending_jobs=True):
    """
    Retrieve the list of pending jobs from the Slurm scheduler.

    :param max_slots_filter: discard jobs that require a number of slots bigger than the given value
    :param log_pending_jobs: log the actual list of pending jobs (rather than just a count)
    """
    jobs = get_jobs_info(filter_by_states=[TORQUE_PENDING_JOB_STATE])
    logging.info("Retrieved {0} pending jobs".format(len(jobs)))
    if log_pending_jobs:
        logging.info("The pending jobs are: {0}".format(jobs))
    pending_jobs = []
    for job in jobs:
        # filtering of ncpus option is already done by the scheduler at job submission time
        # see update_cluster_limits function
        if (
            max_slots_filter
            and job.resources_list.nodes_resources
            and any(ppn > max_slots_filter for _, ppn in job.resources_list.nodes_resources)
        ):
            logging.info(
                "Skipping job %s since required slots (%s) exceed max slots (%d)",
                job.id,
                job.resources_list.nodes_resources,
                max_slots_filter,
            )
        else:
            pending_jobs.append(job)

    return pending_jobs


class TorqueHost(ComparableObject):
    # <Node>
    #     <name>ip-10-0-1-242</name>
    #     <state>free</state>
    #     <power_state>Running</power_state>
    #     <np>4</np>
    #     <ntype>cluster</ntype>
    #     <note>MasterServer</note>
    #     <status>opsys=linux,uname=Linux ip-10-0-1-242 4.4.111-1.el6.elrepo.x86_64...</status>
    #     <mom_service_port>15002</mom_service_port>
    #     <mom_manager_port>15003</mom_manager_port>
    # </Node>
    MAPPINGS = {
        "name": {"field": "name"},
        "np": {"field": "slots", "transformation": int},
        "state": {"field": "state", "transformation": lambda states: states.split(",")},
        "jobs": {"field": "jobs"},
        "note": {"field": "note"},
    }

    def __init__(self, name=None, slots=0, state="", jobs=None, note=""):
        self.name = name
        self.slots = slots
        self.state = state
        self.jobs = jobs
        self.note = note

    @staticmethod
    def from_xml(xml):
        return from_xml_to_obj(xml, TorqueHost)


class TorqueJob(ComparableObject):
    # <Job>
    #     <Job_Id>149.ip-10-0-0-196.eu-west-1.compute.internal</Job_Id>
    #     <Job_Name>STDIN</Job_Name>
    #     <Job_Owner>centos@ip-10-0-0-196.eu-west-1.compute.internal</Job_Owner>
    #     <job_state>R</job_state>
    #     <queue>batch</queue>
    #     <server>ip-10-0-0-196.eu-west-1.compute.internal</server>
    #     <Checkpoint>u</Checkpoint>
    #     <ctime>1562156185</ctime>
    #     <Error_Path>ip-10-0-0-196.eu-west-1.compute.internal:/home/centos/STDIN.e149</Error_Path>
    #     <exec_host>ip-10-0-1-90/0-1+ip-10-0-1-104/0-1</exec_host>
    #     <Hold_Types>n</Hold_Types>
    #     <Join_Path>n</Join_Path>
    #     <Keep_Files>n</Keep_Files>
    #     <Mail_Points>a</Mail_Points>
    #     <mtime>1562156185</mtime>
    #     <Output_Path>ip-10-0-0-196.eu-west-1.compute.internal:/home/centos/STDIN.o149</Output_Path>
    #     <Priority>0</Priority>
    #     <qtime>1562156185</qtime>
    #     <Rerunable>True</Rerunable>
    #     <Resource_List>
    #         <nodes>1:ppn=2</nodes>
    #         <nodect>1</nodect>
    #         <walltime>01:00:00</walltime>
    #     </Resource_List>
    #     <session_id>21782</session_id>
    #     <Variable_List>PBS_O_QUEUE=batch,PBS_O_HOME=/home/centos,PBS_O_LOGNAME=centos,PBS_O_PATH=/usr/lib64/qt-3.3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/torque/bin:/opt/torque/sbin:/home/centos/bin,PBS_O_MAIL=/var/spool/mail/centos,PBS_O_SHELL=/bin/bash,PBS_O_LANG=en_US.UTF-8,PBS_O_WORKDIR=/home/centos,PBS_O_HOST=ip-10-0-0-196.eu-west-1.compute.internal,PBS_O_SERVER=ip-10-0-0-196</Variable_List>
    #     <euser>centos</euser>
    #     <egroup>centos</egroup>
    #     <queue_type>E</queue_type>
    #     <comment>Job started on Wed Jul 03 at 12:16</comment>
    #     <etime>1562156185</etime>
    #     <submit_args>-l nodes=1:ppn=2</submit_args>
    #     <start_time>1562156185</start_time>
    #     <Walltime>
    #         <Remaining>3580</Remaining>
    #     </Walltime>
    #     <start_count>1</start_count>
    #     <fault_tolerant>False</fault_tolerant>
    #     <job_radix>0</job_radix>
    #     <submit_host>ip-10-0-0-196.eu-west-1.compute.internal</submit_host>
    #     <init_work_dir>/home/centos</init_work_dir>
    #     <request_version>1</request_version>
    # </Job>
    MAPPINGS = {
        "Job_Id": {"field": "id"},
        "job_state": {"field": "state"},
        "Resource_List": {
            "field": "resources_list",
            "transformation": lambda res: TorqueResourceList.from_xml(ElementTree.tostring(res)),
            "xml_elem_type": "xml",
        },
        "exec_host": {
            "field": "exec_hosts",
            "transformation": lambda hosts: {host.split("/")[0] for host in hosts.split("+")},
        },
    }

    def __init__(self, id=None, state=None, resources_list=None, exec_hosts=None):
        self.id = id
        self.state = state
        self.resources_list = resources_list
        self.exec_hosts = exec_hosts or set()

    @staticmethod
    def from_xml(xml):
        return from_xml_to_obj(xml, TorqueJob)


def _parse_node_resources(res):
    # format is: 1:ppn=2+2:ppn=3 or 1+2 or 1
    result = []
    for item in res.split("+"):
        nodes_ppn = item.split(":ppn=")
        number_of_nodes = int(nodes_ppn[0])
        ppn = int(nodes_ppn[1]) if len(nodes_ppn) > 1 else 1
        result.append((number_of_nodes, ppn))
    return result


class TorqueResourceList(ComparableObject):
    # <Resource_List>
    #     <nodes>2</nodes>
    #     <nodect>2</nodect>
    #     <walltime>01:00:00</walltime>
    # </Resource_List>
    MAPPINGS = {
        "nodes": {"field": "nodes_resources", "transformation": _parse_node_resources},
        "nodect": {"field": "nodes_count", "transformation": int},
        "ncpus": {"field": "ncpus", "transformation": int},
    }

    def __init__(self, nodes_resources=None, nodes_count=None, ncpus=None):
        self.nodes_resources = nodes_resources
        self.nodes_count = nodes_count
        self.ncpus = ncpus

    @staticmethod
    def from_xml(xml):
        return from_xml_to_obj(xml, TorqueResourceList)
