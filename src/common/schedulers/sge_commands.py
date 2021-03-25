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
import collections
import logging
import re
from xml.etree import ElementTree  # nosec nosemgrep

from common import sge
from common.remote_command_executor import RemoteCommandExecutor
from common.schedulers.converters import ComparableObject, from_xml_to_obj
from common.sge import check_sge_command_output, run_sge_command

QConfCommand = collections.namedtuple("QConfCommand", ["command_flags", "successful_messages", "description"])

QCONF_COMMANDS = {
    "ADD_ADMINISTRATIVE_HOST": QConfCommand(
        command_flags="-ah",
        successful_messages=[r".* added to administrative host list", r'adminhost ".*" already exists'],
        description="add administrative hosts",
    ),
    "ADD_SUBMIT_HOST": QConfCommand(
        command_flags="-as",
        successful_messages=[r".* added to submit host list", r'submithost ".*" already exists'],
        description="add submit hosts",
    ),
    "REMOVE_ADMINISTRATIVE_HOST": QConfCommand(
        command_flags="-dh",
        successful_messages=[
            r".* removed .* from administrative host list",
            r'denied: administrative host ".*" does not exist',
        ],
        description="remove administrative hosts",
    ),
    "REMOVE_SUBMIT_HOST": QConfCommand(
        command_flags="-ds",
        successful_messages=[r".* removed .* from submit host list", r'denied: submit host ".*" does not exist'],
        description="remove submission hosts",
    ),
    "REMOVE_EXECUTION_HOST": QConfCommand(
        command_flags="-de",
        successful_messages=[r".* removed .* from execution host list", r'denied: execution host ".*" does not exist'],
        description="remove execution hosts",
    ),
}

# The state of the queue - one of u(nknown), a(larm), A(larm), C(alendar suspended), s(uspended),
# S(ubordinate), d(isabled), D(isabled), E(rror), c(configuration ambiguous), o(rphaned), P(reempted),
# or some combination thereof.
# Refer to qstat man page for additional details.

# u(nknown) is not considered as busy since the node will eventually be replaced by nodewatcher.
# Otherwise there might be overscaling issue when sge process is temporarily unresponsive
# when hitting bottleneck on network, etc in a large scale setting.
# o(rphaned) is not considered as busy since we assume a node in orphaned state is not present in ASG anymore
SGE_BUSY_STATES = ["C", "s", "D", "E", "P"]

# This state is set by nodewatcher when the node is locked and is being terminated.
SGE_DISABLED_STATE = "d"

# If an o(rphaned) state is displayed for a queue instance, it indicates that the queue instance is no longer demanded
# by the current cluster queue configuration or the host group configuration. The queue instance is kept because jobs
# which have not yet finished are still associated with it, and it will vanish from qstat output when these jobs
# have finished.
SGE_ORPHANED_STATE = "o"

# The states q(ueued)/w(aiting) and h(old) only appear for pending jobs. Pending, unheld job`s are displayed as qw.
# The h(old) state indicates that a job currently is not eligible for execution due to a hold state assigned to it
# via qhold(1), qalter(1) or the qsub(1) -h option, or that the job is waiting for completion of the jobs for which job
# dependencies have been assigned to it job via the -hold_jid or -hold_jid_ad options of qsub(1) or qalter(1).
SGE_HOLD_STATE = "h"

# If the state is u, the corresponding sge_execd(8) cannot be contacted.
# An E(rror) state is displayed for a queue for various reasons such as failing to find executables or directories.
# If an o(rphaned) state is displayed for a queue instance, it indicates that the queue instance is no longer demanded
# by the current cluster queue configuration or the host group configuration. The queue instance is kept because jobs
# which have not yet finished are still associated with it, and it will vanish from qstat output when these jobs have
# finished.
SGE_ERROR_STATES = ["u", "E", "o"]


def exec_qconf_command(hosts, qhost_command):
    if not hosts:
        return []

    hostnames = ",".join([host.hostname for host in hosts])
    try:
        logging.info("Executing operation '%s' for hosts %s", qhost_command.description, hostnames)
        command = "qconf {flags} {hostnames}".format(flags=qhost_command.command_flags, hostnames=hostnames)
        # setting raise_on_error to False and evaluating command output to decide if the execution was successful
        output = check_sge_command_output(command, raise_on_error=False)
        succeeded_hosts = []
        # assuming output contains a message line for each node the command is executed for.
        for host, message in zip(hosts, output.split("\n")):
            if any(re.match(pattern, message) is not None for pattern in qhost_command.successful_messages):
                succeeded_hosts.append(host)

        return succeeded_hosts
    except Exception as e:
        logging.error(
            "Unable to execute operation '%s' for hosts %s. Failed with exception %s",
            qhost_command.description,
            hostnames,
            e,
        )
        return []


def add_hosts_to_group(hosts):
    logging.info("Adding %s to @allhosts group", ",".join([host.hostname for host in hosts]))
    command = "qconf -aattr hostgroup hostlist {hostname} @allhosts"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def add_host_slots(hosts):
    logging.info("Adding %s to all.q queue", ",".join([host.hostname for host in hosts]))
    command = 'qconf -aattr queue slots ["{hostname}={slots}"] all.q'
    return _run_sge_command_for_multiple_hosts(hosts, command)


def remove_hosts_from_group(hosts):
    logging.info("Removing %s from @allhosts group", ",".join([host.hostname for host in hosts]))
    command = "qconf -dattr hostgroup hostlist {hostname} @allhosts"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def remove_hosts_from_queue(hosts):
    logging.info("Removing %s from all.q queue", ",".join([host.hostname for host in hosts]))
    command = "qconf -purge queue '*' all.q@{hostname}"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def install_sge_on_compute_nodes(hosts, cluster_user):
    """Start sge on compute nodes in parallel."""
    command = (
        "sudo sh -c 'ps aux | grep [s]ge_execd || "
        "(cd {0} && {0}/inst_sge -noremote -x -auto /opt/parallelcluster/templates/sge/sge_inst.conf)'"
    ).format(sge.SGE_ROOT)
    hostnames = [host.hostname for host in hosts]
    result = RemoteCommandExecutor.run_remote_command_on_multiple_hosts(command, hostnames, cluster_user, timeout=20)

    succeeded_hosts = []
    for host in hosts:
        if host.hostname in result and result[host.hostname]:
            succeeded_hosts.append(host)

    return succeeded_hosts


def lock_host(hostname):
    logging.info("Locking host %s", hostname)
    command = ["qmod", "-d", "all.q@{0}".format(hostname)]
    run_sge_command(command)


def unlock_host(hostname):
    logging.info("Unlocking host %s", hostname)
    command = ["qmod", "-e", "all.q@{0}".format(hostname)]
    run_sge_command(command)


def _run_sge_command_for_multiple_hosts(hosts, command_template):
    """Sequentially run an sge command on the head node for the given hostnames."""
    succeeded_hosts = []
    for host in hosts:
        command = command_template.format(hostname=host.hostname, slots=host.slots)
        try:
            run_sge_command(command.format(hostname=host.hostname))
            succeeded_hosts.append(host)
        except Exception as e:
            logging.error("Failed when executing command %s with exception %s", command, e)
    return succeeded_hosts


def _run_qstat(full_format=False, hostname_filter=None, job_state_filter=None):
    command = "qstat -xml -g dt -u '*'"
    if full_format:
        command += " -f"
    if hostname_filter:
        short_name = hostname_filter.split(".")[0]
        command += " -l hostname={0}".format(short_name)
    if job_state_filter:
        command += " -s {0}".format(job_state_filter)
    return check_sge_command_output(command)


def get_compute_nodes_info(hostname_filter=None, job_state_filter=None):
    output = _run_qstat(full_format=True, hostname_filter=hostname_filter, job_state_filter=job_state_filter)
    if not output:
        return {}

    root = ElementTree.fromstring(output)
    queue_info = root.findall("./queue_info/*")
    hosts_list = [SgeHost.from_xml(ElementTree.tostring(host)) for host in queue_info]
    return dict((host.name, host) for host in hosts_list)


def get_jobs_info(hostname_filter=None, job_state_filter=None):
    output = _run_qstat(full_format=False, hostname_filter=hostname_filter, job_state_filter=job_state_filter)
    if not output:
        return []

    root = ElementTree.fromstring(output)  # nosec
    job_info = root.findall(".//job_list")
    return [SgeJob.from_xml(ElementTree.tostring(host)) for host in job_info]


def get_pending_jobs_info(max_slots_filter=None, skip_if_state=None, log_pending_jobs=True):
    """
    Retrieve the list of pending jobs.

    :param max_slots_filter: discard jobs that require a number of slots bigger than the given value
    :param skip_if_state: discard jobs that are in the given state
    :param log_pending_jobs: log the actual list of pending jobs (rather than just a count)
    :return: the list of filtered pending jos.
    """
    pending_jobs = get_jobs_info(job_state_filter="p")
    logging.info("Retrieved {0} pending jobs".format(len(pending_jobs)))
    if log_pending_jobs:
        logging.info("The pending jobs are: {0}".format(pending_jobs))
    if max_slots_filter or skip_if_state:
        filtered_jobs = []
        for job in pending_jobs:
            if max_slots_filter and job.slots > max_slots_filter:
                logging.info(
                    "Skipping job %s since required slots (%d) exceed max slots (%d)",
                    job.number,
                    job.slots,
                    max_slots_filter,
                )
            elif skip_if_state and skip_if_state in job.state:
                logging.info("Skipping job %s since in state %s", job.number, job.state)
            else:
                filtered_jobs.append(job)

        return filtered_jobs
    else:
        return pending_jobs


class SgeJob(ComparableObject):
    # <job_list state="running">
    #     <JB_job_number>89</JB_job_number>
    #     <JAT_prio>0.60500</JAT_prio>
    #     <JB_name>STDIN</JB_name>
    #     <JB_owner>centos</JB_owner>
    #     <state>sr</state>
    #     <JAT_start_time>2019-05-15T13:16:51</JAT_start_time>
    #     <master>SLAVE</master>
    #     <slots>1</slots>
    # </job_list>
    MAPPINGS = {
        "JB_job_number": {"field": "number"},
        "slots": {"field": "slots", "transformation": int},
        "state": {"field": "state"},
        "master": {"field": "node_type"},
        "tasks": {"field": "array_index", "transformation": lambda x: int(x) if x is not None else None},
        "queue_name": {"field": "hostname", "transformation": lambda name: name.split("@", 1)[1] if name else None},
    }

    def __init__(self, number=None, slots=0, state="", node_type=None, array_index=None, hostname=None):
        self.number = number
        self.slots = slots
        self.state = state
        self.node_type = node_type
        self.array_index = array_index
        self.hostname = hostname

    @staticmethod
    def from_xml(xml):
        return from_xml_to_obj(xml, SgeJob)


class SgeHost(ComparableObject):
    # <Queue-List>
    #     <name>all.q@ip-10-0-0-166.eu-west-1.compute.internal</name>
    #     <qtype>BIP</qtype>
    #     <slots_used>2</slots_used>
    #     <slots_resv>0</slots_resv>
    #     <slots_total>4</slots_total>
    #     <load_avg>0.01000</load_avg>
    #     <arch>lx-amd64</arch>
    #     <job_list state="running">
    #         <JB_job_number>89</JB_job_number>
    #         <JAT_prio>0.60500</JAT_prio>
    #         <JB_name>STDIN</JB_name>
    #         <JB_owner>centos</JB_owner>
    #         <state>r</state>
    #         <JAT_start_time>2019-05-15T13:16:51</JAT_start_time>
    #         <master>MASTER</master>
    #         <slots>1</slots>
    #     </job_list>
    #     <job_list state="running">
    #         <JB_job_number>95</JB_job_number>
    #         <JAT_prio>0.60500</JAT_prio>
    #         <JB_name>STDIN</JB_name>
    #         <JB_owner>centos</JB_owner>
    #         <state>s</state>
    #         <JAT_start_time>2019-05-15T13:16:51</JAT_start_time>
    #         <slots>1</slots>
    #     </job_list>
    # </Queue-List>
    MAPPINGS = {
        "name": {"field": "name", "transformation": lambda name: name.split("@", 1)[1] if name else None},
        "slots_used": {"field": "slots_used", "transformation": int},
        "slots_total": {"field": "slots_total", "transformation": int},
        "slots_resv": {"field": "slots_reserved", "transformation": int},
        "state": {"field": "state"},
        "job_list": {
            "field": "jobs",
            "transformation": lambda job: SgeJob.from_xml(ElementTree.tostring(job)),
            "xml_elem_type": "xml",
        },
    }

    def __init__(self, name=None, slots_total=0, slots_used=0, slots_reserved=0, state="", jobs=None):
        self.name = name
        self.slots_total = slots_total
        self.slots_used = slots_used
        self.slots_reserved = slots_reserved
        self.state = state
        self.jobs = jobs or []

    @staticmethod
    def from_xml(xml):
        return from_xml_to_obj(xml, SgeHost)
