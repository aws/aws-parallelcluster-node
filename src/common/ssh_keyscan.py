# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
# See the License for the specific language governing permissions and limitations under the License.
import base64
import logging
import os
import socket
from math import ceil
from multiprocessing import Pool

from common.utils import run_command
from paramiko import HostKeys, RSAKey, Transport


def _get_server_keys(hostname):

    server_keys = []

    # key_type_list = ["ssh-ed25519", "ssh-rsa", "ecdsa-sha2-nistp256"] # default key_type used by ssh-keysca
    # Supported key_type for OS
    # alinux     ssh-rsa,ssh-ed25519,ecdsa-sha2-nistp256
    # ubuntu1404 ssh-rsa,ssh-ed25519,ecdsa-sha2-nistp256
    # ubuntu1604 ssh-rsa,ssh-ed25519,ecdsa-sha2-nistp256
    # centos7    ssh-rsa,ssh-ed25519,ecdsa-sha2-nistp256
    # centos6    ssh-rsa
    key_type_list = ["ssh-rsa"]

    for key_type in key_type_list:
        transport = None
        try:
            sock = socket.socket()
            sock.settimeout(5)
            sock.connect((hostname, 22))
            transport = Transport(sock)
            transport._preferred_keys = [key_type]
            transport.start_client()
            server_keys.append(transport.get_remote_server_key())
        except Exception:
            pass
        finally:
            if transport:
                transport.close()

    if not server_keys:
        logging.error("Failed retrieving server key from host '%s'", hostname)

    return hostname, [(server_key.get_base64(), server_key.get_name()) for server_key in server_keys]


def _get_server_key_on_multiple_hosts(hostnames, parallelism=25, timeout=7):
    if not hostnames:
        return {}

    pool = Pool(parallelism)
    try:
        r = pool.map_async(_get_server_keys, hostnames)
        # The pool timeout is computed by adding 2 times the command timeout for each batch of hosts that is
        # processed in sequence. Where the size of a batch is given by the degree of parallelism.
        results = r.get(timeout=int(ceil(len(hostnames) / float(parallelism)) * (2 * timeout)))
        return dict(results)
    except Exception as e:
        logging.error("Failed when retrieving keys from hosts %s with exception %s", ",".join(hostnames), e)
        return dict()
    finally:
        pool.terminate()


def _add_keys_to_known_hosts(server_keys, host_keys_file):
    try:
        if not os.path.isfile(host_keys_file):
            host_keys = HostKeys()
        else:
            host_keys = HostKeys(filename=host_keys_file)

        for hostname, key_list in server_keys.items():
            try:
                for key_tuple in key_list:
                    key = RSAKey(data=base64.b64decode(key_tuple[0]))
                    host_keys.add(hostname=hostname, key=key, keytype=key_tuple[1])
                    host_keys.add(hostname=hostname + ".*", key=key, keytype=key_tuple[1])
                    host_keys.add(hostname=socket.gethostbyname(hostname), key=key, keytype=key_tuple[1])
                    logging.info(
                        "Adding keys to known hosts file '{0}' for host '{1}'".format(host_keys_file, hostname)
                    )
                    host_keys.save(filename=host_keys_file)
            except Exception as e:
                logging.error(
                    "Failed adding keys to known hosts file for host '{0}', with exception: {1}".format(hostname, e)
                )
    except Exception as e:
        logging.error("Failed adding keys to known hosts file '{0}', with exception: {1}".format(host_keys_file, e))


def _remove_keys_from_known_hosts(hostnames, host_keys_file, user):
    for hostname in hostnames:
        command = "ssh-keygen -R " + hostname + " -f " + host_keys_file
        run_command(command, raise_on_error=False, execute_as_user=user)
        command = "ssh-keygen -R " + hostname + ". -f " + host_keys_file
        run_command(command, raise_on_error=False, execute_as_user=user)
        command = "ssh-keygen -R " + socket.gethostbyname(hostname) + " -f " + host_keys_file
        run_command(command, raise_on_error=False, execute_as_user=user)


def update_ssh_known_hosts(events, user):
    host_keys_file = os.path.expanduser("~" + user) + "/.ssh/known_hosts"
    _remove_keys_from_known_hosts(
        [event.host.hostname for event in events if event.action == "REMOVE"], host_keys_file, user
    )
    _add_keys_to_known_hosts(
        _get_server_key_on_multiple_hosts([event.host.hostname for event in events if event.action == "ADD"]),
        host_keys_file,
    )
