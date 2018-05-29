#!/usr/bin/env python3

"""deploy-dispatchers.py

Usage:
    deploy-dispatchers.py [options] <num_dispatchers> <service_name_base> <output_file>

Arguments:
    num_dispatchers    number of dispatchers to deploy
    service_name_base  DC/OS service name base
    output_file        output file

Options:
    --cpus <n>                   number of CPUs to use per dispatcher [default: 1]
    --enable-kerberos <bool>     enable Kerberos configuration [default: False]
    --hdfs-config <url>          URL of the HDFS configuration files
    --history-service <url>      URL of the Spark history service
    --kdc-hostname <hostname>    the name or address of a host running a KDC
    --kdc-port <port>            the port of the host running a KDC [default: 88]
    --kerberos-realm <realm>     the Kerberos realm used to render the principal
    --log-level <level>          log level [default: INFO]
    --mem <n>                    amount of memory (mb) to use per dispatcher [default: 1024.0]
    --options-json <file>        a file containing installation options in JSON format
    --package-name <name>        name of the Spark package name [default: spark]
    --package-repo <url>         URL of the Spark package repo to install from
    --create-quotas <bool>       create drivers and executors quotas [default: True]
    --quota-drivers-cpus <n>     number of CPUs to use for drivers quota [default: 1]
    --quota-drivers-gpus <n>     number of GPUs to use for drivers quota [default: 0]
    --quota-drivers-mem <n>      amount of memory (mb) to use per drivers quota [default: 2048.0]
    --quota-executors-cpus <n>   number of CPUs to use for executors quota [default: 1]
    --quota-executors-gpus <n>   number of GPUs to use for executors quota [default: 0]
    --quota-executors-mem <n>    amount of memory (mb) to use per executors quota [default: 1524.0]
    --role <role>                Mesos role registered by dispatcher [default: *]
    --service-account <account>  Mesos principal registered by dispatcher
    --service-secret <secret>    Mesos secret registered by dispatcher
    --ucr-containerizer <bool>   launch using the Universal Container Runtime [default: True]
    --user <user>                user to run dispatcher service as [default: root]

"""

from docopt import docopt

import ast
import contextlib
import json
import os
import sdk_install
import shakedown
import sys


# This script will deploy the specified number of dispatchers with an optional
# options json file. It will take the given base service name and
# append an index to generate a unique service name for each dispatcher.
#
# The service names of the deployed dispatchers will be written into an output
# file.


@contextlib.contextmanager
def no_stdout():
    save_stdout = sys.stdout
    with open("/dev/null", "w") as null:
        sys.stdout = null
        yield
        sys.stdout = save_stdout


def create_quota(
    name,
    cpus=1,
    gpus=0,
    mem=1024.0
):
    with no_stdout():
        stdout, _, _ = shakedown.run_dcos_command("spark quota list --json", raise_on_error=True)
        existing_quotas = json.loads(stdout)

    # remove existing quotas matching name
    if name in [x['role'] for x in existing_quotas.get('infos', [])]:
        shakedown.run_dcos_command("spark quota remove {}".format(name), raise_on_error=True)

    # create quota
    shakedown.run_dcos_command(
        "spark quota create -c {} -g {} -m {} {}".format(cpus, gpus, mem, name), raise_on_error=True)


def deploy_dispatchers(
    num_dispatchers,
    service_name_base,
    output_file,
    options,
    package_repo=None,
    create_quotas=True,
    quota_drivers_cpus=1,
    quota_drivers_gpus=0,
    quota_drivers_mem=2048.0,
    quota_executors_cpus=1,
    quota_executors_gpus=0,
    quota_executors_mem=1024.0
):
    with open(output_file, "w") as outfile:
        shakedown.run_dcos_command("package install spark --cli --yes", raise_on_error=True)

        for i in range(0, num_dispatchers):
            service_name = "{}-{}".format(service_name_base, str(i))

            # set service name
            options["service"]["name"] = service_name

            if package_repo is not None:
                if package_repo not in [x['uri'] for x in shakedown.get_package_repos()['repositories']]:
                    shakedown.add_package_repo(
                        repo_name="{}-repo".format(service_name_base),
                        repo_url=package_repo)

            # create drivers & executors role quotas
            drivers_role = "{}-drivers-role".format(service_name)
            executors_role = "{}-executors-role".format(service_name)
            if create_quotas:
                create_quota(name=drivers_role,
                             cpus=quota_drivers_cpus, gpus=quota_drivers_gpus, mem=quota_drivers_mem)
                create_quota(name=executors_role,
                             cpus=quota_executors_cpus, gpus=quota_executors_gpus, mem=quota_executors_mem)

            # install dispatcher with appropriate role
            options["service"]["role"] = drivers_role

            sdk_install.install(
                arguments['--package-name'],
                service_name,
                0,  # This library cannot see this non-SDK task.
                additional_options=options,
                wait_for_deployment=False)

            outfile.write("{},{},{}\n".format(service_name, drivers_role, executors_role))


def get_default_options(arguments: dict) -> dict:
    options = {
        "service": {
            "cpus": int(arguments["--cpus"]),
            "mem": float(arguments["--mem"]),
            "role": arguments["--role"],
            "service_account": arguments["--service-account"] or "",
            "service_account_secret": arguments["--service-secret"] or "",
            "user": arguments["--user"],
            "log-level": arguments["--log-level"],
            "spark-history-server-url": arguments["--history-service"] or "",
            "UCR_containerizer": ast.literal_eval(arguments.get("--ucr-containerizer", True)),
            "use_bootstrap_for_IP_detect": False
        },
        "security": {
            "kerberos": {
                "enabled": ast.literal_eval(arguments.get("--enable-kerberos", False)),
                "kdc": {
                    "hostname": arguments["--kdc-hostname"] or "",
                    "port": int(arguments["--kdc-port"])
                },
                "realm": arguments["--kerberos-realm"] or ""
            }
        },
        "hdfs": {
            "config-url": arguments["--hdfs-config"] or ""
        }
    }
    return options


if __name__ == "__main__":
    arguments = docopt(__doc__, version="deploy-dispatchers.py 0.0")

    options_file = arguments['--options-json']
    if options_file:
        if not os.path.isfile(options_file):
            # TODO: Replace with logging
            print("The specified file does not exist: %s", options_file)
            sys.exit(1)

        options = json.load(open(options_file, 'r'))
    else:
        options = get_default_options(arguments)

    deploy_dispatchers(
        num_dispatchers=int(arguments['<num_dispatchers>']),
        service_name_base=arguments['<service_name_base>'],
        output_file=arguments['<output_file>'],
        options=options,
        package_repo=arguments['--package-repo'],
        create_quotas=ast.literal_eval(arguments.get("--create-quotas", True)),
        quota_drivers_cpus=arguments['--quota-drivers-cpus'],
        quota_drivers_gpus=arguments['--quota-drivers-gpus'],
        quota_drivers_mem=arguments['--quota-drivers-mem'],
        quota_executors_cpus=arguments['--quota-executors-cpus'],
        quota_executors_gpus=arguments['--quota-executors-gpus'],
        quota_executors_mem=arguments['--quota-executors-mem'])
