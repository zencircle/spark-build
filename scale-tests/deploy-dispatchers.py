import shakedown
import sys


# This script will deploy the specified number of dispatchers with an optional
# options json file. It will take the given base service name and
# append an index to generate a unique service name for each dispatcher.
#
# Running:
# > dcos cluster setup <cluster url>
# > python deploy-dispatchers.py 2 spark-instance


SPARK_PACKAGE_NAME="spark"


#TODO: add options json file contents
def deploy_dispatchers(num_dispatchers, service_name_base, options_json_file):
    for i in range(0, num_dispatchers):
        service_name = "{}-{}".format(service_name_base, str(i))
        options = {}
        options["service"] = options.get("service", {})
        options["service"]["name"] = service_name

        shakedown.install_package(
            SPARK_PACKAGE_NAME,
            options_json=options)


if __name__ == "__main__":
    """
        Usage: python deploy-dispatchers.py [num_dispatchers] [service_name_base] (options_json_file)
    """

    if len(sys.argv) < 3:
        print("Usage: deploy-dispatchers.py [num_dispatchers] [service_name_base] (options_json_file)")
        sys.exit(2)

    num_dispatchers = int(sys.argv[1])
    service_name_base = sys.argv[2]
    options_json_file = sys.argv[3] if len(sys.argv) > 3 else None
    print("num_dispatchers: {}".format(num_dispatchers))
    print("service_name_base: {}".format(service_name_base))
    print("options_json_file: {}".format(options_json_file))

    deploy_dispatchers(num_dispatchers, service_name_base, options_json_file)
