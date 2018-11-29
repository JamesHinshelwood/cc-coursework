import sys
import webbrowser
import pyperclip
from base64 import b64decode
from json import loads

import boto3
from typing import Tuple, List, Callable
from subprocess import call, Popen, check_output

VoidFn = Callable[[], None]


def unimplemented():
    raise NotImplementedError


class ClusterDefinition:
    worker_nodes = 10
    master_nodes = 1
    worker_type = "t2.small"
    master_type = "t2.large"
    region = "eu-west-2"


def update_definition(bucket: str) -> ClusterDefinition:
    if call(["kops", "get", "cluster", "--state", bucket]) == 1: # Bucket exists, but is empty
        return None

    new_cluster = ClusterDefinition()
    json = loads(check_output(["kops", "get", "group7.k8s.local", "-o", "json", "--state", bucket]))
    print(json[1]['spec']['role'])
    for item in json:
        try:
            if item['spec']['role'] == "Node":
                new_cluster.worker_nodes = int(item['spec']['minSize'])
                new_cluster.worker_type = item['spec']['machineType']
            elif item['spec']['role'] == "Master":
                new_cluster.master_nodes = int(item['spec']['minSize'])
                new_cluster.master_type = item['spec']['machineType']
                new_cluster.region = item['spec']['subnets'][0][:-1]  # Hacky and might not work for other regions.
        except KeyError:
            pass

    return new_cluster


warned = False


def ensure_bucket() -> str:
    global cluster
    global warned
    s3 = boto3.resource('s3')
    buckets = list(filter(lambda b: b.name == "kubernetes.group7", s3.buckets.all()))
    if len(buckets) == 0:
        if not warned:
            print("No bucket named kubernetes.group7 was found to store the cluster state")
            print("One will be created, please confirm")
            input()
            warned = True
        s3.create_bucket(Bucket="kubernetes.group7", CreateBucketConfiguration={"LocationConstraint": cluster.region})
    else:
        if not warned:
            print("Will use existing bucket 'kubernetes.group7' to store the cluster state, please confirm")
            input()
            warned = True
        cluster = update_definition("s3://kubernetes.group7")

    return "s3://kubernetes.group7"


def get_zones(region: str) -> str:
    ec2 = boto3.client('ec2', region_name=region)
    return ",".join(map(lambda d: d['ZoneName'], ec2.describe_availability_zones()['AvailabilityZones']))


def define_cluster():
    global cluster
    global bucket
    cluster = ClusterDefinition()

    call(["kops", "create", "cluster",
          "--zones", get_zones(cluster.region),
          "--node-count", str(cluster.worker_nodes),
          "--master-count", str(cluster.master_nodes),
          "--node-size", cluster.worker_type,
          "--master-size", cluster.master_type,
          "--authorization", "AlwaysAllow",
          "--state", bucket,
          "group7.k8s.local"])


def review_cluster():
    global cluster
    global bucket

    if cluster is None:
        print("You must first define the cluster")
        return

    worker_nodes = input("Worker nodes (" + str(cluster.worker_nodes) + "): ")
    if worker_nodes != "":
        cluster.worker_nodes = int(worker_nodes)

    master_nodes = input("Master nodes (" + str(cluster.master_nodes) + "): ")
    if master_nodes != "":
        cluster.master_nodes = int(master_nodes)

    worker_type = input("Worker instance type (" + str(cluster.worker_type) + "): ")
    if worker_type != "":
        cluster.worker_type = worker_type

    master_type = input("Master instance type (" + str(cluster.master_type) + "): ")
    if master_type != "":
        cluster.master_type = master_type

    region = input("Region (" + str(cluster.region) + "): ")
    if region != "":
        cluster.region = region

    call(["kops", "delete", "cluster", "--unregister", "--state", bucket, "--name", "group7.k8s.local", "--yes"])

    call(["kops", "create", "cluster",
          "--zones", get_zones(cluster.region),
          "--node-count", str(cluster.worker_nodes),
          "--master-count", str(cluster.master_nodes),
          "--node-size", cluster.worker_type,
          "--master-size", cluster.master_type,
          "--authorization", "AlwaysAllow",
          "--state", bucket,
          "group7.k8s.local"])


def launch_cluster():
    global bucket
    call(["kops", "update", "cluster", "--state", bucket, "group7.k8s.local", "--yes"])


def validate_cluster():
    global bucket
    call(["kops", "validate", "cluster", "--state", bucket])


def delete_cluster():
    global bucket
    call(["kops", "delete", "cluster", "--state", bucket, "--name", "group7.k8s.local", "--yes"])


def deploy_web():
    call(["kubectl", "create", "-f",
          "https://raw.githubusercontent.com/kubernetes/dashboard/master/src/deploy/recommended/kubernetes-dashboard.yaml"])

    Popen(["kubectl", "proxy"])


def access_web():
    webbrowser.open("http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/")


def get_password():
    password = loads(check_output(["kubectl", "config", "view", "-o", "json"]))['users'][0]['user']['password']
    print(password)
    pyperclip.copy(password)
    print("Copied to clipboard")


def get_token():
    secret_name = loads(check_output(["kubectl", "get", "serviceaccount", "default", "-o", "json"]))['secrets'][0]['name']
    secret = loads(check_output(["kubectl", "get", "secret", secret_name, "-o", "json"]))['data']['token']
    decoded = b64decode(secret).decode('utf-8')
    print(decoded)
    pyperclip.copy(decoded)
    print("Copied to clipboard")


def view_cluster():
    call(["kubectl", "get", "nodes"])


menu_options: List[Tuple[str, VoidFn, List[Tuple[str, VoidFn]]]] = \
    [("Exit", lambda: sys.exit(0), []),
     ("Define a Kubernetes cluster", define_cluster, [
         ("Review the cluster definition", review_cluster)
     ]),
     ("Launch the cluster on AWS", launch_cluster, [
         ("Validate the cluster", validate_cluster),
         ("Deploy the Kubernetes web-dashboard", deploy_web),
         ("Access the Kubernetes web-dashboard", access_web)
     ]),
     ("View the cluster", view_cluster, [
         ("Get the admin password", get_password),
         ("Get the admin service account token", get_token)
     ]),
     ("Delete the cluster", delete_cluster, [])]


def print_menu_options():
    for i, (option, _, suboptions) in enumerate(menu_options):
        print(str(i) + ": " + option)
        for j, (suboption, _) in enumerate(suboptions):
            print("\t" + str(i) + str(j + 1) + ": " + suboption)


def get_menu_selection(input: str) -> VoidFn:
    try:
        first = int(input[0])
        if len(input) > 1:
            second = int(input[1])
            return menu_options[first][2][second - 1][1]
        else:
            return menu_options[first][1]
    except (ValueError, IndexError):
        print("Invalid selection")
        return lambda: None


if __name__ == '__main__':
    bucket = ensure_bucket()

    while True:
        print_menu_options()
        get_menu_selection(input("Please enter your choice: "))()
        input("Press enter to continue")
