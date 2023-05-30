import os ,sys
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from datetime import datetime, timedelta


# Set the tag and the maximum number of snapshots to keep per disk
tag_name = 'k8s-azure-created-by'
tag_value = 'kubernetes-azure-dd'
max_snapshots_per_disk = 3 # set this to the maximum number of snapshots to keep per disk
cleanup = True # keep this set to True to delete old snapshots
dry_run = False # set this to True to test the script without deleting snapshots
verbose = False


# Set your Azure subscription ID, resource group name, and region

try:
    subscription_id = os.environ['AZURE_SUBSCRIPTION_ID']
except KeyError:
    print('AZURE_SUBSCRIPTION_ID must be set')

# Authenticate with the Azure management API using the default credentials
credential = DefaultAzureCredential()
compute_client = ComputeManagementClient(credential,subscription_id)



def create_snapshot(disk):
    """_summary_

    Args:
        disk (_type_): _description_
    """
    snapshot_name = f"{disk.name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    try:
        if verbose:
            print(f"create snapshot {snapshot_name} in resource group {disk.id.split('/')[4]}")
            
            
            
        try:    
            async_snapshot_creation = compute_client.snapshots.begin_create_or_update(
            disk.id.split('/')[4], # this is the resource group name
            snapshot_name, # this is the snapshot name

            {
                'location': disk.location,
                'incremental': 'true',
                'creation_data': {
                    'incremental': 'true',
                    'create_option': 'Copy',
                    'source_uri': disk.id
                }
            }
            )
        except Exception as Error:
            print(f"issue was detected when creating snapshot {snapshot_name} in resource group {disk.id.split('/')[4]} error message is {Error.message}")

        snapshot = async_snapshot_creation.result()
        if verbose:
            print(f"snapshot {snapshot.name} created for disk {disk.name} in resource group {disk.id.split('/')[4]}")
    except Exception as Error:
        pass

def delete_snapshot(disk,cleaned_snapshots):
    
    """_summary_

    Args:
        disk (_type_): _description_
    """
    if verbose:
        print(f"deleting snapshot for disk {disk.name} in resource group {disk.id.split('/')[4]}")
    snapshots = list(compute_client.snapshots.list())
    
    # by_disk_source = [s for s in snapshots if s.creation_data.source_uri == disk.id]
    by_disk_source = []
    for _snp in snapshots:
        if _snp.creation_data.source_resource_id == disk.id:
            by_disk_source.append(_snp)
        else:
            pass
        
    if len(by_disk_source) >= max_snapshots_per_disk:
        if cleanup:
            by_disk_source.sort(key=lambda s: s.time_created, reverse=True)
            all_but_last_three_snapshot = by_disk_source[max_snapshots_per_disk:]           
            for _snp in all_but_last_three_snapshot:
                snap_rg = _snp.id.split('/')[4]
                if verbose:
                    print(f"deleting snapshot resource group {snap_rg} snapshot name {_snp.name}")

                if disk.name not in cleaned_snapshots:
                    cleaned_snapshots[disk.name]= []
                    cleaned_snapshots[disk.name].append(_snp.name)
                else:
                    cleaned_snapshots[disk.name].append(_snp.name)
                if not dry_run:
                    compute_client.snapshots.begin_delete(resource_group_name=snap_rg,snapshot_name=_snp.name)
            else:
                pass
        else:
            by_disk_source.sort(key=lambda s: s.time_created, reverse=True)
            old_snap = by_disk_source[0]
            snap_rg = old_snap.id.split('/')[4]
            if verbose:
                print(f"deleting snapshot resource group {snap_rg} snapshot name {old_snap.name}")
            compute_client.snapshots.begin_delete(resource_group_name=snap_rg,snapshot_name=old_snap.name)
    else:
        if verbose:
            print(f"there is no snapshot to delete for disk {disk.name} in resource group {disk.id.split('/')[4]}")
        else:
            pass



def snapshot():
    cleaned_snapshots  = {}

    # Get a list of all disks with the specified tag
    disks = compute_client.disks.list()
    
    tagged_disks = [] # create an empty list to store the disks with the tag
    for disk in disks: # iterate through the list of disks
        try:
            if tag_name in disk.tags and disk.tags[tag_name] == tag_value: # if the tag is found
                tagged_disks.append(disk) # add the disk to the list of disks with the tag
        except Exception as Error:
            if verbose:
                print('there is no disk with the tag')
            else:
                pass
    if verbose:
        print(f"this is the disk list {tagged_disks}")
    
    if len(tagged_disks) > 0: # if there are disks with the tag
        for disk in tagged_disks: 
            if verbose:
                print(f"creating snapshot for disk {disk.name} in resource group {disk.id.split('/')[4]}")   
            create_snapshot(disk) # create a snapshot for each disk with the tag
            delete_snapshot(disk,cleaned_snapshots) # delete old snapshots for each disk with the tag
    else:
        print('there is no disk with the tag')

    # print the list of disks and snapshots that were deleted
    for key in cleaned_snapshots:
        print(f"disk {key} snapshots deleted {cleaned_snapshots[key]}") # print the list of snapshots that were deleted for each disk

    
