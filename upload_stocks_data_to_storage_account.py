# coding: utf-8

import os
import random
import uuid

from azure.storage.filedatalake import (
    DataLakeServiceClient,
)

# Go through all files and dir in local_dir_name directory and upload all content, including sub-directories.
def upload_dir(dir_name_azure, local_dir_name, filesystem_client):
    print("Creating a directory named '{}'.".format(dir_name_azure))
    directory_client = filesystem_client.create_directory(dir_name_azure)
    for filename in os.listdir(local_dir_name):
        f = os.path.join(local_dir_name, filename)
        if os.path.isfile(f):
            tmp_path = f
            tmp_file_name = f[len(local_dir_name) + 1:]
            print("Path du fichier que l'on essaye d'uploader:", tmp_path, tmp_file_name)
            
            file_client = directory_client.get_file_client(tmp_file_name)

            
            file_client.create_file()
            with open(tmp_path,"rb") as data:
                file_client.upload_data(data, overwrite=True)
            
        else: 
            print("Un dossier a été détecté :", f)
            upload_dir(dir_name_azure + "/" + f.split("/")[-1], f, filesystem_client)
    
    # get and display the permissions of the parent directory
    acl_props = directory_client.get_access_control()
    print("Permissions of directory '{}' are {}.".format(dir_name_azure, acl_props['permissions']))

    # set the permissions of the parent directory
    new_dir_permissions = 'rwx------'
    directory_client.set_access_control(permissions=new_dir_permissions)

    # get and display the permissions of the parent directory again
    acl_props = directory_client.get_access_control()
    print("New permissions of directory '{}' are {}.".format(dir_name_azure, acl_props['permissions']))

    # iterate through every file and set their permissions to match the directory
    for file in filesystem_client.get_paths(dir_name_azure):
        file_client = filesystem_client.get_file_client(file.name)

        # get the access control properties of the file
        acl_props = file_client.get_access_control()

        if acl_props['permissions'] != new_dir_permissions:
            file_client.set_access_control(permissions=new_dir_permissions)
            print("Set the permissions of file '{}' to {}.".format(file.name, new_dir_permissions))
        else:
            print("Permission for file '{}' already matches the parent.".format(file.name))



def run():
    account_name = os.getenv('STORAGE_ACCOUNT_NAME', "")
    account_key = os.getenv('STORAGE_ACCOUNT_KEY', "")
    
    # Name of main / parent directory created in file system
    genesis_dir_name = os.getenv('GENESIS_DIR_NAME', "")

    # Path to the local directory that you want to upload in Account Storage
    local_dir_path = os.getenv('LOCAL_DIR_PATH', "")


    # set up the service client with the credentials from the environment variables
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https",
        account_name
    ), credential=account_key)

    fs_name = "inputstocks"
    print("Generating a test filesystem named '{}'.".format(fs_name))

    # create the filesystem
    filesystem_client = service_client.create_file_system(file_system=fs_name)

    # invocation of the function that browses the folder and subfolders and uploads the files according to the local hierarchy.
    upload_dir(genesis_dir_name, local_dir_path, filesystem_client)


if __name__ == '__main__':
    run()