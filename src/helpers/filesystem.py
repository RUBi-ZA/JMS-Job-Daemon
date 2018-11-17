import os, shutil, pwd, grp


def print_to_file(filename, data, mode = 'w', permissions = 0o775):
    with open(filename, mode) as f:
        f.write(str(data))
    os.chmod(filename, permissions)    
    
    
def read_file(path):
    content = ""
    with open(path, 'r') as file:
        content = file.read()
    return content
    
    
def copy_file(source, destination, permissions = 0o775):
    if (os.path.isfile(source)):
        shutil.copy(source, destination)
        os.chmod(destination, permissions)
    
    
def set_owner(path, user, group, recursive=False, exclude=[]):  
    uid = pwd.getpwnam(user).pw_uid
    gid = grp.getgrnam(group).gr_gid
    
    if recursive:
        for root, dirs, files in os.walk(path):  
            for momo in dirs:
                full_path = os.path.join(root, momo)
                if full_path not in exclude:
                    os.chown(full_path, uid, gid)
            for momo in files:
                full_path = os.path.join(root, momo)
                if full_path not in exclude:
                    os.chown(full_path, uid, gid)
    else:
        os.chown(path, uid, gid)


def create_directory(path, permissions = 0o775):
    if not os.path.exists(path):
        os.makedirs(path)
        os.chmod(path, permissions)


def copy_directory(source, destination, permissions = 0o775, replace = True, exclude=[]):
    create_directory(destination, permissions)
    
    src_files = os.listdir(source)
    for file_name in src_files:
        full_file_name = os.path.join(source, file_name)
        full_dest_name = os.path.join(destination, file_name)
        
        if not full_file_name in exclude:
            if os.path.isfile(full_file_name) and (replace or not os.path.isfile(full_dest_name)):
                    copy_file(full_file_name, full_dest_name, permissions)
            elif os.path.isdir(full_file_name):
                copy_directory(full_file_name, full_dest_name)
