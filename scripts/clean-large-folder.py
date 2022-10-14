import os
import shutil
import sys

def compute_folder_size(folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

def clean_if_necessary(folder, threshold_size):
    size = compute_folder_size(folder)
    print(f"The size of {folder} is {size}, threshold is {threshold_size}")
    if size > threshold_size:
        print(f"Folder size exceeds the threshold, remove it")
        shutil.rmtree(folder)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 check-clean-target.py check_folder threshold_size")
    check_folder = sys.argv[1]
    threshold_size = int(sys.argv[2])
    clean_if_necessary(check_folder, threshold_size)
