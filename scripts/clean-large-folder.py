import shutil
import sys
from pathlib import Path


def clean_if_necessary(folder, threshold_size):
    root_dir = Path(folder)
    size = sum(f.stat().st_size for f in root_dir.glob(
        '**/*') if f.is_file())

    print(f"The size of {folder} is {size}, threshold is {threshold_size}")
    if size > threshold_size:
        print(f"Folder size exceeds the threshold, remove it")
        shutil.rmtree(folder)


GB = 1024 * 1024 * 1024

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 clean_large_folder.py check_folder threshold_size(GB)")
    check_folder = sys.argv[1]
    threshold_size = int(sys.argv[2]) * GB
    clean_if_necessary(check_folder, threshold_size)
