#!/usr/bin/env python3
"""
Large File Check Script for Pre-commit Hook

This script checks if any files in the commit exceed the maximum allowed file size.
It helps prevent accidentally committing large files like datasets, binaries, or logs
that could bloat the repository.

Usage:
    python large_file_check.py [file1] [file2] ...

Configuration:
    - MAX_FILE_SIZE_MB: Maximum allowed file size in megabytes
    - ALLOWED_LARGE_EXTENSIONS: File extensions that are allowed to be large
    - EXCLUDED_PATHS: Paths to exclude from size checking
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Set


# Configuration
MAX_FILE_SIZE_MB = 10  # Maximum file size in MB
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

# File extensions that are allowed to be larger (if needed)
ALLOWED_LARGE_EXTENSIONS = {
    '.parquet',  # Data files
    '.csv',      # Large datasets (but still check)
    '.zip',      # Compressed files
    '.tar.gz',   # Archives
}

# Paths to exclude from size checking
EXCLUDED_PATHS = {
    '.git/',
    '__pycache__/',
    '.venv/',
    'venv/',
    '.env/',
    'node_modules/',
    '.pytest_cache/',
    '.coverage',
    'dist/',
    'build/',
}

# Binary file extensions that should never be large
BINARY_EXTENSIONS = {
    '.exe', '.dll', '.so', '.dylib',
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.ico',
    '.mp4', '.avi', '.mov', '.wmv', '.flv',
    '.mp3', '.wav', '.ogg', '.flac',
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
}


def format_file_size(size_bytes: int) -> str:
    """Convert bytes to human readable format."""
    if size_bytes >= 1024 * 1024 * 1024:  # GB
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    elif size_bytes >= 1024 * 1024:  # MB
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    elif size_bytes >= 1024:  # KB
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} bytes"


def should_exclude_path(file_path: str) -> bool:
    """Check if the file path should be excluded from size checking."""
    file_path = file_path.replace('\\', '/')  # Normalize path separators
    
    for excluded in EXCLUDED_PATHS:
        if excluded in file_path:
            return True
    return False


def is_binary_file(file_path: str) -> bool:
    """Check if file is a binary file based on extension."""
    extension = Path(file_path).suffix.lower()
    return extension in BINARY_EXTENSIONS


def check_file_size(file_path: str) -> tuple[bool, str]:
    """
    Check if a single file exceeds the size limit.
    
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        if not os.path.exists(file_path):
            return True, ""  # File doesn't exist, skip
            
        if should_exclude_path(file_path):
            return True, ""  # Excluded path, skip
            
        file_size = os.path.getsize(file_path)
        
        if file_size <= MAX_FILE_SIZE_BYTES:
            return True, ""  # File size is OK
            
        # File is too large
        extension = Path(file_path).suffix.lower()
        file_size_formatted = format_file_size(file_size)
        max_size_formatted = format_file_size(MAX_FILE_SIZE_BYTES)
        
        error_msg = f"[ERROR] FILE TOO LARGE: {file_path}\n"
        error_msg += f"   Size: {file_size_formatted} (limit: {max_size_formatted})\n"
        
        if is_binary_file(file_path):
            error_msg += f"   WARNING: Binary file detected - consider using Git LFS or excluding from repo\n"
        elif extension in ALLOWED_LARGE_EXTENSIONS:
            error_msg += f"   WARNING: Large data file - consider compressing or using external storage\n"
        else:
            error_msg += f"   💡 Suggestions:\n"
            error_msg += f"      - Split into smaller files\n"
            error_msg += f"      - Compress the file\n"
            error_msg += f"      - Use external storage (S3, Azure Blob, etc.)\n"
            error_msg += f"      - Add to .gitignore if it's a generated file\n"
            
        return False, error_msg
        
    except Exception as e:
        return False, f"[ERROR] ERROR checking {file_path}: {str(e)}\n"


def check_multiple_files(file_paths: List[str]) -> tuple[bool, List[str]]:
    """
    Check multiple files for size violations.
    
    Returns:
        tuple: (all_valid, list_of_error_messages)
    """
    all_valid = True
    errors = []
    
    for file_path in file_paths:
        is_valid, error_msg = check_file_size(file_path)
        if not is_valid:
            all_valid = False
            errors.append(error_msg)
    
    return all_valid, errors


def get_all_files_in_repo() -> List[str]:
    """Get all files in the current repository (for manual runs)."""
    files = []
    for root, dirs, filenames in os.walk('.'):
        # Skip hidden directories and common build directories
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in {'__pycache__', 'node_modules', 'dist', 'build'}]
        
        for filename in filenames:
            if not filename.startswith('.'):  # Skip hidden files
                file_path = os.path.join(root, filename)
                files.append(file_path)
    return files


def print_summary(total_files: int, large_files: int, max_size_mb: int):
    """Print a summary of the file size check."""
    print(f"\n[SUMMARY] LARGE FILE CHECK SUMMARY:")
    print(f"   Total files checked: {total_files}")
    print(f"   Large files found: {large_files}")
    print(f"   Size limit: {max_size_mb} MB")
    
    if large_files == 0:
        print(f"   [PASS] All files are within the size limit!")
    else:
        print(f"   [FAIL] {large_files} file(s) exceed the size limit")


def main():
    """Main function for the large file check."""
    parser = argparse.ArgumentParser(
        description="Check files for size violations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
    # Check specific files (used by pre-commit)
    python large_file_check.py file1.py file2.csv

    # Check all files in repository
    python large_file_check.py --all

    # Use custom size limit
    python large_file_check.py --max-size 50 file1.py

Configuration:
    Current max file size: {MAX_FILE_SIZE_MB} MB
    Excluded paths: {', '.join(EXCLUDED_PATHS)}
    Binary extensions: {', '.join(sorted(BINARY_EXTENSIONS))}
        """
    )
    
    parser.add_argument(
        'files', 
        nargs='*', 
        help='Files to check (if none provided, checks all files)'
    )
    parser.add_argument(
        '--all', 
        action='store_true', 
        help='Check all files in the repository'
    )
    parser.add_argument(
        '--max-size', 
        type=int, 
        default=MAX_FILE_SIZE_MB,
        help=f'Maximum file size in MB (default: {MAX_FILE_SIZE_MB})'
    )
    parser.add_argument(
        '--verbose', 
        action='store_true', 
        help='Show verbose output including files that pass'
    )
    
    args = parser.parse_args()
    
    # Update max size if provided
    global MAX_FILE_SIZE_BYTES
    MAX_FILE_SIZE_BYTES = args.max_size * 1024 * 1024
    
    # Determine which files to check
    if args.all or not args.files:
        files_to_check = get_all_files_in_repo()
        print(f"[SCAN] Checking all files in repository (max size: {args.max_size} MB)...")
    else:
        files_to_check = args.files
        print(f"[SCAN] Checking {len(files_to_check)} file(s) (max size: {args.max_size} MB)...")
    
    if not files_to_check:
        print("[INFO] No files to check.")
        return 0
    
    # Check files
    all_valid, errors = check_multiple_files(files_to_check)
    
    # Print results
    if errors:
        print("\n" + "="*60)
        print("[ERROR] LARGE FILE VIOLATIONS FOUND:")
        print("="*60)
        for error in errors:
            print(error)
    
    # Print verbose output for passing files
    if args.verbose and all_valid:
        print(f"\n[PASS] All {len(files_to_check)} files are within the size limit")
    
    # Print summary
    print_summary(len(files_to_check), len(errors), args.max_size)
    
    # Exit with appropriate code
    if all_valid:
        return 0
    else:
        print(f"\n💡 To fix these issues:")
        print(f"   1. Remove or compress large files")
        print(f"   2. Add large files to .gitignore if they're generated")
        print(f"   3. Use Git LFS for binary files")
        print(f"   4. Store large datasets in cloud storage")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
