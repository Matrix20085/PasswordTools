import time
import argparse
import lmdb
import os
import sys
import re
import io
import hashlib

from tqdm import tqdm
from charset_normalizer import from_bytes

def parse_cli_options():

    parser = argparse.ArgumentParser(
        description="Deduplication program for word files"
    )
    parser.add_argument("-i", "--input", type=str, required=False,
                        help="Input folder")
    parser.add_argument("-f", "--file", type=str, required=False,
                        help="Input file")
    parser.add_argument("-o", "--output", type=str, required=True,
                        help="Output folder")
    parser.add_argument("-of", "--outputfile", type=str, default="wordlist",
                        help="Output file names (default: wordlist(next number))")
    parser.add_argument("-db", "--database", type=str, default=None,
                        help="Location of database (default: same folder as output)")
    return parser.parse_args()

# Creates a database
def create_lmdb(db_path):

    db_full_path = os.path.join(db_path, "database")
    # dbenv = lmdb.open(db_full_path, map_size=1073741824) # 1GB
    # dbenv = lmdb.open(db_full_path, map_size=10737418240) # 10GB
    dbenv = lmdb.open(db_full_path, map_size=53687091200) # 50GB
    # dbenv = lmdb.open(db_full_path, map_size=536870912000) # 500GB
    return dbenv

# Returns number of entries in DB
def get_db_count(dbenv):

    with dbenv.begin() as txn:
        stats = txn.stat()
        return stats['entries']

# Updates specific key/value pair in DB
def update_db(key,value,dbenv):

    key = key.encode()

    with dbenv.begin(write=True) as txn:
        old_value = txn.get(key)
        if old_value:
            new_value = int(old_value) + value
        else:
            new_value = value
        txn.put(key, str(new_value).encode(), overwrite=True)
        return new_value

# Returns best guess of encoding
def guess_encoding(path):
    with open(path, "rb") as f:
        sample = f.read(1048576)  # Read first 1 MB, safe for huge files
    results = from_bytes(sample)
    best = results.best()
    return best.encoding if best else "utf-8"

# Processes single file and returns stats about what has/has not been entered into DB
def process_file(file_path, dbenv):

    total_bytes = os.path.getsize(file_path)
    encoding = guess_encoding(file_path)

    file_total = 0
    file_new = 0
    file_duplicates = 0
    file_non_words = 0
    buffer_count = 0
    bytes_since_update = 0

    
    if total_bytes >  500000000: # If file is over half a GB, update counter less ofter
        update_interval = 1000000
    else:
        update_interval = 1

    with open(file_path, "rb") as raw:
        with io.TextIOWrapper(raw, encoding=encoding, errors="replace") as f:
            txn = dbenv.begin(write=True)
            with tqdm(total=total_bytes, unit='B', unit_scale=True,
                        desc=f"Processing {os.path.basename(file_path)}") as pbar:
                for line in f:
                    line_bytes = len(line.encode(encoding, errors="replace"))
                    bytes_since_update += line_bytes
                    if bytes_since_update > update_interval:
                        pbar.update(bytes_since_update)
                        bytes_since_update = 0
                    file_total += 1
                    line = line.strip()
                    if not line.isprintable() or not len(line) < 33: # Check string-level issues
                        file_non_words += 1
                        continue
                    line_utf8 = line.encode("utf-8")
                    if not line_utf8 or len(line_utf8) > 511: # Check byte-level issues
                        file_non_words += 1
                        continue
                    try:
                        result = txn.put(line_utf8, b'new', overwrite=False)
                        if result:
                            file_new += 1
                        else:
                            file_duplicates += 1
                    except lmdb.Error as e:
                        print(f"LMDB error during put: {e}")
                        print("Attempting to write buffer to DB")
                        try:
                            txn.commit()
                        except:
                            txn.abort()
                        raise
                    except Exception as e:
                        print(f"Non-LMDB error during put: {e}")
                        pass
                    buffer_count += len(line_utf8)
                    if buffer_count > 100000000: # Write to DB every 100MB
                        try:
                            txn.commit()
                        except lmdb.Error as e:
                            print(f"LMDB Error: {e.__class__.__name__} - {e}")
                            txn.abort()
                        txn = dbenv.begin(write=True)
                        buffer_count = 0
                    
            txn.commit()

    return file_total, file_new, file_duplicates, file_non_words

# Checks if file has been added then sends to process_file
# Returns total stats
def process_folder(folder_path, dbenv):

    total_words_in_files = 0
    total_new_words = 0
    total_duplicates = 0
    total_non_words = 0

    for entry in os.listdir(folder_path):
        file_path = os.path.join(folder_path, entry)
        if os.path.isfile(file_path) and not check_file_in_db(dbenv, hash_file(file_path)):
            t, n, d, a = process_file(file_path, dbenv)
            total_words_in_files += t
            total_new_words += n
            total_duplicates += d
            total_non_words += a
            add_file_to_db(dbenv, hash_file(file_path))
        else:
            print(entry, "is already in the database.")
            continue

    return total_words_in_files, total_new_words, total_duplicates, total_non_words

# Gets last number used for output wordlist
def get_file_counter(output_folder, base_filename):

    max_counter = 0
    pattern = re.compile(r'^' + re.escape(base_filename) + r'(\d+)\.txt$')

    for filename in os.listdir(output_folder):
        match = pattern.match(filename)
        if match:
            num = int(match.group(1))
            if num > max_counter:
                max_counter = num
    return max_counter + 1

# Saves all new words to a text file
def export_new_words(output_folder, base_filename, dbenv):

    max_file_size = 1 * 1024 * 1024 * 1024  # 1GB limit in bytes

    # Determine starting file counter (e.g., if wordlist1.txt exists, continue with the next number)
    file_counter = get_file_counter(output_folder, base_filename)
    current_file_path = os.path.join(output_folder, f"{base_filename}{file_counter}.txt")
    current_file = open(current_file_path, "w", encoding="utf-8")
    current_file_size = 0

    with dbenv.begin(write=True) as txn:
        total_keys = txn.stat()['entries']
        with txn.cursor() as cursor:
            with tqdm(total=total_keys, unit='lines', desc="Writing output files: ") as pbar:
                # Iterate through all key-value pairs in the database
                for key, value in cursor:
                    pbar.update(1)
                    # Only add word to list if it is a new word
                    if value == b'new':
                        # Decode key to a string for writing; re-encode if needed for LMDB
                        word = key.decode("utf-8", errors="replace")
                        line = word + "\n"
                        encoded_line = line.encode("utf-8")
                        
                        # Check if adding this line would exceed our 1GB cap
                        if current_file_size + len(encoded_line) > max_file_size:
                            current_file.close()
                            file_counter += 1
                            current_file_path = os.path.join(output_folder, f"{base_filename}{file_counter}.txt")
                            current_file = open(current_file_path, "w", encoding="utf-8")
                            current_file_size = 0
                        
                        # Write the line to the file and update the current file size counter
                        current_file.write(line)
                        current_file_size += len(encoded_line)
                        
                        # Update the LMDB entry from 'new' to 'old'
                        cursor.put(key, b'old', overwrite=True)
                    
    current_file.close()

# Gets SHA1 of file
def hash_file(filepath):

   h = hashlib.sha1()
   print("Hashing", filepath, end='\r')
   with open(filepath,'rb') as file:
       chunk = 0
       while chunk != b'':
           chunk = file.read(1024)
           h.update(chunk)
   return h.hexdigest()

# Checks if file hash is in DB
def check_file_in_db(dbenv, sha1_hash):

    key = sha1_hash.encode("utf-8")
    with dbenv.begin() as txn:
        stored_value = txn.get(key)
        if stored_value == "processed".encode():
            return True

# Adds file hash into DB
def add_file_to_db(dbenv, sha1_hash):

    value = "processed"
    key = sha1_hash.encode("utf-8")
    with dbenv.begin(write=True) as txn:
        txn.put(key, value.encode(), overwrite=True)

def main():
    start_time = time.time()

    options = parse_cli_options()

    print("Starting the deduplication program")

    if options.input:
        options.input = os.path.abspath(options.input)
        if not os.path.exists(options.input):
            print(f"Error: Input folder '{options.input}' does not exist.")
            sys.exit(1)
        print("Input folder:", options.input)
    elif options.file:
        options.file = os.path.abspath(options.file)
        print("Input file:", options.file)
        if not os.path.exists(options.file):
            print(f"Error: Input file '{options.file}' does not exist.")
            sys.exit(1)
    else:
        print("Error: You must included either a folder (-i) or a file (-f)")
        sys.exit(1)

    options.output = os.path.abspath(options.output)

    if options.database:
        options.database = os.path.abspath(options.database)
    else:
        options.database = os.path.abspath(options.output)


    
    print("Output folder:", options.output)
    print("Output file names: ", options.outputfile, "{#}.txt", sep='')
    print("Database location: ", options.database, "\\database", sep='')
    
    # Create the output folder if it doesn't exist
    if not os.path.exists(options.output):
        os.makedirs(options.output)
        print(f"Created output folder: {options.output}")

    # Create the database folder if it doesn't exist
    if not os.path.exists(options.database):
        os.makedirs(options.database)
        print(f"Created database folder: {options.database}")

    input("Check everything is correct, then hit Enter to continue...")

    # Create the DB connector
    dbenv = create_lmdb(options.database)
    db_before = get_db_count(dbenv)

    if options.input:
        words_in_files, new_words, duplicates, non_words = process_folder(options.input, dbenv)
    elif options.file and not check_file_in_db(dbenv, hash_file(options.file)):
        words_in_files, new_words, duplicates, non_words = process_file(options.file, dbenv)
        add_file_to_db(dbenv, hash_file(options.file))
    else:
        print(options.file, "is already in the database.")
        sys.exit(1)

    print("\n\nStarting to output new words...")
    if new_words > 0:
       export_new_words(options.output,options.outputfile,dbenv)

    db_after = get_db_count(dbenv)

    print("\nStatistics:")
    print("Total words in DB before running:", f"{db_before:,}")
    print("Total words in DB after running:", f"{db_after:,}")
    print("Number of new words added:", f"{new_words:,}")
    print("Number of total words in this run:", f"{words_in_files:,}")
    print("Number of total words over all time:", f"{update_db('total_words',words_in_files,dbenv):,}")
    print("Number of duplicates in this run:", f"{duplicates:,}")
    print("Number of duplicates over all time:", f"{update_db('total_duplicates',duplicates,dbenv):,}")
    print("Number of lines that were not words:",f"{non_words:,}")
    
    
    dbenv.close()

    elapsed_time = time.time() - start_time
    formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    print("Total runtime:", formatted_time)

if __name__ == "__main__":
    main()