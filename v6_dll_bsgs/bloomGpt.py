import sys
import time
import ctypes
import os
import platform
import math
import secp256k1 as ice
import concurrent.futures

###############################################################################
###############################################################################
# Platform-dependent DLL loading (BSGS.dll for Windows, BSGS.so for Linux)
if platform.system().lower().startswith('win'):
    dllfile = 'BSGS.dll'
    if os.path.isfile(dllfile):
        pathdll = os.path.realpath(dllfile)
        icebsgs = ctypes.CDLL(pathdll)
    else:
        print('File {} not found'.format(dllfile))

elif platform.system().lower().startswith('lin'):
    dllfile = 'BSGS.so'
    if os.path.isfile(dllfile):
        pathdll = os.path.realpath(dllfile)
        icebsgs = ctypes.CDLL(pathdll)
    else:
        print('File {} not found'.format(dllfile))
else:
    print('[-] Unsupported Platform currently for ctypes dll method. Only [Windows and Linux] is working')
    sys.exit()

# Define the argument types for the C function `init_bsgs_bloom`
icebsgs.init_bsgs_bloom.argtypes = [ctypes.c_int, ctypes.c_ulonglong, ctypes.c_ulonglong, ctypes.c_int, ctypes.POINTER(ctypes.c_char)]

###############################################################################
# Create Baby Table Function (Using the external BSGS library)
def create_table(start_value, end_value):
    baby_steps = ice.create_baby_table(start_value, end_value)
    return baby_steps

# Process Bloom Filter Part (Handles concurrent creation of the bloom filter)
def process_bloom_filter_part(start, end, bloom_bits, bloom_hashes, num_cpu):
    bloom_filter = bytes(b'\x00') * (bloom_bits // 8)  # Initialize bloom filter to null

    # Convert to ctypes buffer (important to pass this correctly to the C function)
    bloom_filter_ctypes = ctypes.create_string_buffer(bloom_filter)

    # Call the C function `init_bsgs_bloom`
    icebsgs.init_bsgs_bloom(num_cpu, end - start, bloom_bits, bloom_hashes, bloom_filter_ctypes)

    # Return the part of the bloom filter
    return bloom_filter_ctypes.raw

# Main Bloom Filter Creation Function
def create_bloom_filter(total, bloom_bits, bloom_hashes, num_cpu):
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cpu) as executor:
        # Calculate chunk size per process
        chunk_size = total // num_cpu
        futures = []

        for i in range(num_cpu):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < num_cpu - 1 else total
            futures.append(executor.submit(process_bloom_filter_part, start, end, bloom_bits, bloom_hashes, num_cpu))

        # Collect results from each future
        bloom_filter_parts = [future.result() for future in futures]

    # Concatenate the bloom filter parts into the final bloom filter
    full_bloom_filter = b''.join(bloom_filter_parts)
    return full_bloom_filter

###############################################################################
# Main Program Logic
if __name__ == '__main__':
    if len(sys.argv) > 5 or len(sys.argv) < 5:
        print('[+] Program Usage.... ')
        print('{} <bP items> <output bpfilename> <output bloomfilename> <Number of cpu>\n'.format(sys.argv[0]))
        print('Example to create a File with 400 million items using 4 cpu:\n{} 400000000 bpfile.bin bloomfile.bin 4'.format(sys.argv[0]))
        sys.exit()

    st = time.time()
    total = int(sys.argv[1])
    bs_file = sys.argv[2]
    bloom_file = sys.argv[3]
    num_cpu = int(sys.argv[4])

    # Creating bpfile
    print('\n[+] Program Running please wait...')
    if total % (num_cpu * 1000) != 0:
        total = num_cpu * 1000 * (total // (num_cpu * 1000))
        print('[*] Number of elements should be a multiple of 1000*num_cpu. Automatically corrected it to nearest value:', total)

    w = math.ceil(math.sqrt(total))  # Calculate range

    bloom_prob = 0.000000001  # False Positive rate
    bloom_bpe = -(math.log(bloom_prob) / 0.4804530139182014)

    bloom_bits = int(total * bloom_bpe)  # Bits needed for bloom filter
    if bloom_bits % 8:
        bloom_bits = 8 * (1 + (bloom_bits // 8))  # Round up to the nearest byte

    bloom_hashes = math.ceil(0.693147180559945 * bloom_bpe)  # Hash functions used for bloom filter

    print('[+] Number of items required for Final Script : [bp : {0}] [bloom : {1}]'.format(w, total))
    print('[+] Output Size of the files : [bp : {0} Bytes] [bloom : {1} Bytes]'.format(w * 32, bloom_bits // 8))

    print('[+] Creating bpfile in range {0} to {1}'.format(1, w))

    # Call create_table to write the baby steps
    results = create_table(1, w)

    with open(bs_file, 'wb') as out:
        out.write(results)
        out.flush()
        os.fsync(out.fileno())

    print('[+] File : {0} created successfully in {1:.2f} sec\n'.format(bs_file, time.time() - st))

    # Create bloom filter
    print('=' * 75)
    print('[+] Starting bloom file creation ... with False Positive probability:', bloom_prob)
    print('[+] bloom bits  :', bloom_bits, '   size [%s MB]' % (bloom_bits // (8 * 1024 * 1024)))
    print('[+] bloom hashes:', bloom_hashes)

    print('[+] Initializing the bloom filters to null')
    bloom_filter = create_bloom_filter(total, bloom_bits, bloom_hashes, num_cpu)

    # Save the bloom filter to the file
    print('[+] Saving bloom filter to File')
    with open(bloom_file, 'wb') as fh:
        fh.write(bloom_filter)

    print('[+] File : {0} created successfully in {1:.2f} sec'.format(bloom_file, time.time() - st))
    print('[+] Program Finished \n')
