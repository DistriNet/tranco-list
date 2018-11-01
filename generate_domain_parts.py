import csv
import glob
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import tldextract


def generate_parts_list(input_fp, output_fp):
    print(input_fp)
    print(output_fp)
    with open(output_fp, 'w', encoding='UTF-8') as output_file:
        output = csv.writer(output_file)
        with open(input_fp, encoding='UTF-8') as input_file:
            for l in input_file:
                rank, fqdn = l.rstrip('\n').split(",")
                ext = tldextract.extract(fqdn)
                pld = ext.registered_domain
                is_pld = pld == fqdn
                ps = ext.suffix
                tld = fqdn[fqdn.rfind(".") + 1:]
                sld = ext.domain
                subd = ext.subdomain
                output.writerow([rank, fqdn, pld, sld, subd, ps, tld, is_pld])

if __name__ == '__main__':
    data_folder = sys.argv[1]
    pattern = sys.argv[2] if len(sys.argv) > 2 else "*"
    with ProcessPoolExecutor(max_workers=8) as executor:
        for fp in glob.glob(os.path.join(data_folder, "source/*/*{}*.csv".format(pattern))):
            input_fp = fp
            output_fp = "/".join(fp.split("/")[:-2]) + "/" + fp.split("/")[-2] + "/parts/" + fp.split("/")[-1][:-4] + "_parts.csv"
            executor.submit(generate_parts_list, input_fp, output_fp)