import csv
import sys

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
    input_fp = sys.argv[1]
    output_fp = "/".join(input_fp.split("/")[:-1]) + "/parts/" + input_fp.split("/")[-1][:-4] + "_parts.csv"
    generate_parts_list(input_fp, output_fp)