import gzip
import json
from collections import Counter

import tldextract

input_file = "ref-wiki-en_urls.txt.gz"


def sort_domains():
    domains = []
    domain_counter = Counter()

    with gzip.open(input_file, "rt") as infile:
        for line in infile:
            url = line.strip()
            extracted = tldextract.extract(url)
            registered_domain = f"{extracted.domain}.{extracted.suffix}"
            domains.append(registered_domain)
            domain_counter[registered_domain] += 1

    sorted_domains = domain_counter.most_common()
    domain_dict = dict(sorted_domains)

    with open("top_domains.json", "w") as json_file:
        json.dump(domain_dict, json_file, indent=4)

    with open("top_domains.txt", "w") as txt_file:
        for domain, _ in domain_dict.items():
            txt_file.write(f"{domain}\n")


if __name__ == "__main__":
    sort_domains()
