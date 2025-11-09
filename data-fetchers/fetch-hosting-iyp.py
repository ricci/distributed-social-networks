from collections import defaultdict
from neo4j import GraphDatabase
from pathlib import Path
from string import Template
import csv
import pickle
import sys

country = None

if len(sys.argv) > 1:
    country = sys.argv[1]

# IYP sandbox (beta) database
URI = 'neo4j://sandbox.ihr.live:7687'
AUTH = None
db = GraphDatabase.driver(URI, auth=AUTH)

OUTPUT_FILE = ""
if not country:
    OUTPUT_FILE = (Path(__file__).parent / "../data-static/hosting-worldwide-from-iyp.csv").resolve()
else:
    OUTPUT_FILE = (Path(__file__).parent / f"../data-static/hosting-by-country/{country}.csv").resolve()

# Will be used to cache AS sibling sets
picklefile = (Path(__file__).parent / "sets.pickle").resolve()

### Functions for sibling sets
# Save a list of frozensets to a file
def save_frozensets(filename, data):
    with open(filename, "wb") as f:
        pickle.dump(data, f)

# Load them back
def load_frozensets(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)

def find_set_containing(sets, element):
    for s in sets:
        if element in s:
            return s
    return None

sibset = []

if Path(picklefile).is_file():
    sibset = load_frozensets(picklefile)

def find_in_sibset(asn):
    s = next((s for s in sibset if asn in s), None)
    if s is not None:
        return s
    else: 
        # This query gets all ASes that are siblings of the passed set
        # NB: the AS *itself* is not in the returned set, so we have to add
        # it explicitly
        records, _, _ = db.execute_query(
            '''
            MATCH (a:AS)-[:SIBLING_OF]-(s:AS) 
            WHERE a.asn = {:d}
            RETURN COLLECT(DISTINCT s.asn)
            '''.format(asn)
        )
        for r in records:
            set = frozenset(r.values()[0] + [asn])
            sibset.append(set)
            return set

# Group records by the ASN they belong to - note that we pick an arbitrary
# ASN, but in a fozenset this is stable
def group_records_by_asn(records):
    grouped = defaultdict(list)
    for record in records:
        asn = find_in_sibset(record["a.asn"])
        grouped[asn].append(record)
    return grouped
### end of sibling sets


db.verify_connectivity()

query = ""
if not country:
    # If we're doing worldwide, we use the Tranco list, which is worldwide
    query = """
        MATCH (dn:DomainName)-[r:RANK {reference_name:'tranco.top1m'}]-(:Ranking  {name: 'Tranco top 1M'})
        WITH dn
        MATCH (dn)-[:PART_OF]-(hn:HostName)-[:RESOLVES_TO {reference_name: 'openintel.tranco1m'}]-(:IP)-[:PART_OF]-(:Prefix)-[:ORIGINATE]-(a:AS)
        WHERE dn.name = hn.name
        RETURN 
        a.asn, COUNT(DISTINCT hn) as nb_hostnames ORDER BY nb_hostnames DESC
        """
else:
    # If we're doing one country, we use crUX since it's organized that way
    query = Template("""
        MATCH (hn:HostName)-[r:RANK {reference_name:'google.crux_top1m_country'}]-(:Ranking)-[:COUNTRY]-(cc:Country)
        WHERE cc.country_code = '$country' AND r.rank < 10001
        WITH hn
        MATCH (hn)-[:RESOLVES_TO {reference_name: 'openintel.crux'}]-(:IP)-[:PART_OF]-(:Prefix)-[:ORIGINATE]-(a:AS)
        RETURN 
        a.asn, COUNT(DISTINCT hn) as nb_hostnames ORDER BY nb_hostnames DESC
        """).substitute(country=country)

records, _, _ = db.execute_query(query)

grouped = group_records_by_asn(records)

totals = {
    next(iter(asn)): sum(r["nb_hostnames"] for r in recs)
    for asn, recs in grouped.items()
}

fieldnames = ["asn","nb_hostnames"]

pairs = sorted(totals.items(), key=lambda x: x[1], reverse=True)
with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(fieldnames)
    writer.writerows(pairs)


db.close()

save_frozensets(picklefile,sibset)

