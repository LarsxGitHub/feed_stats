import sys, pybgpstream, argparse
from collections import defaultdict
from pytricia import PyTricia
import pandas as pd
def yield_elements(day, collector = None, peer_asn = None, ribs_only = False):
    kwl = []
    # first rib of the day
    kwl.append(
            {"from_time":    day+' 00:00:00 UTC',
            "until_time":    day+' 00:00:00 UTC',
            "record_type":   "ribs"})

    if not ribs_only:
        # all the updates throughout the day
        kwl.append(
            {"from_time":    day+' 00:00:00 UTC',
            "until_time":    day+' 23:59:59 UTC',
            "record_type":   "updates"})


    for kwargs in kwl:
        if collector: kwargs["collector"] = collector
        if peer_asn: kwargs["filter"] = f"peer {peer_asn}"

    for kwargs in kwl:
        # stream the data
        stream = pybgpstream.BGPStream(**kwargs)
        for elem_id, elem in enumerate(stream):
            # if elem_id == 10000: break
            if not elem.type in ['A', "R"]: continue
            yield elem

def pfx2addrs(pfx):
    baseaddr, cidr = pfx.rsplit('/', 1)
    cmax = 32
    if baseaddr.find('.', 0, 4) == -1:
        cmax = 128
    return 2**(cmax-int(cidr))


def count_addr_in_prefix(pfxs, proto):
    # building pytricia trie
    if proto == 4:
        pyt = PyTricia()
    else:
        pyt = PyTricia(128)
    for pfx in pfxs:
        pyt.insert(pfx, '')

    total = 0
    for pfx in pyt:
        if pyt.parent(pfx): continue
        total += pfx2addrs(pfx)
    return total

def extract_links_asns_and_origin(elem):
    if not 'as-path' in elem.fields:
        return set(), set(), -1

    hops = elem.fields['as-path'].split()
    if not hops:
        return set(), set(), -1

    # remove prepending
    path = [hops[0]]
    hops = elem.fields['as-path'].split()
    for i in range(1, len(hops)):
        if hops[i] != path[-1]:
            path.append(hops[i])

    # extract links and asns
    links, asns = set(), set()
    for i in range(1, len(path)):
        try:
            asn = int(path[i])
            asns.add(asn)
            prev = int(path[i-1])
            asns.add(prev)
            links.add(f'{prev}-{asn}')
        except ValueError: # as-set
            continue

    try:
        origin = int(path[-1])
    except ValueError:
        origin = -1
    return links, asns, origin

def directed_to_undirected_links(dlinks):
    ulinks = set()
    for link in dlinks:
        a, b = map(int, link.split('-'))
        if a < b:
            ulinks.add(link)
        else:
            ulinks.add(f'{b}-{a}')
    return ulinks

def finalize(state):
    keys = state.keys()
    features = []
    for p in [4, 6]:
        for f in ['pfxs', 'ips', 'origins', 'asns', 'dlinks', 'ulinks', 'comms']:
            features.append(f'{f}{p}')
    d = pd.DataFrame(0, index=keys, columns=features, dtype = float)
    for id in state:
        row = []
        for p in [4, 6]:
            # prefixes
            row.append(len(state[id][f'pfxs{p}']))
            # addresses
            row.append(count_addr_in_prefix(state[id][f'pfxs{p}'], p))
            # origins
            row.append(len(state[id][f'origins{p}']))
            # asns
            row.append(len(state[id][f'asns{p}']))
            # dlinks
            row.append(len(state[id][f'dlinks{p}']))
            # ulinks
            row.append(len(directed_to_undirected_links(state[id][f'dlinks{p}'])))
            # communities
            row.append(len(state[id][f'comms{p}']))
        d.loc[id] = row
    return d

def get_session_id(elem):
    return f'{elem.collector}-{elem.peer_asn}-{elem.peer_address}'

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--collector", required=False, help="select only data from a single collector", default=None)
    ap.add_argument("-p", "--peer_asn", required=False, help="select only data from a single peer asn", default=None)
    ap.add_argument("DATE", help="UTC+0 Date in format yyyy-mm-dd")
    kwargs = vars(ap.parse_args())

    data_per_asn = defaultdict(lambda: defaultdict(set))
    data_per_sess = defaultdict(lambda: defaultdict(set))

    day=kwargs['DATE']
    for elem in yield_elements(day, kwargs['collector'], kwargs['peer_asn'], ribs_only = True):
        if not 'prefix' in elem.fields: continue

        # extract prefix and proto
        pfx = elem.fields['prefix']
        proto = 4
        if pfx.find('.', 0, 4) == -1:
            proto = 6

        dlinks, asns, origin = extract_links_asns_and_origin(elem)

        data_per_asn[elem.peer_asn][f'pfxs{proto}'].add(pfx)
        data_per_asn[elem.peer_asn][f'asns{proto}'].update(asns)
        data_per_asn[elem.peer_asn][f'dlinks{proto}'].update(dlinks)
        data_per_asn[elem.peer_asn][f'comms{proto}'].update(elem.fields['communities'])
        if origin != -1:
            data_per_asn[elem.peer_asn][f'origins{proto}'].add(origin)


        sid = get_session_id(elem)
        data_per_sess[sid][f'pfxs{proto}'].add(pfx)
        data_per_sess[sid][f'asns{proto}'].update(asns)
        data_per_sess[sid][f'dlinks{proto}'].update(dlinks)
        data_per_sess[sid][f'comms{proto}'].update(elem.fields['communities'])
        if origin != -1:
            data_per_sess[sid][f'origins{proto}'].add(origin)

    pd_p_asn = finalize(data_per_asn)
    pd_p_asn.to_pickle("features_per_asn.pkl")
    pd_p_sess = finalize(data_per_sess)
    pd_p_sess.to_pickle("features_per_sess.pkl")
