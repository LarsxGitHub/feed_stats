# feed_stats

A script that produces a bunch of metrics per session and per feed. It saves the output as pandas pickle. 

To produce data for yourself simply run:

~~~
# date format is yyyy-mm-dd, e.g., 2022-04-01
python3 stream_data.py <date>
~~~

This will run for approximately 4 hours and them produce the files: 

~~~
# features per peer ASN (across all route collectors and all session)
features_per_asn.pkl

# features per RC session
features_per_sess.pkl
~~~

To peek at the data you can run:

~~~
show.py <features_per_asn.pkl/features_per_sess.pkl>
~~~

which runs a single line of code to show few lines.

The feature names correspond to the following meanings:

~~~
pfxs<4/6> - number of ipv4/6 prefixes for which the peer/session sees routes
ips<4/6> - the number of addresses covered by these routes 
origins<4/6> - the total number ASNs that this  peer/session sees originating ipv4/6 routes
asns<4/6> - the total number of ASNs that this peer/session sees along any ipv4/6 route
dlinks<4/6> - the total number of _directed_ ASNs links that this peer/session sees along any ipv4/6 route
ulinks<4/6> - the total number of _undirected_ ASNs links that this peer/session sees along any ipv4/6 route
comms<4/6> - the total number of BGP Community attributes that this peer/session sees along any ipv4/6 route
~~~

