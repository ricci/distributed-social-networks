# Distributed Social Networks

Simple code and data to measure how distributed various social networks are in practice.

* `hhi.py <file.csv>`: Computes [Herfindahl–Hirschman index](https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index) - values closer to 0 indicate competitive ecosystems, closer to 1 indicate monopolies
* `fetch-bsky.py`: Scrapes PDS list and user counts from the main bluesky relay
* `fetch-fedilist.py`: Scrapes host data from the HTML version of fedlist
* `fedilist.csv`: Contains user data for the fediverse (ActivityPub), from https://fedilist.com/instance
* `fedilist-fromhtml.csv`: Same as above, but scraped from the HTML version
* `atproto-bsky-relay.csv`: Contains user data for the atmosphere (AT Protocol), scraped by `fetch-bsky.py` from the main bluesky relay
* `mackuba.csv`: Contains user data for the atmosphere (AT Protocol), from https://blue.mackuba.eu/directory/pdses and bsky number comes from https://bsky.jazco.dev/stats
* `www/`: Simple dashboard to display indices
