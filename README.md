# Distributed Social Networks

Simple code and data to measure how distributed various networks are in practice.

This is used to feed https://arewedecentralizedyet.online/ .

* `hhi.py <file.csv>`: Computes [Herfindahl–Hirschman index](https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index) and other statistics
* `fetch-bsky.py`: Scrapes PDS list and user counts from the main bluesky relay
* `fetch-fedilist.py`: Scrapes host data from the HTML version of fedlist
* `fetch-sh.py`: Scrapes git hosting data from the HTML version of Software Heritage's coverage page
* `fedilist-fromhtml.csv`: Contains user data for the fediverse (ActivityPub), from https://fedilist.com/instance
* `atproto-bsky-relay.csv`: Contains user data for the atmosphere (AT Protocol)
* `sh-fromhtml.csv`: Contains repository data scraped from Software Heritage
* `worldwide.csv`: Contains hosting data pulled from Internet Yellow Pages for the top 100M websites worldwide
* `dns-byid.csv`: Data from the Habib et al. 2025 paper, processed by AS ID
* `cert-byid.csv`: Data from the Habib et al. 2025 paper, processed by AS ID
* `www/`: Simple dashboard to display indices
* `BIndex.md`: A proposal for a way of measuring blockability on distributed social networks

TODO:
* Add email hosting, get data from https://dl.acm.org/doi/10.1145/3487552.3487820
* Add matrix, see https://codeberg.org/ricci/are-we-decentralized-yet/issues/2

