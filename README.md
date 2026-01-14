# Are We Decentralized Yet?

Code and data to measure how distributed various networks are in practice.

This is used to feed https://arewedecentralizedyet.online/ .

* `BIndex.md`: A proposal for a way of measuring blockability on distributed social networks
* `centralization_stats.py <file.csv>`: Computes [Herfindahl–Hirschman index](https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index) and other statistics. Pass `--json` to get machine-readable output
* `data-fetchers/` contains various scripts to grab statistics from a number of sources
* `data-static/` contains static versions of the data - some are fetched with scripts from `data-fetchers/`, others are one-time dumps from sources such as academic papers
* `habib-paper/` code and data specifically related to the Habib et al. paper
* `liu-paper/` code and data specifically related to the Liu et al. paper
* `helpers/` helper scripts for things like running all data fetchers, maintaining the website
* `www/` code for the website; intended for static hosting

TODO:
* Add matrix, see https://codeberg.org/ricci/are-we-decentralized-yet/issues/2
