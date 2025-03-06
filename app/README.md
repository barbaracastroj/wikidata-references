
# Wikidata-refs 

React App that lets you search for external references for any Wikidata item claim. It uses a pre-built Solr index for the search of authoritative sources and the Wikidata API to fetch Wikidata items and their up-to-date statements.


# How it works

First you need to enter a Wikidata identifier.

<p align="center">
  <img src="https://i.imgur.com/TiZZ462.png" width="600">
</p>

This permforms a query to Wikidata to obtain information about the Item and the list of its statements.

<p align="center">
  <img src="https://i.imgur.com/XGh1aPH.png" width="600">
</p>

Then by clicking search references the Solr index returns the 3 best sources where the information may be validated for that claim.

<p align="center">
  <img src="https://i.imgur.com/tiO3tkS.png" width="600">
</p>
