# Patrol bot for Wikidata
Wikidata bot that marks outdated and overwritten changes as patrolled.

Warning: the code is relatively immature, inefficient, and incomplete. Yet, it works for now …

## Technical requirements
The bot is currently scheduled to run weekly on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) from within the `msynbot` tool account. It depends on the [shared pywikibot files](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Pywikibot#Using_the_shared_Pywikibot_files_(recommended_setup)) and is running in a Kubernetes environment using Python 3.11.2.
