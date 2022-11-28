from datetime import datetime
from math import floor, log
from os.path import expanduser
from re import match as re_match, search as re_search
from time import strftime, time, gmtime
from typing import Any, Callable, Optional

from lxml import etree
import mariadb
import pywikibot as pwb
import requests


SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()

WIKIDATA_API_ENDPOINT = 'https://www.wikidata.org/w/api.php'
DB_PARAMS = {
    'host' : 'wikidatawiki.analytics.db.svc.wikimedia.cloud',
    'database' : 'wikidatawiki_p',
    'default_file' : f'{expanduser("~")}/replica.my.cnf'
}

DAY_LIMIT = None # int or None; days

# TODO:
# sitelink moves
# wbsetlabeldescriptionaliases

# claims

# editentity
# merge
# revert actions


#### database management
class WikidataReplica:
    def __init__(self) -> None:
        self.replica = mariadb.connect(**DB_PARAMS)
        self.cursor = self.replica.cursor(dictionary=True)

    def __enter__(self) -> mariadb.connection.cursor:
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


def query_mediawiki(query:str, params:Optional[tuple[Any]]=None) -> list[dict[str, Any]]:
    with WikidataReplica() as db_cursor:
        if params is None:
            db_cursor.execute(query)
        else:
            db_cursor.execute(query, params)
        result = db_cursor.fetchall()

    return result


#### generic patrolling function
def patrol_revisions(rev_ids:list) -> None:
    cnt = len(rev_ids)
    if cnt==0:
        return

    digits = floor(log(cnt, 10))+1

    try:
        patrols = SITE.patrol(revid=rev_ids) # "patrols" is a generator
    except pwb.exceptions.APIError as exception:
        print(exception)
        return

    done = 0
    while True:
        try:
            try:
                patrol = next(patrols)
            except pwb.exceptions.Error as exception:
                print(exception)
            except pwb.exceptions.APIError as exception:
                print(exception)
            else:
                done += 1
                print(f'({done:{digits}d}/{cnt:{digits}d}) Patrolled rc_id {patrol["rcid"]}' \
                      f' of page {patrol["title"]} (ns{patrol["ns"]})')
        except StopIteration:
            break


#### reverted revisions
def get_reverted_unpatrolled_revisions() -> list:
    sql = """SELECT
  rc_id,
  rc_this_oldid
FROM
  recentchanges
    JOIN change_tag ON ct_rc_id=rc_id
WHERE
  rc_patrolled=0
  AND ct_tag_id=674""" # 674=mw-reverted

    query_result = query_mediawiki(sql)
    rev_ids = [ elem['rc_this_oldid'] for elem in query_result ]
    return rev_ids


def patrol_reverted_revisions() -> None:
    rev_ids = get_reverted_unpatrolled_revisions()

    print(f'Found {len(rev_ids)} revisions to be patrolled')
    patrol_revisions(rev_ids)


#### revisions in redirected items
def get_revisions_in_redirected_items() -> list:
    sql = """SELECT
  rc_id,
  rc_this_oldid,
  comment_text
FROM
  recentchanges
    JOIN page ON rc_cur_id=page_id
    JOIN comment_recentchanges ON rc_comment_id=comment_id
WHERE
  rc_patrolled=0
  AND rc_namespace=0
  AND page_is_redirect=1"""

    query_result = query_mediawiki(sql)
    rev_ids = []
    for elem in query_result:
        if re_search(
            r'(wbmergeitems\-to|wbmergeitems\-from|wbcreateredirect)',
            elem['comment_text'].decode('utf8')
        ) is None:
            rev_ids.append(elem['rc_this_oldid'])
    return rev_ids


def patrol_revisions_redirected_items() -> None:
    rev_ids = get_revisions_in_redirected_items()

    print(f'Found {len(rev_ids)} revisions to be patrolled')
    patrol_revisions(rev_ids)


#### generic processors
def query_revision_subset(action:str, limit=None) -> list[dict[str, Any]]:
    if limit is None:
        query_limit = '\n'
    else:
        timestmp = strftime('%Y%m%d', gmtime(time()-limit*86400) )
        query_limit = f'\n  AND rc_timestamp>{timestmp}000000'

    sql = f"""SELECT
  rc_id,
  rc_this_oldid,
  rc_title,
  comment_text
FROM
  recentchanges
    JOIN comment_recentchanges ON rc_comment_id=comment_id
WHERE
  rc_patrolled=0
  AND rc_namespace=0{query_limit}
  AND comment_text LIKE '/* {action}:%'"""

    return query_mediawiki(sql)


def process_revision_subset(action:str, pattern:str, check_function:Callable) -> None:
    limit = DAY_LIMIT
    query_result = query_revision_subset(action, limit)
    for i, elem in enumerate(query_result, start=1): # elem: tpl (rc_id, rev_id, qid, edit_summary)
        match = re_match(
            pattern,
            elem['comment_text'].decode('utf8')
        )
        if match is None: # cannot process
            continue

        qid = elem['rc_title'].decode('utf8')
        revision_id = elem['rc_this_oldid']
        key = match.group(2) # language, project identifier, etc
        value = match.group(3) # modified value

        if action.startswith('wbsetaliases-'): # special treatment for alias modifications
            try:
                diff = get_revision_diff(revision_id)
            except RuntimeError:
                value = None
            else:
                value = scrape_aliases_from_diff(diff)

        if check_function(qid=qid, key=key, value=value) is True:
            print(
                f'{i}/{len(query_result)}',
                qid,
                key,
                value,
                next(SITE.patrol(revid=revision_id)) # process generator with one item
            )
        else:
            if i%100 == 0:
                print(f'Progress: {i}/{len(query_result)}')


#### helpers
def get_revision_diff(revision_id:int) -> str:
    response = requests.get(
        WIKIDATA_API_ENDPOINT,
        params={
            'action' : 'compare',
            'fromrev' : str(revision_id),
            'torelative' : 'prev',
            'format' : 'json'
        }
    )

    payload = response.json()

    if payload.get('compare') is None:
        raise RuntimeError(f'Unsuccessful API call ("compare" key missing for rev {revision_id})')
    if payload.get('compare').get('*') is None:
        raise RuntimeError(f'Unsuccessful API call ("*" key missing for rev {revision_id})')

    return payload.get('compare').get('*') # this is a HTML string representation of the diff


def scrape_aliases_from_diff(diff:str) -> dict[str, list[str]]:
    if len(diff)==0:
        raise RuntimeError('Cannot scrape links from empty string')

    aliases:dict[str, list[str]] = { 'add' : [], 'remove' : [] }

    tree = etree.fromstring(diff, etree.HTMLParser())
    try:
        tr_tags = tree.xpath('//tr') # list of lxml.etree._Element
    except AttributeError as exception:
        raise RuntimeError('xpath attribute missing') from exception

    process_table_cells = False
    for i, tr_tag in enumerate(tr_tags, start=1):
        td_tags = tr_tag.findall('td') # list of lxml.etree._Element
        for td_tag in td_tags:
            td_cls = td_tag.attrib.get('class')
            td_txt = str(td_tag.text)

            if i%2 == 1: # odd header lines
                if td_cls != 'diff-lineno': # ignore then
                    process_table_cells=False
                    continue

                process_table_cells = td_txt.startswith('aliases / ')

            else: # even content lines
                if process_table_cells is not True:
                    continue

                if td_cls=='diff-deletedline':
                    aliases['remove'].append(td_tag.find('div').find('del').text)
                if td_cls=='diff-addedline':
                    aliases['add'].append(td_tag.find('div').find('ins').text)

    return aliases


#### internal decision functions
def should_patrol_sitelink_removal(qid:str='', key:str='', value:str='') -> bool:
    # True if:
    #* the removed sitelink has meanwhile been added to this item again
    #* the removed sitelink page has been deleted meanwhile
    #* the removed sitelink has been added to another item meanwhile (disputable)

    ## first attempt: check whether the sitelink in question is in the item
    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage():
        return False

    item_page.get()

    if not item_page.sitelinks:
        return False

    try:
        connected_sitelink = item_page.sitelinks.get(key)
    except pwb.exceptions.NoUsername:
        return False
    else:
        if connected_sitelink is not None and str(connected_sitelink)[2:-2] == value:
            return True # removed sitelink already present again

    ## second attempt: check which item is connected to the sitelink
    family, lang = str(pwb.site.APISite.fromDBName(key)).split(':')
    project_page = pwb.Page(
        pwb.Site(code=lang, fam=family),
        value
    )

    try:
        if not project_page.exists():
            return True
    except pwb.exceptions.InvalidTitleError:
        print('Invalid title:', value)
        return False

    try:
        connected_item = pwb.ItemPage.fromPage(project_page)
    except pwb.exceptions.NoPageError: # no item connected
        #print(project_page)
        #print(exception)
        return False
    else:
        #connected_item.get()
        if qid != connected_item.title(): # sitelink is meanwhile connected to another item
            return True

    return False


def should_patrol_sitelink_addition(qid:str='', key:str='', value:str='') -> bool:
    # True if:
    #* item is a redirect page
    #* item does not have any sitelinks (i.e. the added sitelink is gone as well)
    #* item does not have a sitelink to the project in question (i.e. added sitelink has
    #  been removed meanwhile)
    #* a different sitelink is meanwhile present in the item
    #* sitelink page has been deleted meanwhile; this means that this sitelinks is probably
    #  gone as well
    #* sitelink has been moved to another item

    ## first attempt: check whether the sitelink in question is in the item
    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage(): # item meanwhile redirects
        return True

    item_page.get()

    if not item_page.sitelinks:
        return True # sitelink was meanwhile removed again

    if not key in item_page.sitelinks:
        return True # no such sitelink any longer present

    try:
        connected_sitelink = item_page.sitelinks.get(key)
    except pwb.exceptions.NoUsernameError as exception:
        print(exception)
        return False
    else:
        try:
            if connected_sitelink is not None and str(connected_sitelink)[2:-2] != value:
                return True # different sitelink meanwhile present
        except pwb.exceptions.NoUsernameError as exception:
            print(exception)
            return False

    ## second attempt: check which item is connected to the sitelink
    family, lang = str(pwb.site.APISite.fromDBName(key)).split(':')
    project_page = pwb.Page(
        pwb.Site(code=lang, fam=family),
        value
    )

    try:
        if not project_page.exists():
            return True # sitelink meanwhile deleted
    except pwb.exceptions.InvalidTitleError as exception:
        print(value, exception)
        return False
    except pwb.exceptions.UnsupportedPageError as exception:
        print(value, exception)
        return False

    try:
        connected_item = pwb.ItemPage.fromPage(project_page)
    except pwb.exceptions.NoPageError: # no item connected
        #print(exception)
        return False
    else:
        if qid != connected_item.title(): # sitelink is meanwhile connected to another item
            return True

    return False


def should_patrol_label_removal(qid:str='', key:str='', value:str='') -> bool:
    # True if:
    #* item is a redirect
    #* item has this label again

    item_page = pwb.ItemPage(REPO, qid)

    try:
        if not item_page.exists():
            return False
    except ValueError:
        print(qid, key, value)
        return False

    if item_page.isRedirectPage():
        return True # item meanwhile redirects

    item_page.get()

    if not item_page.labels:
        return False

    if not key in item_page.labels:
        return False

    if item_page.labels.get(key) == value:
        return True # removed label meanwhile present again

    return False


def should_patrol_label_modification(qid:str='', key:str='', value:str='') -> bool:
    # True if:
    #* item is redirect
    #* item has no labels
    #* item has no label in this language
    #* item has a different label in this language

    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage(): # item meanwhile redirects
        return True

    item_page.get()

    if not item_page.labels:
        return True # no labels, i.e. label in question has been removed again

    if not key in item_page.labels:
        return True # no label in this language any longer present

    if item_page.labels.get(key) is not None and item_page.labels.get(key) != value:
        return True # different label meanwhile present

    return False


def should_patrol_description_removal(qid:str='', key:str='', value:str='') -> bool:
    # True if:
    #* item is a redirect
    #* item has this description again

    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage():
        return True # item meanwhile redirects

    item_page.get()

    if not item_page.descriptions:
        return False

    if not key in item_page.descriptions:
        return False

    if item_page.descriptions.get(key) == value:
        return True # removed description meanwhile present again

    return False


def should_patrol_description_modification(qid:str='', key:str='', value:str='') -> bool:
    # True if:
    #* item is redirect
    #* item has no descriptions
    #* item has no description in this language
    #* item has a different description in this language

    def tidy_description(description:str) -> str:
        appendices = [
            '#suggestededit-add 1.0' ,
            '#suggestededit-translate 1.0',
            '([[w:en:Wikipedia:Shortdesc helper|Shortdesc helper]])',
#            'Moving sitelink to [[Qxxx]]'  # need a regex here
        ]

        if ',' not in description:
            return description

        parts = description.split(', ')
        if parts[-1] not in appendices:
            return description

        tidied_description = ', '.join(parts[:-1])

        return tidied_description


    value = tidy_description(value)

    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage(): # item meanwhile redirects
        return True

    item_page.get()

    if not item_page.descriptions:
        return True # no descriptions, i.e. description in question has been removed again

    if not key in item_page.descriptions:
        return True # no description in this language any longer present

    if item_page.descriptions.get(key) is not None and item_page.descriptions.get(key) != value:
        return True # different description meanwhile present

    return False


def should_patrol_alias_additions(qid:str='', key:str='', value:dict[str, list[str]]=None) -> bool:
    # True if:
    #* item is a redirect
    #* item has no aliases
    #* item has no aliases in this language
    #* none of the added aliases is any longer present in this language
    if value is None: # something went wrong with the diff
        return False

    if len(value.get('remove', [])) > 0: # should not happen here
        return False

    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage(): # item meanwhile redirects
        return True

    item_page.get()

    if not item_page.aliases:
        return True # no aliases, i.e. aliases in question have been removed again

    if not key in item_page.aliases:
        return True # no aliases in this language any longer present

    aliases_still_existing = [ alias for alias in value.get('add', []) if alias in item_page.aliases.get(key) ]
    if len(aliases_still_existing) == 0:
        return True # none of the added aliases is still present

    return False


def should_patrol_alias_removals(qid:str='', key:str='', value:dict[str, list[str]]=None) -> bool:
    # True if:
    #* item is a redirect
    #* none of the removed aliases is still missing in this language
    if value is None: # something went wrong with the diff
        return False

    if len(value.get('add', [])) > 0: # should not happen here
        return False

    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage(): # item meanwhile redirects
        return True

    item_page.get()

    if not item_page.aliases:
        return False # no aliases, i.e. aliases in question have not been added again

    if not key in item_page.aliases:
        return False # still no aliases in this language present

    aliases_still_missing = [ alias for alias in value.get('remove', []) if alias not in item_page.aliases.get(key) ]
    if len(aliases_still_missing) == 0:
        return True # none of the removed aliases is still missing

    return False


def should_patrol_alias_modifications(qid:str='', key:str='', value:dict[str, list[str]]=None) -> bool:
    # True if:
    #* item is a redirect
    #* none of the removed aliases is still missing, and none of the added aliases is still present
    if value is None: # something went wrong with the diff
        return False

    item_page = pwb.ItemPage(REPO, qid)

    if not item_page.exists():
        return False

    if item_page.isRedirectPage(): # item meanwhile redirects
        return True

    item_page.get()

    if len(value.get('remove', [])) > 0 and not item_page.aliases:
        return False # no aliases, i.e. removed aliases in question have not been added again

    if len(value.get('remove', [])) > 0 and not key in item_page.aliases:
        return False # removed aliases still not present in this language present

    aliases_still_missing = [ alias for alias in value.get('remove', []) if alias not in item_page.aliases.get(key) ]

    if not item_page.aliases or not key in item_page.aliases:
        aliases_still_existing = []
    else:
        aliases_still_existing = [ alias for alias in value.get('add', []) if alias in item_page.aliases.get(key) ]

    if len(aliases_still_missing) == 0 and len(aliases_still_existing) == 0:
        return True # none of the removed aliases is still missing, and none of the added aliases is still present

    return False


def should_patrol_sitelink_deletion(qid:str='', key:str='', value:dict=None) -> bool:
    return True


#### caller functions
def patrol_sitelink_removals() -> None:
    action = 'wbsetsitelink-remove'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z_]+) \*\/ (.*)$'
    check_function = should_patrol_sitelink_removal

    process_revision_subset(action, pattern, check_function)


def patrol_sitelink_additions() -> None:
    action = 'wbsetsitelink-add'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z_]+) \*\/ (.*)$'
    check_function = should_patrol_sitelink_addition

    process_revision_subset(action, pattern, check_function)


def patrol_label_additions() -> None:
    action = 'wbsetlabel-add'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_label_modification

    process_revision_subset(action, pattern, check_function)


def patrol_label_modifications() -> None:
    action = 'wbsetlabel-set'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_label_modification

    process_revision_subset(action, pattern, check_function)


def patrol_label_removals() -> None:
    action = 'wbsetlabel-remove'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_label_removal

    process_revision_subset(action, pattern, check_function)


def patrol_description_additions() -> None:
    action = 'wbsetdescription-add'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_description_modification

    process_revision_subset(action, pattern, check_function)


def patrol_description_modifications() -> None:
    action = 'wbsetdescription-set'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_description_modification

    process_revision_subset(action, pattern, check_function)


def patrol_description_removals() -> None:
    action = 'wbsetdescription-remove'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_description_removal

    process_revision_subset(action, pattern, check_function)


def patrol_alias_additions() -> None:
    action = 'wbsetaliases-add'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_alias_additions

    process_revision_subset(action, pattern, check_function)


def patrol_alias_removals() -> None:
    action = 'wbsetaliases-remove'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_alias_removals

    process_revision_subset(action, pattern, check_function)


def patrol_alias_settings() -> None:
    action = 'wbsetaliases-set'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_alias_modifications

    process_revision_subset(action, pattern, check_function)


def patrol_alias_updates() -> None:
    action = 'wbsetaliases-update'
    pattern = fr'^\/\* {action}:(\d+)\|([a-z0-9-]+) \*\/ (.*)$'
    check_function = should_patrol_alias_modifications

    process_revision_subset(action, pattern, check_function)


def patrol_sitelink_deletions() -> None:
    action = 'clientsitelink-remove'
    pattern = fr'^\/\* {action}:(\d+)\|\|([a-z-]+) \*\/ (.*)$'
    check_function = should_patrol_sitelink_deletion

    process_revision_subset(action, pattern, check_function)


#### main
def main_by_hour() -> None:
    SITE.login()

    jobs = {
        0 : patrol_reverted_revisions,
        1 : patrol_revisions_redirected_items,
        2 : patrol_sitelink_additions,
        5 : patrol_sitelink_removals,
        6 : patrol_sitelink_deletions,
        7 : patrol_label_additions,
        10 : patrol_label_removals,
        11 : patrol_label_modifications,
        12 : patrol_description_additions,
        15 : patrol_description_removals,
        16 : patrol_description_modifications,
        17 : patrol_alias_additions,
        18 : patrol_alias_removals,
        19 : patrol_alias_settings,
        20 : patrol_alias_updates,
    }

    func = jobs.get(datetime.now().hour)
    if func is not None:
        func()


def main() -> None:
    SITE.login()

    patrol_reverted_revisions()
    patrol_revisions_redirected_items()
    patrol_sitelink_additions()
    patrol_sitelink_removals()
    patrol_sitelink_deletions()
    patrol_label_additions()
    patrol_label_removals()
    patrol_label_modifications()
    patrol_description_additions()
    patrol_description_removals()
    patrol_description_modifications()
    patrol_alias_additions()
    patrol_alias_removals()
    patrol_alias_settings()
    patrol_alias_updates()


def main_testing() -> None:
    pass


if __name__=='__main__':
    main()

