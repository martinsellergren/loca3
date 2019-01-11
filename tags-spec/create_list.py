import xml.etree.cElementTree as ET
import urllib2
import json

import sys
reload(sys)

if sys.version_info.major < 3:
    sys.setdefaultencoding('utf8')

def fromOverpassTurbo(lan):
    with open('iD_presets_{}.json'.format(lan)) as f:
        data = json.load(f)

    completeTags = []
    fallbacks = []
    for tag, body in data.iteritems():
        t = tag.split('/')
        key = t[0]
        value = t[1] if len(t) > 1 else ''
        word = body['name']

        if value != '':
            completeTags.append('{}|{}|{}'.format(word, key, value))
        else:
            fallbacks.append('{}|{}'.format(word, key))
    return sorted(completeTags), sorted(fallbacks)



def fromNominatimSpecialPhrases(lan):
    url = 'https://wiki.openstreetmap.org/wiki/Special:Export/Nominatim/Special_Phrases/{}'.format(lan.upper())
    root = ET.ElementTree(file=urllib2.urlopen(url)).getroot()
    raw = root[1][3][7].text
    raw = raw.replace('\n', '')
    elems = raw.split('|-')

    elems = elems[2:]
    elems[-1] = elems[-1].split('|}')[0]
    if (len(elems[-1].strip()) == 0):
        del elems[-1]

    tags = []
    for elem in elems:
        parts = elem.split('||')
        word = parts[0][1:].strip()
        key = parts[1].strip()
        value = parts[2].strip()
        operator = parts[3].strip()
        plural = parts[4].strip()

        if operator == '-' and plural == 'N':
            tags.append('{}|{}|{}'.format(word, key, value))
    return sorted(tags)

def specified(key, tags):
    for line in tags:
        key2 = line.split('|')[1]
        if key == key2: return True
    return False

def merge(tags1, tags2):
    tags = tags1
    for line in tags2:
        key = line.split('|')[1]
        if not specified(key, tags):
            tags.append(line)
    return sorted(tags)

languages = ['en', 'sv']
lan = languages[0]
tags1 = fromNominatimSpecialPhrases(lan);
tags2, fallbacks = fromOverpassTurbo(lan);
tags = merge(tags1, tags2)
print tags
