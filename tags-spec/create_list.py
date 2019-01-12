import xml.etree.cElementTree as ET
import urllib2
import json

import sys
reload(sys)

if sys.version_info.major < 3:
    sys.setdefaultencoding('utf8')


def loadOverpass(lan):
    with open('iD_presets_{}.json'.format(lan)) as f:
        return json.load(f)

def extractOverpassParts(tag, body):
    t = tag.split('/')
    key = t[0]
    value = t[1] if len(t) > 1 else '-'
    word = body['name']
    return key, value, word

def fromOverpassTurbo():
    data = loadOverpass('en')
    completeTags = []
    fallbacks = []
    for tag, body in data.iteritems():
        key, value, word = extractOverpassParts(tag, body)
        if value != '-':
            completeTags.append('{}|{}|{}'.format(key, value, word))
        else:
            fallbacks.append('{}|{}|{}'.format(key, value, word))
    return sorted(completeTags), sorted(fallbacks)

def loadNominatim(lan):
    url = 'https://wiki.openstreetmap.org/wiki/Special:Export/Nominatim/Special_Phrases/{}'.format(lan.upper())
    xml = ET.ElementTree(file=urllib2.urlopen(url)).getroot()
    raw = xml[1][3][7].text
    raw = raw.replace('\n', '')
    elems = raw.split('|-')
    elems = elems[2:]
    elems[-1] = elems[-1].split('|}')[0]
    if (len(elems[-1].strip()) == 0):
        del elems[-1]
    return elems

def extractNominatimParts(elem):
    parts = elem.split('||')
    if len(parts) != 5: return None, None, None
    word = parts[0][1:].strip()
    key = parts[1].strip()
    value = parts[2].strip()
    operator = parts[3].strip()
    plural = parts[4].strip()
    if operator == '-' and plural == 'N':
        return key, value, word
    else:
        return None, None, None

def fromNominatimSpecialPhrases():
    elems = loadNominatim('en')
    tags = []
    for elem in elems:
        key, value, word = extractNominatimParts(elem)
        if key is not None:
            tags.append('{}|{}|{}'.format(key, value, word))
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


def addLangs(langs, tags):
    for lan in langs:
        addLan(lan, tags)

def addLan(lan, tags):
    nominatimData = loadNominatim(lan)
    overpassData = loadOverpass(lan)

    for i in range(0, len(tags)):
        line = tags[i]
        key = line.split('|')[0]
        value = line.split('|')[1]
        word = getWord(key, value, nominatimData, overpassData)
        tags[i] = line + '|' + word

def getWord(key, value, nominatimData, overpassData):
    word = getWord_nominatim(key, value, nominatimData)
    if word == None:
        word = getWord_overpass(key, value, overpassData)
    if word is not None: return word
    else: return 'UNSPECIFIED'

def getWord_nominatim(key, value, data):
    for elem in data:
        key2, value2, word = extractNominatimParts(elem)
        if key == key2 and value == value2: return word
    return None

def getWord_overpass(key, value, data):
    for tag, body in data.iteritems():
        key2, value2, word = extractOverpassParts(tag, body)
        if key == key2 and value == value2: return word
    return None

tags1 = fromNominatimSpecialPhrases();
tags2, fallbacks = fromOverpassTurbo();
tags = merge(tags1, tags2)
tags.extend(fallbacks)

moreLangs = ['sv']
addLangs(moreLangs, tags)

with open("tags-spec-raw", "w") as file:
    for i in range(0, len(tags)):
        file.write(tags[i])
        if i < len(tags)-1:
            file.write('\n')
